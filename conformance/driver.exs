# Conformance driver for `firemq-conformance`.
#
# Reads one JSON command per line on stdin and writes exactly one JSON reply per line on
# stdout. Logs go to stderr. Compile first so Mix prints nothing on stdout:
#
#     mix compile
#     firemq-conformance run --driver "mix run conformance/driver.exs"

defmodule FireMQ.Conformance.Driver do
  def main do
    # Keep stdout for replies: send log output to stderr.
    :logger.remove_handler(:default)

    :logger.add_handler(:default, :logger_std_h, %{
      config: %{type: :standard_error},
      formatter: Logger.default_formatter()
    })

    loop(%{conn: nil, subs: %{}, messages: %{}, next_sub: 1})
  end

  defp loop(state) do
    case IO.read(:stdio, :line) do
      line when is_binary(line) ->
        case String.trim(line) do
          "" ->
            loop(state)

          json ->
            command = Jason.decode!(json)
            {reply, state} = run(command, state)
            IO.puts(Jason.encode!(Map.put(reply, "id", command["id"])))
            loop(state)
        end

      _eof_or_error ->
        if state.conn, do: FireMQ.close(state.conn)
        :ok
    end
  end

  defp run(command, state) do
    case handle(command["op"], command, state) do
      {:ok, result, state} -> {%{"ok" => true, "result" => result}, state}
      {:error, error, state} -> {%{"ok" => false, "error" => error_json(error)}, state}
    end
  rescue
    e ->
      {%{"ok" => false, "error" => %{"kind" => "other", "message" => Exception.message(e)}},
       state}
  end

  defp handle("hello", _c, state) do
    {:ok,
     %{
       "sdk" => "firemq-elixir",
       "version" => to_string(Application.spec(:firemq, :vsn)),
       "capabilities" => []
     }, state}
  end

  defp handle("connect", c, state) do
    case FireMQ.connect(c["url"]) do
      {:ok, conn} -> {:ok, %{}, %{state | conn: conn, subs: %{}, messages: %{}}}
      {:error, error} -> {:error, error, state}
    end
  end

  defp handle("close", _c, state) do
    if state.conn, do: FireMQ.close(state.conn)
    {:ok, %{}, %{state | conn: nil, subs: %{}, messages: %{}}}
  end

  defp handle(_op, _c, %{conn: nil} = state) do
    {:error, %FireMQ.Error{kind: :closed, message: "not connected"}, state}
  end

  defp handle("declare_queue", c, state) do
    opts = [
      durable: c["durable"],
      exclusive: c["exclusive"],
      auto_delete: c["auto_delete"],
      args: c["args"] || %{}
    ]

    case FireMQ.declare_queue(state.conn, c["name"], opts) do
      {:ok, info} ->
        result = %{
          "name" => info.name,
          "messages" => info.messages,
          "consumers" => info.consumers
        }

        {:ok, result, state}

      {:error, error} ->
        {:error, error, state}
    end
  end

  defp handle("delete_queue", c, state) do
    FireMQ.delete_queue(state.conn, c["name"]) |> count(state)
  end

  defp handle("purge_queue", c, state) do
    FireMQ.purge_queue(state.conn, c["name"]) |> count(state)
  end

  defp handle("declare_exchange", c, state) do
    FireMQ.declare_exchange(state.conn, c["name"], c["type"], durable: c["durable"])
    |> result(state)
  end

  defp handle("delete_exchange", c, state) do
    FireMQ.delete_exchange(state.conn, c["name"]) |> result(state)
  end

  defp handle("bind_queue", c, state) do
    FireMQ.bind_queue(state.conn, c["queue"], c["exchange"], c["key"], args: c["args"] || %{})
    |> result(state)
  end

  defp handle("unbind_queue", c, state) do
    FireMQ.unbind_queue(state.conn, c["queue"], c["exchange"], c["key"]) |> result(state)
  end

  defp handle("bind_exchange", c, state) do
    FireMQ.bind_exchange(state.conn, c["destination"], c["source"], c["key"]) |> result(state)
  end

  defp handle("publish", c, state) do
    body =
      case c do
        %{"body_base64" => encoded} when is_binary(encoded) -> Base.decode64!(encoded)
        %{"body" => body} -> body
      end

    opts = [
      persistent: c["persistent"],
      mandatory: c["mandatory"],
      properties: c["properties"] || %{},
      headers: c["headers"] || %{}
    ]

    FireMQ.publish(state.conn, c["exchange"], c["key"], body, opts) |> result(state)
  end

  defp handle("subscribe", c, state) do
    case FireMQ.subscribe(state.conn, c["queue"], prefetch: c["prefetch"], no_ack: c["no_ack"]) do
      {:ok, ref} ->
        id = "sub-#{state.next_sub}"
        state = %{state | subs: Map.put(state.subs, id, ref), next_sub: state.next_sub + 1}
        {:ok, %{"sub" => id}, state}

      {:error, error} ->
        {:error, error, state}
    end
  end

  defp handle("next", c, state) do
    ref = Map.fetch!(state.subs, c["sub"])

    receive do
      {:firemq, ^ref, message} -> message_result(message, state)
      {:firemq_cancelled, ^ref, _reason} -> {:ok, %{"message" => nil}, state}
    after
      c["timeout_ms"] -> {:ok, %{"message" => nil}, state}
    end
  end

  defp handle("ack", c, state) do
    {message, messages} = Map.pop(state.messages, c["tag"])
    state = %{state | messages: messages}
    if message, do: FireMQ.ack(message) |> result(state), else: {:ok, %{}, state}
  end

  defp handle("nack", c, state) do
    {message, messages} = Map.pop(state.messages, c["tag"])
    state = %{state | messages: messages}
    if message, do: FireMQ.nack(message, c["requeue"]) |> result(state), else: {:ok, %{}, state}
  end

  defp handle("unsubscribe", c, state) do
    {ref, subs} = Map.pop(state.subs, c["sub"])
    state = %{state | subs: subs}
    if ref, do: FireMQ.unsubscribe(state.conn, ref) |> result(state), else: {:ok, %{}, state}
  end

  defp handle("get", c, state) do
    case FireMQ.get(state.conn, c["queue"], no_ack: c["no_ack"]) do
      {:ok, nil} -> {:ok, %{"message" => nil}, state}
      {:ok, message} -> message_result(message, state)
      {:error, error} -> {:error, error, state}
    end
  end

  defp handle(op, _c, state) do
    {:error, {:unsupported, "op #{inspect(op)} is not supported"}, state}
  end

  defp result(:ok, state), do: {:ok, %{}, state}
  defp result({:error, error}, state), do: {:error, error, state}

  defp count({:ok, messages}, state), do: {:ok, %{"messages" => messages}, state}
  defp count({:error, error}, state), do: {:error, error, state}

  defp message_result(message, state) do
    tag = Integer.to_string(message.delivery_tag)
    state = %{state | messages: Map.put(state.messages, tag, message)}

    json = %{
      "body" => if(String.valid?(message.body), do: message.body),
      "body_base64" => Base.encode64(message.body),
      "exchange" => message.exchange,
      "routing_key" => message.routing_key,
      "redelivered" => message.redelivered,
      "tag" => tag,
      "properties" => Map.new(message.properties, fn {k, v} -> {Atom.to_string(k), v} end),
      "headers" => message.headers
    }

    {:ok, %{"message" => json}, state}
  end

  defp error_json({:unsupported, message}), do: %{"kind" => "unsupported", "message" => message}

  defp error_json(%FireMQ.Error{} = error) do
    kind =
      if error.kind in [:server, :unroutable, :nacked, :closed],
        do: Atom.to_string(error.kind),
        else: "other"

    json = %{"kind" => kind, "message" => Exception.message(error)}
    if error.code, do: Map.put(json, "code", error.code), else: json
  end
end

FireMQ.Conformance.Driver.main()
