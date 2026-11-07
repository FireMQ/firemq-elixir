defmodule FireMQ do
  @moduledoc """
  Elixir client for [FireMQ](https://github.com/FireMQ/firemq), over the native FireMQ
  protocol.

      {:ok, conn} = FireMQ.connect("fire://guest:guest@localhost:5673")
      {:ok, _info} = FireMQ.declare_queue(conn, "jobs")
      :ok = FireMQ.publish(conn, "", "jobs", "hello")

      {:ok, sub} = FireMQ.subscribe(conn, "jobs")

      receive do
        {:firemq, ^sub, message} ->
          IO.puts(message.body)
          :ok = FireMQ.ack(message)
      end

      :ok = FireMQ.close(conn)

  Defaults favour safety: queues and exchanges are durable, messages are persistent, and
  `publish/5` returns once the broker confirms the message.

  Functions return `:ok`, `{:ok, value}` or `{:error, %FireMQ.Error{}}`. Each has a bang
  variant, such as `publish!/5`, that returns the value or raises `FireMQ.Error`.

  Every function that waits for the broker accepts a `:timeout` option in milliseconds.
  It defaults to `30_000`.
  """

  alias FireMQ.{Connection, Error, Message, QueueInfo}

  @typedoc "A connection: the pid returned by `connect/2`, or a registered name."
  @type conn :: GenServer.server()

  @typedoc "A subscription, as returned by `subscribe/3`."
  @type sub_ref :: reference()

  @typedoc "Queue, exchange and binding arguments, or application headers."
  @type table :: %{optional(String.t() | atom()) => term()} | keyword()

  @timeout 30_000

  ## Connection

  @doc """
  Opens a connection.

  The URL looks like `fire://user:password@host:5673/vhost`. User and password default to
  `guest`, the port to 5673 (`fire://`) or 5674 (`fires://`, TLS), and the vhost to `/`.

  The connection belongs to the calling process and closes when that process exits. To
  run a connection under a supervisor, use `FireMQ.Connection.start_link/1`.

  ## Options

    * `:heartbeat` - heartbeat interval in seconds; `0` disables heartbeats. Defaults to `30`.
    * `:connection_name` - a name shown in the management UI.
    * `:connect_timeout` - in milliseconds. Defaults to `10_000`.
    * `:ssl` - options for `:ssl.connect/3` on `fires://` URLs, for example
      `ssl: [cacertfile: "ca.pem"]`.

  ## Examples

      {:ok, conn} = FireMQ.connect("fire://localhost")
      {:error, %FireMQ.Error{kind: :server, code: 403}} =
        FireMQ.connect("fire://guest:wrong@localhost")
  """
  @spec connect(String.t(), keyword()) :: {:ok, pid()} | {:error, Error.t()}
  def connect(url \\ "fire://localhost", opts \\ []) do
    Connection.start_owned(self(), Keyword.put(opts, :url, url))
  end

  @doc "Like `connect/2`, but returns the pid or raises."
  @spec connect!(String.t(), keyword()) :: pid()
  def connect!(url \\ "fire://localhost", opts \\ []), do: url |> connect(opts) |> unwrap!()

  @doc """
  Closes the connection gracefully. Returns `:ok`, also when the connection is already
  closed.
  """
  @spec close(conn()) :: :ok
  def close(conn) do
    case GenServer.whereis(conn) do
      pid when is_pid(pid) ->
        # Wait for the process to exit, so the connection is gone when close/1 returns.
        ref = Process.monitor(pid)

        case call(pid, :close, 5_000) do
          {:error, %Error{kind: :timeout}} -> GenServer.stop(pid, :normal)
          _ -> :ok
        end

        receive do
          {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
        after
          5_000 -> Process.demonitor(ref, [:flush])
        end

        :ok

      _ ->
        :ok
    end
  catch
    :exit, _ -> :ok
  end

  @doc """
  Returns what the broker reported in the handshake: `:server` (name and version),
  `:conn_id`, `:heartbeat`, `:max_frame` and `:caps`.
  """
  @spec server_info(conn()) :: {:ok, map()} | {:error, Error.t()}
  def server_info(conn) do
    case call(conn, :server_info, 5_000) do
      {:error, _} = error -> error
      info -> {:ok, info}
    end
  end

  ## Topology

  @doc """
  Declares a queue and returns its name and counts.

  An empty name asks the broker to pick a unique one, which `%FireMQ.QueueInfo{name: name}`
  reports.

  ## Options

    * `:durable` - survives a broker restart. Defaults to `true`.
    * `:exclusive` - used only by this connection and deleted when it closes.
    * `:auto_delete` - deleted when its last subscription is removed.
    * `:passive` - only check that the queue exists.
    * `:temporary` - shorthand for `durable: false, exclusive: true, auto_delete: true`.
    * `:args` - queue arguments with RabbitMQ's names, such as `"x-message-ttl"`,
      `"x-max-length"`, `"x-overflow"` or `"x-dead-letter-exchange"`.

  ## Examples

      {:ok, %FireMQ.QueueInfo{name: "jobs"}} = FireMQ.declare_queue(conn, "jobs")
      {:ok, %FireMQ.QueueInfo{name: "amq.gen-" <> _}} =
        FireMQ.declare_queue(conn, "", temporary: true)
  """
  @spec declare_queue(conn(), String.t(), keyword()) :: {:ok, QueueInfo.t()} | {:error, Error.t()}
  def declare_queue(conn, name, opts \\ []) do
    {temporary?, opts} = Keyword.pop(opts, :temporary, false)

    defaults =
      if temporary?,
        do: [durable: false, exclusive: true, auto_delete: true],
        else: [durable: true]

    opts = Keyword.merge(defaults, opts)

    header = %{
      name: name,
      durable: flag(opts, :durable),
      exclusive: flag(opts, :exclusive),
      auto_delete: flag(opts, :auto_delete),
      passive: flag(opts, :passive),
      args: table(opts[:args])
    }

    request(conn, :declare_queue, header, {:queue_info, name}, opts)
  end

  @doc """
  Deletes a queue and returns how many messages it held.

  ## Options

    * `:if_unused` - fail with code 406 when the queue has consumers.
    * `:if_empty` - fail with code 406 when the queue has messages.
  """
  @spec delete_queue(conn(), String.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def delete_queue(conn, name, opts \\ []) do
    header = %{name: name, if_unused: flag(opts, :if_unused), if_empty: flag(opts, :if_empty)}
    request(conn, :delete_queue, header, :messages, opts)
  end

  @doc "Removes every ready message from a queue and returns how many were removed."
  @spec purge_queue(conn(), String.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def purge_queue(conn, name, opts \\ []) do
    request(conn, :purge_queue, %{name: name}, :messages, opts)
  end

  @doc """
  Declares an exchange of type `direct`, `fanout`, `topic` or `headers`. The type can be
  a string or an atom.

  ## Options

    * `:durable` - survives a broker restart. Defaults to `true`.
    * `:auto_delete` - deleted when its last binding is removed.
    * `:internal` - cannot be published to directly.
    * `:passive` - only check that the exchange exists.
    * `:args` - exchange arguments, such as `"alternate-exchange"`.
  """
  @spec declare_exchange(conn(), String.t(), String.t() | atom(), keyword()) ::
          :ok | {:error, Error.t()}
  def declare_exchange(conn, name, type, opts \\ []) do
    opts = Keyword.put_new(opts, :durable, true)

    header = %{
      name: name,
      type: to_string(type),
      durable: flag(opts, :durable),
      auto_delete: flag(opts, :auto_delete),
      internal: flag(opts, :internal),
      passive: flag(opts, :passive),
      args: table(opts[:args])
    }

    request(conn, :declare_exchange, header, :ok, opts)
  end

  @doc """
  Deletes an exchange.

  ## Options

    * `:if_unused` - fail with code 406 when the exchange has bindings.
  """
  @spec delete_exchange(conn(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def delete_exchange(conn, name, opts \\ []) do
    request(conn, :delete_exchange, %{name: name, if_unused: flag(opts, :if_unused)}, :ok, opts)
  end

  @doc """
  Binds `queue` to `exchange` with a routing key or pattern.

  ## Options

    * `:args` - binding arguments, such as `"x-match"` for headers exchanges.
  """
  @spec bind_queue(conn(), String.t(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def bind_queue(conn, queue, exchange, key \\ "", opts \\ []) do
    header = %{exchange: exchange, queue: queue, key: key, args: table(opts[:args])}
    request(conn, :bind, header, :ok, opts)
  end

  @doc "Removes a binding made with `bind_queue/5`. Takes the same options."
  @spec unbind_queue(conn(), String.t(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def unbind_queue(conn, queue, exchange, key \\ "", opts \\ []) do
    header = %{exchange: exchange, queue: queue, key: key, args: table(opts[:args])}
    request(conn, :unbind, header, :ok, opts)
  end

  @doc """
  Binds exchange `destination` to exchange `source`: messages that `source` routes with
  `key` also go to `destination`. Accepts the same options as `bind_queue/5`.
  """
  @spec bind_exchange(conn(), String.t(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def bind_exchange(conn, destination, source, key \\ "", opts \\ []) do
    header = %{exchange: source, to_exchange: destination, key: key, args: table(opts[:args])}
    request(conn, :bind, header, :ok, opts)
  end

  ## Publishing

  @doc """
  Publishes a message and waits for the broker's confirm.

  Use exchange `""` to send straight to the queue named by `key`. The body is a binary or
  iodata.

  ## Options

    * `:persistent` - write the message to disk. Defaults to `true`.
    * `:mandatory` - fail with kind `:unroutable` when no queue receives the message.
    * `:confirm` - wait for the broker's confirm. Defaults to `true`. When `false`,
      `publish/5` returns once the message is written to the socket.
    * `:properties` - a map or keyword list of message properties: `:content_type`,
      `:content_encoding`, `:correlation_id`, `:reply_to`, `:expiration` (milliseconds),
      `:message_id`, `:timestamp` (seconds since the Unix epoch, or a `DateTime`), `:type`,
      `:user_id`, `:app_id` and `:priority` (0 to 255).
    * `:headers` - application headers, a map with string keys and any MessagePack value.

  ## Errors

    * kind `:unroutable` - a `mandatory` message reached no queue.
    * kind `:nacked` - the broker rejected the message, for example because a queue with
      `x-overflow: reject-publish` is full.

  ## Examples

      :ok = FireMQ.publish(conn, "", "jobs", "hello")

      :ok =
        FireMQ.publish(conn, "orders", "order.created", Jason.encode!(order),
          properties: [content_type: "application/json", message_id: "o-17"],
          headers: %{"trace" => "abc"}
        )
  """
  @spec publish(conn(), String.t(), String.t(), iodata(), keyword()) :: :ok | {:error, Error.t()}
  def publish(conn, exchange, key, body, opts \\ []) do
    header = %{
      exchange: exchange,
      key: key,
      mandatory: flag(opts, :mandatory),
      persistent: Keyword.get(opts, :persistent, true) == true,
      props: properties(opts[:properties]),
      headers: table(opts[:headers])
    }

    confirm? = Keyword.get(opts, :confirm, true) != false
    call(conn, {:publish, header, body, confirm?}, timeout(opts))
  end

  ## Consuming

  @doc """
  Starts consuming from a queue. Messages arrive in the calling process's mailbox as

      {:firemq, sub_ref, %FireMQ.Message{}}

  When the broker ends the subscription (for example because the queue was deleted) or
  the connection closes, the process receives
  `{:firemq_cancelled, sub_ref, %FireMQ.Error{}}`. The subscription ends when the
  subscribing process exits; messages it had not settled go back to the queue.

  Acknowledge each message with `ack/1` or `nack/2`. The broker sends at most `:prefetch`
  unacknowledged messages; the client grants more as you settle them, half the window at
  a time.

  ## Options

    * `:prefetch` - how many unacknowledged messages the broker may send at once.
      Defaults to `100`.
    * `:no_ack` - treat messages as acknowledged when sent. Faster, but messages are lost
      if the consumer crashes.
    * `:exclusive` - fail with code 405 if the queue has other consumers, and block new ones.
    * `:tag` - consumer tag shown in the management UI.
    * `:args` - consumer arguments.
  """
  @spec subscribe(conn(), String.t(), keyword()) :: {:ok, sub_ref()} | {:error, Error.t()}
  def subscribe(conn, queue, opts \\ []) do
    sub_opts = [
      prefetch: Keyword.get(opts, :prefetch, 100),
      no_ack: flag(opts, :no_ack),
      exclusive: flag(opts, :exclusive),
      tag: Keyword.get(opts, :tag, ""),
      args: table(opts[:args])
    ]

    call(conn, {:subscribe, queue, sub_opts, self()}, timeout(opts))
  end

  @doc """
  Stops a subscription. Messages already received can still be acknowledged; messages
  that were on their way go back to the queue.
  """
  @spec unsubscribe(conn(), sub_ref(), keyword()) :: :ok | {:error, Error.t()}
  def unsubscribe(conn, sub_ref, opts \\ []) when is_reference(sub_ref) do
    call(conn, {:unsubscribe, sub_ref}, timeout(opts))
  end

  @doc """
  Acknowledges a message. Does nothing for messages received with `no_ack: true` or
  already settled.
  """
  @spec ack(Message.t()) :: :ok | {:error, Error.t()}
  def ack(%Message{} = message), do: settle(message, :ack, false)

  @doc """
  Rejects a message. With `requeue` set to `true` it is delivered again, with
  `redelivered: true`. Otherwise it is dead-lettered when the queue has a dead-letter
  exchange, and dropped if not.
  """
  @spec nack(Message.t(), boolean()) :: :ok | {:error, Error.t()}
  def nack(%Message{} = message, requeue) when is_boolean(requeue),
    do: settle(message, :nack, requeue)

  defp settle(%Message{no_ack: true}, _op, _requeue), do: :ok

  defp settle(%Message{conn: nil}, _op, _requeue),
    do: {:error, Error.closed("message has no connection")}

  defp settle(%Message{conn: conn, delivery_tag: tag}, op, requeue) do
    call(conn, {:settle, tag, op, requeue}, @timeout)
  end

  @doc """
  Takes one message from a queue. Returns `{:ok, nil}` when the queue is empty.

  ## Options

    * `:no_ack` - treat the message as acknowledged once sent. Defaults to `false`, in
      which case settle it with `ack/1` or `nack/2`.
  """
  @spec get(conn(), String.t(), keyword()) :: {:ok, Message.t() | nil} | {:error, Error.t()}
  def get(conn, queue, opts \\ []) do
    no_ack = flag(opts, :no_ack)
    request(conn, :get, %{queue: queue, no_ack: no_ack}, {:get, no_ack}, opts)
  end

  ## Bang variants

  @doc "Like `declare_queue/3`, but returns the `FireMQ.QueueInfo` or raises."
  def declare_queue!(conn, name, opts \\ []), do: unwrap!(declare_queue(conn, name, opts))

  @doc "Like `delete_queue/3`, but returns the message count or raises."
  def delete_queue!(conn, name, opts \\ []), do: unwrap!(delete_queue(conn, name, opts))

  @doc "Like `purge_queue/3`, but returns the message count or raises."
  def purge_queue!(conn, name, opts \\ []), do: unwrap!(purge_queue(conn, name, opts))

  @doc "Like `declare_exchange/4`, but raises on error."
  def declare_exchange!(conn, name, type, opts \\ []),
    do: unwrap!(declare_exchange(conn, name, type, opts))

  @doc "Like `delete_exchange/3`, but raises on error."
  def delete_exchange!(conn, name, opts \\ []), do: unwrap!(delete_exchange(conn, name, opts))

  @doc "Like `bind_queue/5`, but raises on error."
  def bind_queue!(conn, queue, exchange, key \\ "", opts \\ []),
    do: unwrap!(bind_queue(conn, queue, exchange, key, opts))

  @doc "Like `unbind_queue/5`, but raises on error."
  def unbind_queue!(conn, queue, exchange, key \\ "", opts \\ []),
    do: unwrap!(unbind_queue(conn, queue, exchange, key, opts))

  @doc "Like `bind_exchange/5`, but raises on error."
  def bind_exchange!(conn, destination, source, key \\ "", opts \\ []),
    do: unwrap!(bind_exchange(conn, destination, source, key, opts))

  @doc "Like `publish/5`, but raises on error."
  def publish!(conn, exchange, key, body, opts \\ []),
    do: unwrap!(publish(conn, exchange, key, body, opts))

  @doc "Like `subscribe/3`, but returns the subscription reference or raises."
  def subscribe!(conn, queue, opts \\ []), do: unwrap!(subscribe(conn, queue, opts))

  @doc "Like `unsubscribe/3`, but raises on error."
  def unsubscribe!(conn, sub_ref, opts \\ []), do: unwrap!(unsubscribe(conn, sub_ref, opts))

  @doc "Like `get/3`, but returns the message (or `nil`) or raises."
  def get!(conn, queue, opts \\ []), do: unwrap!(get(conn, queue, opts))

  @doc "Like `ack/1`, but raises on error."
  def ack!(message), do: unwrap!(ack(message))

  @doc "Like `nack/2`, but raises on error."
  def nack!(message, requeue), do: unwrap!(nack(message, requeue))

  defp unwrap!(:ok), do: :ok
  defp unwrap!({:ok, value}), do: value
  defp unwrap!({:error, %Error{} = error}), do: raise(error)

  ## Helpers

  defp request(conn, op, header, kind, opts) do
    call(conn, {:request, op, header, kind}, timeout(opts))
  end

  defp call(conn, message, timeout) do
    GenServer.call(conn, message, timeout)
  catch
    :exit, {:timeout, _} ->
      {:error, %Error{kind: :timeout, message: "no reply from the broker within #{timeout} ms"}}

    :exit, {{:shutdown, %Error{} = error}, _} ->
      {:error, error}

    :exit, _ ->
      {:error, Error.closed("connection is closed")}
  end

  defp timeout(opts), do: Keyword.get(opts, :timeout, @timeout)

  defp flag(opts, key), do: Keyword.get(opts, key, false) == true

  defp table(nil), do: %{}
  defp table(map) when is_map(map), do: map
  defp table(list) when is_list(list), do: Map.new(list)

  @property_names Map.new(Connection.property_names(), &{Atom.to_string(&1), &1})

  defp properties(nil), do: %{}

  defp properties(props) when is_map(props) or is_list(props) do
    for {key, value} <- props, value != nil, into: %{} do
      name = property_name(key)
      {name, property_value(name, value)}
    end
  end

  defp property_name(key) when is_atom(key), do: property_name(Atom.to_string(key))

  defp property_name(key) when is_binary(key) do
    case @property_names do
      %{^key => name} ->
        name

      _ ->
        raise ArgumentError,
              "unknown message property #{inspect(key)}; expected one of " <>
                Enum.join(Connection.property_names(), ", ")
    end
  end

  defp property_value(:timestamp, %DateTime{} = value), do: DateTime.to_unix(value)

  defp property_value(name, value)
       when name in [:expiration, :timestamp, :priority] and is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> int
      _ -> raise ArgumentError, "message property #{name} must be a non-negative integer"
    end
  end

  defp property_value(_name, value), do: value
end
