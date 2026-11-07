defmodule FireMQ.Connection do
  @moduledoc """
  A connection to FireMQ: a process that owns one TCP or TLS socket.

  The connection sends requests and matches replies by request id, tracks publisher
  confirms, hands deliveries to subscribers, grants credit and exchanges heartbeats. Use
  the functions in `FireMQ` to talk to it.

  ## Starting a connection

  `FireMQ.connect/2` opens a connection that belongs to the calling process: it closes
  when that process exits. Its exit does not affect the caller.

  `start_link/1` opens a connection linked to the caller, which suits a supervisor:

      children = [
        {FireMQ.Connection, url: "fire://guest:guest@localhost:5673", name: MyApp.FireMQ}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Then call `FireMQ.publish(MyApp.FireMQ, "", "jobs", "hello")`.

  When the broker closes the connection or the socket fails, the process exits with
  `{:shutdown, %FireMQ.Error{}}` and a supervisor starts a new one. The connection does
  not reconnect on its own, and subscriptions do not survive a restart: subscribers
  receive `{:firemq_cancelled, sub_ref, %FireMQ.Error{}}` and must subscribe again.

  ## Options

    * `:url` - the connection URL. Defaults to `"fire://localhost"`.
    * `:name` - registers the process under a name (`start_link/1` only).
    * `:heartbeat` - desired heartbeat interval in seconds; `0` disables heartbeats.
      Defaults to `30`.
    * `:connection_name` - a name shown in the management UI.
    * `:connect_timeout` - how long to wait for the TCP connection and the handshake, in
      milliseconds. Defaults to `10_000`.
    * `:ssl` - options for `:ssl.connect/3` on `fires://` URLs, merged over the defaults
      (`verify: :verify_peer` with the system CA store). For example
      `ssl: [cacertfile: "ca.pem"]` or, for local development only,
      `ssl: [verify: :verify_none]`.
  """

  use GenServer

  require Logger

  alias FireMQ.{Error, Frame, Message, QueueInfo, URL}

  @version Mix.Project.config()[:version]
  @default_heartbeat 30
  @connect_timeout 10_000
  @close_timeout 2_000
  @active 100

  # On OTP 26 and later, init/1 may return {:error, reason}: start_link/1 returns the error
  # and the process exits normally, so a linked caller does not crash. Older releases only
  # know {:stop, reason}.
  @init_error if String.to_integer(System.otp_release()) >= 26, do: :error, else: :stop
  @property_names ~w(content_type content_encoding correlation_id reply_to expiration
                     message_id timestamp type user_id app_id priority)a
  @property_keys Map.new(@property_names, &{Atom.to_string(&1), &1})

  @doc """
  Starts a connection linked to the caller. See the module documentation for options.

  Returns `{:error, %FireMQ.Error{}}` when the broker cannot be reached or refuses the
  handshake.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {server_opts, opts} = Keyword.split(opts, [:name, :debug, :spawn_opt, :hibernate_after])
    GenServer.start_link(__MODULE__, {:linked, opts}, server_opts)
  end

  @doc false
  # Starts a connection owned by `owner`: not linked, closed when `owner` exits.
  def start_owned(owner, opts) do
    GenServer.start(__MODULE__, {{:owner, owner}, opts})
  end

  @doc false
  # Message property names, in the order the spec lists them.
  def property_names, do: @property_names

  ## Callbacks

  @impl true
  def init({mode, opts}) do
    Process.flag(:trap_exit, true)

    with {:ok, url} <- URL.parse(Keyword.get(opts, :url, "fire://localhost")),
         :ok <- check_transport(url),
         {:ok, state} <- handshake(url, opts) do
      owner =
        case mode do
          {:owner, pid} -> Process.monitor(pid)
          :linked -> nil
        end

      {:ok, schedule_heartbeat(%{state | owner: owner})}
    else
      {:error, %Error{} = error} -> {@init_error, error}
    end
  end

  @impl true
  def handle_call(_request, _from, %{closing: {_, _}} = state) do
    {:reply, {:error, Error.closed("connection is closing")}, state}
  end

  def handle_call({:request, op, header, kind}, from, state) do
    {:noreply, request(state, op, header, {from, kind})}
  end

  def handle_call({:publish, header, body, confirm?}, from, state) do
    seq = if confirm?, do: state.next_seq, else: 0
    frame = Frame.encode_body(:publish, compact(Map.put(header, :seq, seq)), body)
    size = IO.iodata_length(frame)

    cond do
      size > state.max_frame ->
        message =
          "frame of #{size} bytes exceeds the broker's maximum of #{state.max_frame} bytes"

        {:reply, {:error, %Error{kind: :protocol, message: message}}, state}

      confirm? ->
        state = transmit(state, frame)
        confirms = :gb_trees.insert(seq, from, state.confirms)
        {:noreply, %{state | confirms: confirms, next_seq: seq + 1}}

      true ->
        {:reply, :ok, transmit(state, frame)}
    end
  end

  def handle_call({:subscribe, queue, opts, pid}, from, state) do
    sub = state.next_sub
    ref = make_ref()
    window = max(Keyword.get(opts, :prefetch, 100), 1)
    no_ack = Keyword.get(opts, :no_ack, false)

    entry = %{
      ref: ref,
      pid: pid,
      monitor: Process.monitor(pid),
      no_ack: no_ack,
      window: window,
      released: 0
    }

    header = %{
      sub: sub,
      queue: queue,
      credit: window,
      no_ack: no_ack,
      exclusive: Keyword.get(opts, :exclusive, false),
      tag: Keyword.get(opts, :tag, ""),
      args: Keyword.get(opts, :args, %{})
    }

    state = %{
      state
      | next_sub: sub + 1,
        subs: Map.put(state.subs, sub, entry),
        refs: Map.put(state.refs, ref, sub)
    }

    {:noreply, request(state, :subscribe, header, {from, {:subscribe, sub}})}
  end

  def handle_call({:unsubscribe, ref}, from, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, sub} -> {:noreply, cancel(state, sub, from)}
      :error -> {:reply, :ok, state}
    end
  end

  def handle_call({:settle, tag, op, requeue}, _from, state) do
    case Map.pop(state.unsettled, tag) do
      {nil, _} ->
        {:reply, :ok, state}

      {owner, unsettled} ->
        header = if op == :nack, do: %{tag: tag, requeue: requeue}, else: %{tag: tag}
        state = %{state | unsettled: unsettled} |> send_frame(op, header) |> release(owner)
        {:reply, :ok, state}
    end
  end

  def handle_call(:server_info, _from, state) do
    {:reply, state.welcome, state}
  end

  def handle_call(:close, from, state) do
    state = send_frame(state, :close, %{code: 200, msg: "client closed"})
    timer = Process.send_after(self(), :close_timeout, @close_timeout)
    {:noreply, %{state | closing: {from, timer}}}
  end

  @impl true
  def handle_info({tag, socket, data}, %{socket: socket} = state) when tag in [:tcp, :ssl] do
    state = %{state | last_recv: now()}

    case Frame.decode_body(data) do
      {:ok, {op, header, payload}} ->
        handle_frame(op, header, payload, state)

      {:error, reason} ->
        Logger.warning("FireMQ: ignoring undecodable frame: #{reason}")
        {:noreply, state}
    end
  end

  def handle_info({tag, socket}, %{socket: socket} = state)
      when tag in [:tcp_passive, :ssl_passive] do
    _ = setopts(state.transport, socket, active: @active)
    {:noreply, state}
  end

  def handle_info({tag, socket}, %{socket: socket} = state)
      when tag in [:tcp_closed, :ssl_closed] do
    stop(state, Error.closed("the broker closed the connection"))
  end

  def handle_info({tag, socket, reason}, %{socket: socket} = state)
      when tag in [:tcp_error, :ssl_error] do
    stop(state, Error.closed("connection lost: #{format_reason(reason)}"))
  end

  def handle_info({:send_failed, reason}, state) do
    stop(state, Error.closed("connection lost: #{format_reason(reason)}"))
  end

  def handle_info(:heartbeat, state) do
    now = now()
    interval = state.heartbeat * 1000

    if now - state.last_recv > 2 * interval do
      stop(state, Error.closed("the broker missed heartbeats"))
    else
      state =
        if now - state.last_sent >= div(interval, 4),
          do: send_frame(state, :ping, %{}),
          else: state

      {:noreply, schedule_heartbeat(state)}
    end
  end

  def handle_info(:close_timeout, %{closing: {_, _}} = state) do
    stop(state, Error.closed("client closed the connection"), :normal)
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner: ref} = state) do
    state = send_frame(state, :close, %{code: 200, msg: "owner exited"})
    stop(state, Error.closed("the owner of the connection exited"), :normal)
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Enum.find(state.subs, fn {_, entry} -> entry.monitor == monitor end) do
      {sub, _entry} -> {:noreply, state |> cancel(sub, nil) |> requeue_unsettled(sub)}
      nil -> {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{socket: nil}), do: :ok

  def terminate(_reason, state) do
    state = send_frame(state, :close, %{code: 200, msg: "client closed"})
    shutdown(state, Error.closed("connection is closed"))
    :ok
  end

  ## Handshake

  defp check_transport(%URL{transport: t}) when t in [:tcp, :tls], do: :ok

  defp check_transport(_url) do
    message = "the Elixir client connects with fire:// or fires://; WebSocket is for browsers"
    {:error, %Error{kind: :url, message: message}}
  end

  defp handshake(url, opts) do
    timeout = Keyword.get(opts, :connect_timeout, @connect_timeout)
    heartbeat = Keyword.get(opts, :heartbeat, @default_heartbeat)
    transport = if url.transport == :tls, do: :ssl, else: :gen_tcp
    hello = Frame.encode(:hello, hello(url, heartbeat, opts))

    case open(transport, url, opts, timeout) do
      {:ok, socket} ->
        case greet(transport, socket, hello, timeout) do
          {:ok, state} ->
            {:ok, state}

          {:error, reason} ->
            transport.close(socket)
            {:error, handshake_error(reason, url, timeout)}
        end

      {:error, :timeout} ->
        {:error, %Error{kind: :timeout, message: "connecting to #{authority(url)} timed out"}}

      {:error, reason} ->
        {:error, Error.closed("cannot connect to #{authority(url)}: #{format_reason(reason)}")}
    end
  end

  defp greet(transport, socket, hello, timeout) do
    with :ok <- transport.send(socket, [Frame.magic() | hello]),
         :ok <- setopts(transport, socket, packet: 4, packet_size: Frame.max_hello_frame()),
         {:ok, data} <- transport.recv(socket, 0, timeout) do
      case Frame.decode_body(data) do
        {:ok, {:welcome, welcome, _}} ->
          welcome(transport, socket, welcome)

        {:ok, {:error, error, _}} ->
          {:error, server_error(error)}

        {:ok, {op, _, _}} ->
          {:error, %Error{kind: :protocol, message: "expected welcome, got #{inspect(op)}"}}

        {:error, reason} ->
          {:error, %Error{kind: :protocol, message: reason}}
      end
    end
  end

  defp handshake_error(%Error{} = error, _url, _timeout), do: error

  defp handshake_error(:timeout, url, timeout),
    do: %Error{kind: :timeout, message: "no welcome from #{authority(url)} within #{timeout} ms"}

  defp handshake_error(:closed, _url, _timeout),
    do: Error.closed("the broker closed the connection during the handshake")

  defp handshake_error(reason, url, _timeout),
    do: Error.closed("handshake with #{authority(url)} failed: #{format_reason(reason)}")

  defp open(:gen_tcp, url, _opts, timeout) do
    :gen_tcp.connect(URL.connect_host(url), url.port, socket_options(), timeout)
  end

  defp open(:ssl, url, opts, timeout) do
    ssl_opts =
      Keyword.merge(tls_defaults(Keyword.get(opts, :ssl, [])), Keyword.get(opts, :ssl, []))

    :ssl.connect(URL.connect_host(url), url.port, socket_options() ++ ssl_opts, timeout)
  end

  defp socket_options, do: [:binary, active: false, packet: :raw, nodelay: true, keepalive: true]

  # :ssl.connect/4 derives SNI and the hostname check from the host argument: a host name
  # is sent as SNI and checked against the certificate, an IP address is checked against
  # its IP entries. Setting server_name_indication: :disable would skip the check.
  defp tls_defaults(user_opts) do
    cacerts =
      if Keyword.has_key?(user_opts, :cacertfile) or Keyword.has_key?(user_opts, :cacerts) do
        []
      else
        try do
          [cacerts: :public_key.cacerts_get()]
        rescue
          _ -> []
        end
      end

    [
      verify: :verify_peer,
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ] ++ cacerts
  end

  defp hello(url, heartbeat, opts) do
    client = %{name: "firemq-elixir", version: @version, lang: "elixir"}

    client =
      case Keyword.get(opts, :connection_name) do
        nil -> client
        name -> Map.put(client, :connection_name, name)
      end

    %{
      ver: 1,
      vhost: url.vhost,
      auth: %{mech: "plain", user: url.user, pass: url.pass},
      heartbeat: heartbeat,
      client: client,
      caps: ["confirms", "credit"]
    }
  end

  defp welcome(transport, socket, w) do
    max_frame = max(int(w["max_frame"], 16 * 1024 * 1024), Frame.max_hello_frame())
    :ok = setopts(transport, socket, packet_size: max_frame, active: @active)

    welcome = %{
      version: int(w["ver"], 1),
      server: w["server"] || %{},
      conn_id: w["conn_id"] || "",
      heartbeat: int(w["heartbeat"], 0),
      max_frame: max_frame,
      caps: w["caps"] || []
    }

    {:ok,
     %{
       transport: transport,
       socket: socket,
       welcome: welcome,
       max_frame: max_frame,
       heartbeat: welcome.heartbeat,
       last_sent: now(),
       last_recv: now(),
       next_rid: 1,
       next_seq: 1,
       next_sub: 1,
       requests: %{},
       confirms: :gb_trees.empty(),
       returned: %{},
       subs: %{},
       refs: %{},
       gone: %{},
       unsettled: %{},
       owner: nil,
       closing: nil
     }}
  end

  ## Frames from the broker

  defp handle_frame(:ok, header, _payload, state) do
    {:noreply, reply(state, int(header["rid"], 0), {:ok, header})}
  end

  defp handle_frame(:get_ok, header, payload, state) do
    {:noreply, reply(state, int(header["rid"], 0), {:get_ok, header, payload})}
  end

  defp handle_frame(:error, header, _payload, state) do
    error = server_error(header)

    state =
      case header["rid"] do
        rid when is_integer(rid) ->
          reply(state, rid, {:error, error})

        _ ->
          unless header["fatal"] == true do
            Logger.warning("FireMQ: the broker reported an error: #{Exception.message(error)}")
          end

          state
      end

    if header["fatal"] == true, do: stop(state, error), else: {:noreply, state}
  end

  defp handle_frame(:confirm, %{"nack" => true} = header, _payload, state) do
    seq = int(header["seq"], 0)
    returned = Map.delete(state.returned, seq)

    case :gb_trees.lookup(seq, state.confirms) do
      {:value, from} ->
        message = "publish rejected: #{header["msg"] || ""}"

        GenServer.reply(
          from,
          {:error, %Error{kind: :nacked, code: int(header["code"], 0), message: message}}
        )

        {:noreply, %{state | confirms: :gb_trees.delete(seq, state.confirms), returned: returned}}

      :none ->
        {:noreply, %{state | returned: returned}}
    end
  end

  defp handle_frame(:confirm, %{"upto" => upto}, _payload, state) when is_integer(upto) do
    {:noreply, confirm_upto(state, upto)}
  end

  defp handle_frame(:return, header, _payload, state) do
    case int(header["seq"], 0) do
      0 -> {:noreply, state}
      seq -> {:noreply, %{state | returned: Map.put(state.returned, seq, header)}}
    end
  end

  defp handle_frame(:deliver, header, payload, state) do
    sub = int(header["sub"], 0)
    tag = int(header["tag"], 0)

    case state.subs do
      %{^sub => entry} ->
        send(entry.pid, {:firemq, entry.ref, message(header, payload, entry.no_ack)})

        if entry.no_ack do
          {:noreply, release(state, sub)}
        else
          {:noreply, %{state | unsettled: Map.put(state.unsettled, tag, sub)}}
        end

      _ ->
        # A delivery that crossed our unsubscribe. Requeue it so another consumer gets it.
        case state.gone do
          %{^sub => false} -> {:noreply, send_frame(state, :nack, %{tag: tag, requeue: true})}
          _ -> {:noreply, state}
        end
    end
  end

  defp handle_frame(:cancelled, header, _payload, state) do
    sub = int(header["sub"], 0)

    case Map.pop(state.subs, sub) do
      {nil, _} ->
        {:noreply, state}

      {entry, subs} ->
        Process.demonitor(entry.monitor, [:flush])
        error = server_error(header)
        send(entry.pid, {:firemq_cancelled, entry.ref, error})
        {:noreply, %{state | subs: subs, refs: Map.delete(state.refs, entry.ref)}}
    end
  end

  defp handle_frame(:ping, _header, _payload, state) do
    {:noreply, send_frame(state, :pong, %{})}
  end

  defp handle_frame(:close, _header, _payload, %{closing: {_, _}} = state) do
    stop(state, Error.closed("client closed the connection"), :normal)
  end

  defp handle_frame(:close, header, _payload, state) do
    state = send_frame(state, :close, %{code: 200})
    code = int(header["code"], 0)

    message =
      case header["msg"] do
        msg when is_binary(msg) and msg != "" -> "closed by the broker: #{msg}"
        _ -> "closed by the broker"
      end

    stop(state, Error.closed(message, code))
  end

  defp handle_frame(_op, _header, _payload, state), do: {:noreply, state}

  ## Requests and replies

  defp request(state, op, header, waiter) do
    rid = state.next_rid
    state = send_frame(state, op, Map.put(header, :rid, rid))
    %{state | next_rid: rid + 1, requests: Map.put(state.requests, rid, waiter)}
  end

  defp reply(state, rid, result) do
    case Map.pop(state.requests, rid) do
      {nil, _} ->
        state

      {{from, kind}, requests} ->
        {response, state} = finish(kind, result, %{state | requests: requests})
        if from, do: GenServer.reply(from, response)
        state
    end
  end

  defp finish({:subscribe, sub}, {:ok, _}, state) do
    case state.subs do
      %{^sub => entry} -> {{:ok, entry.ref}, state}
      _ -> {{:error, Error.closed("subscription was cancelled")}, state}
    end
  end

  defp finish({:subscribe, sub}, {:error, error}, state) do
    case Map.pop(state.subs, sub) do
      {nil, _} ->
        {{:error, error}, state}

      {entry, subs} ->
        Process.demonitor(entry.monitor, [:flush])
        {{:error, error}, %{state | subs: subs, refs: Map.delete(state.refs, entry.ref)}}
    end
  end

  defp finish({:unsubscribe, sub}, result, state) do
    state = %{state | gone: Map.delete(state.gone, sub)}

    case result do
      {:error, error} -> {{:error, error}, state}
      _ -> {:ok, state}
    end
  end

  defp finish(_kind, {:error, error}, state), do: {{:error, error}, state}

  defp finish(:ok, _result, state), do: {:ok, state}

  defp finish({:queue_info, name}, {:ok, h}, state) do
    info = %QueueInfo{
      name: h["name"] || name,
      messages: int(h["messages"], 0),
      consumers: int(h["consumers"], 0)
    }

    {{:ok, info}, state}
  end

  defp finish(:messages, {:ok, h}, state), do: {{:ok, int(h["messages"], 0)}, state}

  defp finish({:get, _no_ack}, {:ok, _empty}, state), do: {{:ok, nil}, state}

  defp finish({:get, no_ack}, {:get_ok, header, payload}, state) do
    message = message(header, payload, no_ack)

    state =
      if no_ack,
        do: state,
        else: %{state | unsettled: Map.put(state.unsettled, message.delivery_tag, :get)}

    {{:ok, message}, state}
  end

  defp finish(_kind, {:get_ok, _, _}, state) do
    {{:error, %Error{kind: :protocol, message: "unexpected get_ok"}}, state}
  end

  defp finish(_kind, {:ok, _}, state), do: {:ok, state}

  defp confirm_upto(state, upto) do
    if :gb_trees.is_empty(state.confirms) do
      state
    else
      {seq, from, rest} = :gb_trees.take_smallest(state.confirms)

      if seq > upto do
        state
      else
        {returned, map} = Map.pop(state.returned, seq)

        result =
          case returned do
            nil ->
              :ok

            r ->
              message =
                "message returned: no queue is bound to exchange '#{r["exchange"] || ""}' with key '#{r["key"] || ""}'"

              {:error, %Error{kind: :unroutable, code: int(r["code"], 312), message: message}}
          end

        GenServer.reply(from, result)
        confirm_upto(%{state | confirms: rest, returned: map}, upto)
      end
    end
  end

  ## Subscriptions and credit

  # Removes a subscription and asks the broker to stop it. `from` is nil when the
  # subscriber exited.
  defp cancel(state, sub, from) do
    {entry, subs} = Map.pop!(state.subs, sub)
    Process.demonitor(entry.monitor, [:flush])

    state = %{
      state
      | subs: subs,
        refs: Map.delete(state.refs, entry.ref),
        gone: Map.put(state.gone, sub, entry.no_ack)
    }

    request(state, :unsubscribe, %{sub: sub}, {from, {:unsubscribe, sub}})
  end

  defp requeue_unsettled(state, sub) do
    {mine, rest} = Map.split_with(state.unsettled, fn {_tag, owner} -> owner == sub end)

    mine
    |> Map.keys()
    |> Enum.sort()
    |> Enum.reduce(%{state | unsettled: rest}, fn tag, state ->
      send_frame(state, :nack, %{tag: tag, requeue: true})
    end)
  end

  # Records one handled message and grants credit back in batches of half the window.
  defp release(state, sub) do
    case state.subs do
      %{^sub => entry} ->
        released = entry.released + 1

        if released >= max(div(entry.window, 2), 1) do
          state = send_frame(state, :credit, %{sub: sub, credit: released})
          %{state | subs: Map.put(state.subs, sub, %{entry | released: 0})}
        else
          %{state | subs: Map.put(state.subs, sub, %{entry | released: released})}
        end

      _ ->
        state
    end
  end

  ## Messages

  defp message(header, payload, no_ack) do
    %Message{
      body: payload,
      exchange: header["exchange"] || "",
      routing_key: header["key"] || "",
      properties: properties(header["props"]),
      headers: if(is_map(header["headers"]), do: header["headers"], else: %{}),
      redelivered: header["redelivered"] == true,
      delivery_tag: int(header["tag"], 0),
      no_ack: no_ack,
      conn: self()
    }
  end

  defp properties(props) when is_map(props) do
    for {key, value} <- props,
        value != nil,
        atom = @property_keys[key],
        into: %{},
        do: {atom, value}
  end

  defp properties(_), do: %{}

  ## Socket

  defp send_frame(state, op, header) do
    transmit(state, Frame.encode_body(op, compact(header)))
  end

  # Omitted header fields take their zero value, so leave out the empty ones.
  defp compact(header) do
    for {key, value} <- header,
        value not in [nil, false, "", []],
        value != %{},
        into: %{},
        do: {key, value}
  end

  defp transmit(%{socket: nil} = state, _frame), do: state

  defp transmit(state, frame) do
    case state.transport.send(state.socket, frame) do
      :ok ->
        %{state | last_sent: now()}

      {:error, reason} ->
        send(self(), {:send_failed, reason})
        state
    end
  end

  defp setopts(:gen_tcp, socket, opts), do: :inet.setopts(socket, opts)
  defp setopts(:ssl, socket, opts), do: :ssl.setopts(socket, opts)

  defp schedule_heartbeat(%{heartbeat: 0} = state), do: state

  defp schedule_heartbeat(state) do
    Process.send_after(self(), :heartbeat, max(div(state.heartbeat * 1000, 2), 500))
    state
  end

  ## Shutdown

  defp stop(state, error, reason \\ nil) do
    state = shutdown(state, error)
    {:stop, reason || {:shutdown, error}, state}
  end

  # Fails every waiter, tells subscribers, and closes the socket.
  defp shutdown(state, error) do
    for {_rid, {from, _kind}} <- state.requests, from != nil do
      GenServer.reply(from, {:error, error})
    end

    for {_seq, from} <- :gb_trees.to_list(state.confirms) do
      GenServer.reply(from, {:error, error})
    end

    for {_sub, entry} <- state.subs do
      send(entry.pid, {:firemq_cancelled, entry.ref, error})
    end

    case state.closing do
      {from, timer} ->
        Process.cancel_timer(timer)
        GenServer.reply(from, :ok)

      nil ->
        :ok
    end

    if state.socket, do: state.transport.close(state.socket)

    %{
      state
      | socket: nil,
        requests: %{},
        confirms: :gb_trees.empty(),
        subs: %{},
        refs: %{},
        closing: nil
    }
  end

  ## Helpers

  defp server_error(header) do
    %Error{kind: :server, code: int(header["code"], 0), message: header["msg"] || ""}
  end

  defp int(value, _default) when is_integer(value), do: value
  defp int(_value, default), do: default

  defp now, do: System.monotonic_time(:millisecond)

  defp authority(%URL{host: host, port: port}) do
    if String.contains?(host, ":"), do: "[#{host}]:#{port}", else: "#{host}:#{port}"
  end

  defp format_reason(reason) when is_atom(reason) do
    case :inet.format_error(reason) do
      ~c"unknown POSIX error" ++ _ -> Atom.to_string(reason)
      text -> "#{reason} (#{text})"
    end
  end

  defp format_reason({:tls_alert, {_alert, description}}),
    do: "TLS handshake failed: #{description}"

  defp format_reason(reason), do: inspect(reason)
end
