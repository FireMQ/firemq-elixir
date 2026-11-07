# firemq-elixir

Elixir client for [FireMQ](https://github.com/FireMQ/firemq), a RabbitMQ-compatible
message broker. It speaks the native
[FireMQ protocol](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md) over
TCP or TLS and is written in pure Elixir.

Defaults favour safety: queues and exchanges are durable, messages are persistent, and
`publish` returns only after the broker confirms the message.

## Install

Add `:firemq` to your dependencies in `mix.exs`. It needs Elixir 1.15 or later.

```elixir
def deps do
  [
    {:firemq, "~> 0.1"}
  ]
end
```

## Quickstart

Start a broker, for example with `firemq --dev`, then:

```elixir
{:ok, conn} = FireMQ.connect("fire://guest:guest@localhost:5673")
{:ok, _info} = FireMQ.declare_queue(conn, "jobs")

# Returns once the broker has taken responsibility for the message.
:ok = FireMQ.publish(conn, "", "jobs", "hello")

{:ok, sub} = FireMQ.subscribe(conn, "jobs")

receive do
  {:firemq, ^sub, message} ->
    IO.puts(message.body)
    :ok = FireMQ.ack(message)
end

:ok = FireMQ.close(conn)
```

The `examples/` directory has the classic tutorials: `hello_world`, `work_queues`,
`publish_subscribe`, `routing`, `topics` and `rpc`. Run one with
`FIREMQ_URL=fire://localhost mix run examples/topics.exs`.

## Connect

```elixir
{:ok, conn} = FireMQ.connect("fire://user:password@host:5673/vhost", heartbeat: 30)
```

The URL schemes are `fire://` (TCP, port 5673 by default) and `fires://` (TLS, port
5674). User and password default to `guest`. The vhost is the URL path without its
leading `/`, percent-decoded; an empty path means vhost `/`.

Options:

| Option | Default | Meaning |
|---|---|---|
| `:heartbeat` | `30` | Heartbeat interval in seconds. `0` disables heartbeats. |
| `:connection_name` | none | Name shown in the management UI. |
| `:connect_timeout` | `10_000` | Milliseconds to wait for the TCP connection and the handshake. |
| `:ssl` | `[]` | Options for `:ssl.connect/3`, merged over the defaults. |

For `fires://`, the client verifies the broker's certificate against the system CA store.
Pass your own CA with `ssl: [cacertfile: "ca.pem"]`. For local development only, you can
skip verification with `ssl: [verify: :verify_none]`.

A connection opened with `FireMQ.connect/2` belongs to the calling process: it closes
when that process exits. To keep a connection running independently, put it under a
supervisor (see [Supervision](#supervision)).

## API

Every function takes the connection first and returns `:ok`, `{:ok, value}` or
`{:error, %FireMQ.Error{}}`. Each one also has a bang variant, such as `publish!/5`, that
returns the value or raises `FireMQ.Error`. Functions that wait for the broker accept a
`:timeout` option in milliseconds, `30_000` by default.

### Topology

```elixir
{:ok, %FireMQ.QueueInfo{name: "jobs", messages: 0, consumers: 0}} =
  FireMQ.declare_queue(conn, "jobs", args: %{"x-max-length" => 10_000})

# An empty name lets the broker pick one. A temporary queue is exclusive to this
# connection and deleted when it closes.
{:ok, %FireMQ.QueueInfo{name: reply_queue}} = FireMQ.declare_queue(conn, "", temporary: true)

:ok = FireMQ.declare_exchange(conn, "orders", :topic)
:ok = FireMQ.bind_queue(conn, "jobs", "orders", "order.*")
:ok = FireMQ.unbind_queue(conn, "jobs", "orders", "order.*")
:ok = FireMQ.bind_exchange(conn, "audit", "orders", "#")

{:ok, purged} = FireMQ.purge_queue(conn, "jobs")
{:ok, deleted} = FireMQ.delete_queue(conn, "jobs", if_empty: true)
:ok = FireMQ.delete_exchange(conn, "orders")
```

`declare_queue/3` options: `:durable` (default `true`), `:exclusive`, `:auto_delete`,
`:passive`, `:temporary` and `:args`. `declare_exchange/4` options: `:durable` (default
`true`), `:auto_delete`, `:internal`, `:passive` and `:args`. Arguments use RabbitMQ's
names, such as `x-message-ttl`, `x-dead-letter-exchange` or `x-overflow`.

### Publish

```elixir
:ok =
  FireMQ.publish(conn, "orders", "order.created", ~s({"id":42}),
    mandatory: true,
    properties: [content_type: "application/json", message_id: "o-42"],
    headers: %{"trace" => "abc", "attempt" => 1}
  )
```

Use exchange `""` to send straight to the queue named by the routing key.

| Option | Default | Meaning |
|---|---|---|
| `:persistent` | `true` | Write the message to disk. |
| `:mandatory` | `false` | Fail with kind `:unroutable` when no queue receives the message. |
| `:confirm` | `true` | Wait for the broker's confirm. With `false`, return once the message is written to the socket. |
| `:properties` | `[]` | `content_type`, `content_encoding`, `correlation_id`, `reply_to`, `expiration` (ms), `message_id`, `timestamp` (Unix seconds or a `DateTime`), `type`, `user_id`, `app_id`, `priority`. |
| `:headers` | `%{}` | Application headers: string keys, any MessagePack value. |

### Consume

`subscribe/3` delivers messages to the calling process as
`{:firemq, sub_ref, %FireMQ.Message{}}`:

```elixir
{:ok, sub} = FireMQ.subscribe(conn, "jobs", prefetch: 10)

receive do
  {:firemq, ^sub, %FireMQ.Message{body: body} = message} ->
    case process(body) do
      :ok -> FireMQ.ack(message)
      :retry -> FireMQ.nack(message, true)
      :fail -> FireMQ.nack(message, false)
    end

  {:firemq_cancelled, ^sub, %FireMQ.Error{} = reason} ->
    # The broker ended the subscription (for example, the queue was deleted)
    # or the connection closed.
    {:stopped, reason}
end

:ok = FireMQ.unsubscribe(conn, sub)
```

A message has `body`, `exchange`, `routing_key`, `properties` (a map with atom keys),
`headers` (a map with string keys), `redelivered` and `delivery_tag`.

The broker sends at most `:prefetch` unacknowledged messages (100 by default). The client
grants more credit as you settle messages, half the window at a time. With
`no_ack: true`, messages count as acknowledged when sent and the client grants credit as
they arrive.

`nack(message, true)` requeues the message, which comes back with `redelivered: true`.
`nack(message, false)` dead-letters it when the queue has a dead-letter exchange, and
drops it otherwise.

When the subscribing process exits, the client cancels the subscription and returns the
messages that process had not settled to the queue.

To take a single message, use `get/3`. It returns `{:ok, nil}` when the queue is empty:

```elixir
{:ok, message} = FireMQ.get(conn, "jobs")
if message, do: FireMQ.ack(message)
```

## Supervision

`FireMQ.Connection` is a `GenServer` with a `child_spec/1`, so you can start it under a
supervisor and reach it by name:

```elixir
children = [
  {FireMQ.Connection, url: "fire://guest:guest@localhost:5673", name: MyApp.FireMQ}
]

Supervisor.start_link(children, strategy: :one_for_one)

FireMQ.publish(MyApp.FireMQ, "", "jobs", "hello")
```

`FireMQ.Connection.start_link/1` takes the same options as `FireMQ.connect/2`, plus
`:url` and `:name`. If the broker cannot be reached or refuses the login, `start_link/1`
returns `{:error, %FireMQ.Error{}}`.

When the broker closes the connection or the socket fails, the connection process exits
with `{:shutdown, %FireMQ.Error{}}` and the supervisor starts a new one. The client does
not reconnect on its own, and subscriptions do not survive a restart: subscribers receive
`{:firemq_cancelled, sub_ref, error}` and must subscribe again.

## Errors

Failures return `{:error, %FireMQ.Error{kind: kind, code: code, message: message}}`.

| `kind` | When | `code` |
|---|---|---|
| `:server` | The broker refused a request. | AMQP reply code, such as `403`, `404`, `405` or `406`. |
| `:unroutable` | A `mandatory` publish reached no queue. | `312` |
| `:nacked` | The broker rejected a publish, for example because a `reject-publish` queue is full. | The broker's code, usually `406`. |
| `:closed` | The connection is closed or could not be opened. | The broker's close code, or `nil`. |
| `:timeout` | The broker did not answer in time. | `nil` |
| `:protocol` | The broker sent something the client cannot read, or a frame would exceed the broker's size limit. | `nil` |
| `:url` | The connection URL is invalid or uses an unsupported scheme. | `nil` |

```elixir
case FireMQ.publish(conn, "orders", "order.unknown", "x", mandatory: true) do
  :ok -> :ok
  {:error, %FireMQ.Error{kind: :unroutable}} -> Logger.warning("nobody is listening")
  {:error, %FireMQ.Error{} = error} -> raise error
end
```

A server error on one request, such as a `406` when you redeclare a queue with other
properties, does not close the connection.

## Development

The unit tests need no broker:

```sh
mix test
```

The integration tests and examples need a running broker:

```sh
firemq --ephemeral &
FIREMQ_URL=fire://guest:guest@localhost:5673 mix test --only integration
```

The conformance driver in `conformance/driver.exs` runs the FireMQ SDK conformance suite.
Compile first, so that Mix prints nothing on the driver's standard output:

```sh
mix compile
firemq-conformance run --driver "mix run conformance/driver.exs"
```

## License

Apache-2.0. See [LICENSE](LICENSE).
