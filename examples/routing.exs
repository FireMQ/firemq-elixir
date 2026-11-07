# Routing: a direct exchange delivers by exact routing key.
#
#     FIREMQ_URL=fire://guest:guest@localhost:5673 mix run examples/routing.exs

url = System.get_env("FIREMQ_URL", "fire://localhost")
conn = FireMQ.connect!(url)

FireMQ.declare_exchange!(conn, "direct_logs.ex", :direct)
%FireMQ.QueueInfo{name: queue} = FireMQ.declare_queue!(conn, "", temporary: true)

for severity <- ["error", "warning"] do
  FireMQ.bind_queue!(conn, queue, "direct_logs.ex", severity)
end

for {severity, text} <- [{"info", "all good"}, {"warning", "disk at 80%"}, {"error", "disk full"}] do
  FireMQ.publish!(conn, "direct_logs.ex", severity, text)
end

sub = FireMQ.subscribe!(conn, queue)

# Only "warning" and "error" are bound, so "info" never arrives.
for _ <- 1..2 do
  receive do
    {:firemq, ^sub, message} ->
      IO.puts(" [x] #{message.routing_key}: #{message.body}")
      FireMQ.ack!(message)
  after
    5_000 -> raise "no message within 5 seconds"
  end
end

FireMQ.close(conn)
