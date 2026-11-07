# Publish/subscribe: a fanout exchange copies every message to every bound queue.
#
#     FIREMQ_URL=fire://guest:guest@localhost:5673 mix run examples/publish_subscribe.exs

url = System.get_env("FIREMQ_URL", "fire://localhost")
conn = FireMQ.connect!(url)

FireMQ.declare_exchange!(conn, "logs.ex", :fanout)

# Two subscribers, each with its own temporary queue named by the broker.
subs =
  for _ <- 1..2 do
    %FireMQ.QueueInfo{name: queue} = FireMQ.declare_queue!(conn, "", temporary: true)
    FireMQ.bind_queue!(conn, queue, "logs.ex")
    FireMQ.subscribe!(conn, queue)
  end

FireMQ.publish!(conn, "logs.ex", "", "info: hello everyone")

for {sub, n} <- Enum.with_index(subs, 1) do
  receive do
    {:firemq, ^sub, message} ->
      IO.puts(" [x] Subscriber #{n} got '#{message.body}'")
      FireMQ.ack!(message)
  after
    5_000 -> raise "no message within 5 seconds"
  end
end

FireMQ.close(conn)
