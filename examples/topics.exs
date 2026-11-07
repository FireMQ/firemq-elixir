# Topics: route with word patterns. `*` matches one word, `#` matches zero or more.
#
#     FIREMQ_URL=fire://guest:guest@localhost:5673 mix run examples/topics.exs

url = System.get_env("FIREMQ_URL", "fire://localhost")
conn = FireMQ.connect!(url)

FireMQ.declare_exchange!(conn, "topic_logs.ex", :topic)
%FireMQ.QueueInfo{name: queue} = FireMQ.declare_queue!(conn, "", temporary: true)
FireMQ.bind_queue!(conn, queue, "topic_logs.ex", "*.critical")

for key <- ["kern.critical", "kern.info", "app.critical"] do
  FireMQ.publish!(conn, "topic_logs.ex", key, key)
end

sub = FireMQ.subscribe!(conn, queue)

for _ <- 1..2 do
  receive do
    {:firemq, ^sub, message} ->
      IO.puts(" [x] #{message.body}")
      FireMQ.ack!(message)
  after
    5_000 -> raise "no message within 5 seconds"
  end
end

FireMQ.close(conn)
