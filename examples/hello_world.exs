# Hello world: publish one message and consume it.
#
#     FIREMQ_URL=fire://guest:guest@localhost:5673 mix run examples/hello_world.exs

url = System.get_env("FIREMQ_URL", "fire://localhost")
conn = FireMQ.connect!(url)

FireMQ.declare_queue!(conn, "hello.ex")
:ok = FireMQ.publish!(conn, "", "hello.ex", "Hello World!")
IO.puts(" [x] Sent 'Hello World!'")

sub = FireMQ.subscribe!(conn, "hello.ex")

receive do
  {:firemq, ^sub, message} ->
    IO.puts(" [x] Received '#{message.body}'")
    FireMQ.ack!(message)
after
  5_000 -> raise "no message within 5 seconds"
end

FireMQ.close(conn)
