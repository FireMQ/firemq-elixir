# RPC: a request carries reply_to and correlation_id; the server answers there.
#
#     FIREMQ_URL=fire://guest:guest@localhost:5673 mix run examples/rpc.exs

url = System.get_env("FIREMQ_URL", "fire://localhost")
conn = FireMQ.connect!(url)

FireMQ.declare_queue!(conn, "rpc_queue.ex")

# The server squares numbers. It runs in its own process, which owns the subscription.
spawn_link(fn ->
  requests = FireMQ.subscribe!(conn, "rpc_queue.ex")

  receive do
    {:firemq, ^requests, request} ->
      n = String.to_integer(request.body)

      FireMQ.publish!(conn, "", request.properties.reply_to, Integer.to_string(n * n),
        properties: [correlation_id: request.properties.correlation_id]
      )

      FireMQ.ack!(request)
  end
end)

# The client sends a request and waits for the matching reply.
%FireMQ.QueueInfo{name: replies} = FireMQ.declare_queue!(conn, "", temporary: true)
answers = FireMQ.subscribe!(conn, replies)

FireMQ.publish!(conn, "", "rpc_queue.ex", "7",
  properties: [reply_to: replies, correlation_id: "req-1"]
)

receive do
  {:firemq, ^answers, %FireMQ.Message{properties: %{correlation_id: "req-1"}} = answer} ->
    IO.puts(" [.] square(7) = #{answer.body}")
    FireMQ.ack!(answer)
after
  5_000 -> raise "no reply within 5 seconds"
end

FireMQ.close(conn)
