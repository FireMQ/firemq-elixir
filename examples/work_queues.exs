# Work queues: share tasks between workers, one unacknowledged task per worker at a time.
#
#     FIREMQ_URL=fire://guest:guest@localhost:5673 mix run examples/work_queues.exs

url = System.get_env("FIREMQ_URL", "fire://localhost")
conn = FireMQ.connect!(url)

FireMQ.declare_queue!(conn, "task_queue.ex")

# Each publish returns once the broker has written the task to disk.
for n <- 1..6, do: FireMQ.publish!(conn, "", "task_queue.ex", "task #{n}")

parent = self()

# Two workers. With prefetch 1 the broker hands a worker its next task only after it
# acknowledges the previous one.
for name <- ["worker 1", "worker 2"] do
  spawn_link(fn ->
    sub = FireMQ.subscribe!(conn, "task_queue.ex", prefetch: 1)

    work = fn work ->
      receive do
        {:firemq, ^sub, task} ->
          Process.sleep(100)
          IO.puts(" [#{name}] Done #{task.body}")
          FireMQ.ack!(task)
          send(parent, :done)
          work.(work)
      end
    end

    work.(work)
  end)
end

for _ <- 1..6 do
  receive do
    :done -> :ok
  after
    5_000 -> raise "the workers stopped making progress"
  end
end

FireMQ.close(conn)
