defmodule FireMQ.ConnectionTest do
  # Drives FireMQ.Connection against a scripted broker, to pin down behaviour a real
  # broker makes hard to trigger on demand.
  use ExUnit.Case, async: true

  import FireMQ.Test.FakeBroker

  alias FireMQ.{Error, Message}

  test "hello carries credentials, vhost, heartbeat and capabilities" do
    {_conn, _socket, hello} = connect(heartbeat: 7, connection_name: "billing")

    assert hello["ver"] == 1
    assert hello["vhost"] == "test"
    assert hello["auth"] == %{"mech" => "plain", "user" => "guest", "pass" => "guest"}
    assert hello["heartbeat"] == 7
    assert hello["caps"] == ["confirms", "credit"]

    assert %{"name" => "firemq-elixir", "lang" => "elixir", "connection_name" => "billing"} =
             hello["client"]
  end

  test "server_info reports the welcome" do
    {conn, _socket, _hello} = connect()

    assert {:ok, %{conn_id: "test", max_frame: 1_048_576, server: %{"name" => "fake"}}} =
             FireMQ.server_info(conn)
  end

  test "replies match their request ids" do
    {conn, socket, _} = connect()

    declare = Task.async(fn -> FireMQ.declare_queue(conn, "", temporary: true) end)
    {header, ""} = expect(socket, :declare_queue)
    assert header["durable"] == nil
    assert header["exclusive"] == true and header["auto_delete"] == true

    purge = Task.async(fn -> FireMQ.purge_queue(conn, "jobs") end)
    {purge_header, ""} = expect(socket, :purge_queue)

    send_frame(socket, :ok, %{rid: purge_header["rid"], messages: 4})
    send_frame(socket, :ok, %{rid: header["rid"], name: "amq.gen-1", messages: 0, consumers: 0})

    assert Task.await(purge) == {:ok, 4}
    assert {:ok, %FireMQ.QueueInfo{name: "amq.gen-1"}} = Task.await(declare)
  end

  test "an error reply fails only its request" do
    {conn, socket, _} = connect()

    task = Task.async(fn -> FireMQ.bind_queue(conn, "q", "missing", "k") end)
    {header, ""} = expect(socket, :bind)
    send_frame(socket, :error, %{rid: header["rid"], code: 404, msg: "no exchange 'missing'"})

    assert {:error, %Error{kind: :server, code: 404, message: "no exchange 'missing'"}} =
             Task.await(task)

    assert Process.alive?(conn)
  end

  test "cumulative confirms resolve every publish up to seq; a nack fails only its own" do
    {conn, socket, _} = connect()

    tasks =
      for body <- ~w(a b c),
          into: %{},
          do: {body, Task.async(fn -> FireMQ.publish(conn, "", "q", body) end)}

    seqs =
      for _ <- 1..3, into: %{} do
        {header, body} = expect(socket, :publish)
        assert header["persistent"] == true
        {body, header["seq"]}
      end

    assert seqs |> Map.values() |> Enum.sort() == [1, 2, 3]

    send_frame(socket, :confirm, %{
      seq: seqs["b"],
      nack: true,
      code: 406,
      msg: "queue 'q' is full"
    })

    send_frame(socket, :confirm, %{upto: 3})

    assert Task.await(tasks["a"]) == :ok
    assert Task.await(tasks["c"]) == :ok
    assert {:error, %Error{kind: :nacked, code: 406}} = Task.await(tasks["b"])
  end

  test "a confirm covers only publishes up to its seq" do
    {conn, socket, _} = connect()

    first = Task.async(fn -> FireMQ.publish(conn, "", "q", "1") end)
    {%{"seq" => 1}, "1"} = expect(socket, :publish)
    second = Task.async(fn -> FireMQ.publish(conn, "", "q", "2") end)
    {%{"seq" => 2}, "2"} = expect(socket, :publish)

    send_frame(socket, :confirm, %{upto: 1})
    assert Task.await(first) == :ok
    assert Task.yield(second, 100) == nil

    send_frame(socket, :confirm, %{upto: 2})
    assert Task.await(second) == :ok
  end

  test "a return before the confirm makes the publish unroutable" do
    {conn, socket, _} = connect()

    task = Task.async(fn -> FireMQ.publish(conn, "orders", "nowhere", "hi", mandatory: true) end)
    {header, "hi"} = expect(socket, :publish)
    assert header["mandatory"] == true

    send_frame(
      socket,
      :return,
      %{seq: header["seq"], code: 312, msg: "NO_ROUTE", exchange: "orders", key: "nowhere"},
      "hi"
    )

    send_frame(socket, :confirm, %{upto: header["seq"]})

    assert {:error, %Error{kind: :unroutable, code: 312, message: message}} = Task.await(task)
    assert message =~ "exchange 'orders' with key 'nowhere'"
  end

  test "publish without confirm sends seq 0 and returns at once" do
    {conn, socket, _} = connect()

    assert :ok = FireMQ.publish(conn, "", "q", "fast", confirm: false, persistent: false)
    {header, "fast"} = expect(socket, :publish)
    assert header["seq"] == 0
    assert header["persistent"] == nil
  end

  test "properties and headers go out as maps" do
    {conn, socket, _} = connect()

    task =
      Task.async(fn ->
        FireMQ.publish(conn, "", "q", "{}",
          properties: [
            content_type: "application/json",
            expiration: "60000",
            timestamp: ~U[2026-01-01 00:00:00Z]
          ],
          headers: %{"trace" => "abc", "n" => 2}
        )
      end)

    {header, "{}"} = expect(socket, :publish)

    assert header["props"] == %{
             "content_type" => "application/json",
             "expiration" => 60_000,
             "timestamp" => 1_767_225_600
           }

    assert header["headers"] == %{"trace" => "abc", "n" => 2}
    send_frame(socket, :confirm, %{upto: 1})
    assert Task.await(task) == :ok
  end

  test "unknown properties raise" do
    {conn, _socket, _} = connect()

    assert_raise ArgumentError, ~r/unknown message property/, fn ->
      FireMQ.publish(conn, "", "q", "x", properties: [colour: "red"])
    end
  end

  test "a publish larger than max_frame fails and the connection stays open" do
    {conn, _socket, _} = connect([], %{"max_frame" => 70_000})
    big = :binary.copy("x", 80_000)
    assert {:error, %Error{kind: :protocol}} = FireMQ.publish(conn, "", "q", big)
    assert Process.alive?(conn)
  end

  test "subscribe grants the prefetch as credit and tops it up in half-window batches" do
    {conn, socket, _} = connect()
    {sub, sub_id} = subscribe(conn, socket, "jobs", prefetch: 4)

    for tag <- 1..4,
        do: send_frame(socket, :deliver, %{sub: sub_id, tag: tag, key: "jobs"}, "m#{tag}")

    messages =
      for _ <- 1..4 do
        assert_receive {:firemq, ^sub, %Message{} = message}
        message
      end

    assert :ok = FireMQ.ack(Enum.at(messages, 0))
    assert {%{"tag" => 1}, ""} = expect(socket, :ack)
    assert recv(socket, 100) == {:error, :timeout}

    assert :ok = FireMQ.nack(Enum.at(messages, 1), true)
    assert {%{"tag" => 2, "requeue" => true}, ""} = expect(socket, :nack)
    assert {%{"sub" => ^sub_id, "credit" => 2}, ""} = expect(socket, :credit)
  end

  test "settling twice sends one frame" do
    {conn, socket, _} = connect()
    {sub, sub_id} = subscribe(conn, socket, "jobs", prefetch: 10)

    send_frame(socket, :deliver, %{sub: sub_id, tag: 9}, "x")
    assert_receive {:firemq, ^sub, message}

    assert :ok = FireMQ.ack(message)
    assert :ok = FireMQ.ack(message)
    assert :ok = FireMQ.nack(message, true)
    assert {%{"tag" => 9}, ""} = expect(socket, :ack)
    assert recv(socket, 100) == {:error, :timeout}
  end

  test "no_ack subscriptions grant credit on receipt" do
    {conn, socket, _} = connect()
    {sub, sub_id} = subscribe(conn, socket, "jobs", prefetch: 2, no_ack: true)

    send_frame(socket, :deliver, %{sub: sub_id, tag: 1}, "x")
    assert_receive {:firemq, ^sub, %Message{no_ack: true} = message}
    assert {%{"sub" => ^sub_id, "credit" => 1}, ""} = expect(socket, :credit)

    assert :ok = FireMQ.ack(message)
    assert recv(socket, 100) == {:error, :timeout}
  end

  test "deliveries that cross an unsubscribe are requeued" do
    {conn, socket, _} = connect()
    {sub, sub_id} = subscribe(conn, socket, "jobs", prefetch: 10)

    task = Task.async(fn -> FireMQ.unsubscribe(conn, sub) end)
    {header, ""} = expect(socket, :unsubscribe)
    assert header["sub"] == sub_id

    send_frame(socket, :deliver, %{sub: sub_id, tag: 5}, "late")
    assert {%{"tag" => 5, "requeue" => true}, ""} = expect(socket, :nack)

    send_frame(socket, :ok, %{rid: header["rid"]})
    assert Task.await(task) == :ok
    refute_received {:firemq, ^sub, _}
  end

  test "a subscriber that exits is unsubscribed and its unsettled messages requeued" do
    {conn, socket, _} = connect()
    test = self()

    subscriber =
      spawn(fn ->
        {:ok, sub} = FireMQ.subscribe(conn, "jobs")
        send(test, {:subscribed, sub})
        receive do: ({:firemq, ^sub, _message} -> send(test, :got_it))
        receive do: (:stop -> :ok)
      end)

    {header, ""} = expect(socket, :subscribe)
    send_frame(socket, :ok, %{rid: header["rid"]})
    assert_receive {:subscribed, _sub}

    send_frame(socket, :deliver, %{sub: header["sub"], tag: 3}, "x")
    assert_receive :got_it
    send(subscriber, :stop)

    assert {%{"sub" => sub_id}, ""} = expect(socket, :unsubscribe)
    assert sub_id == header["sub"]
    assert {%{"tag" => 3, "requeue" => true}, ""} = expect(socket, :nack)
  end

  test "a cancelled frame tells the subscriber" do
    {conn, socket, _} = connect()
    {sub, sub_id} = subscribe(conn, socket, "jobs", [])

    send_frame(socket, :cancelled, %{sub: sub_id, code: 404, msg: "queue 'jobs' was deleted"})
    assert_receive {:firemq_cancelled, ^sub, %Error{kind: :server, code: 404}}
  end

  test "get returns nil on empty and a message otherwise" do
    {conn, socket, _} = connect()

    task = Task.async(fn -> FireMQ.get(conn, "jobs") end)
    {header, ""} = expect(socket, :get)
    send_frame(socket, :ok, %{rid: header["rid"], empty: true})
    assert Task.await(task) == {:ok, nil}

    task = Task.async(fn -> FireMQ.get(conn, "jobs") end)
    {header, ""} = expect(socket, :get)

    send_frame(
      socket,
      :get_ok,
      %{
        rid: header["rid"],
        tag: 4,
        key: "jobs",
        remaining: 0,
        redelivered: true,
        props: %{"type" => "t", "priority" => 2},
        headers: %{"h" => [1, "two"]}
      },
      <<0, 255>>
    )

    assert {:ok, %Message{} = message} = Task.await(task)
    assert message.body == <<0, 255>>
    assert message.routing_key == "jobs"
    assert message.redelivered
    assert message.properties == %{type: "t", priority: 2}
    assert message.headers == %{"h" => [1, "two"]}

    assert :ok = FireMQ.ack(message)
    assert {%{"tag" => 4}, ""} = expect(socket, :ack)
  end

  test "a ping gets a pong" do
    {_conn, socket, _} = connect()
    send_frame(socket, :ping, %{})
    assert {%{}, ""} = expect(socket, :pong)
  end

  test "the client sends pings when idle and gives up on a silent broker" do
    {conn, socket, _} = connect([heartbeat: 1], %{"heartbeat" => 1})
    ref = Process.monitor(conn)

    assert {%{}, ""} = expect(socket, :ping, 1_500)

    assert_receive {:DOWN, ^ref, :process, ^conn,
                    {:shutdown, %Error{kind: :closed, message: message}}},
                   3_000

    assert message =~ "missed heartbeats"
  end

  test "a broker close fails pending work and ends subscriptions" do
    {conn, socket, _} = connect()
    {sub, _sub_id} = subscribe(conn, socket, "jobs", [])

    task = Task.async(fn -> FireMQ.declare_queue(conn, "jobs") end)
    expect(socket, :declare_queue)
    send_frame(socket, :close, %{code: 320, msg: "shutting down"})

    assert {%{"code" => 200}, ""} = expect(socket, :close)
    assert {:error, %Error{kind: :closed, code: 320}} = Task.await(task)
    assert_receive {:firemq_cancelled, ^sub, %Error{kind: :closed}}
    assert {:error, %Error{kind: :closed}} = FireMQ.purge_queue(conn, "jobs")
  end

  test "a fatal error closes the connection" do
    {conn, socket, _} = connect()
    ref = Process.monitor(conn)
    send_frame(socket, :error, %{code: 501, msg: "frame error", fatal: true})
    assert_receive {:DOWN, ^ref, :process, ^conn, {:shutdown, %Error{kind: :server, code: 501}}}
  end

  test "close waits for the broker's close" do
    {conn, socket, _} = connect()
    task = Task.async(fn -> FireMQ.close(conn) end)

    assert {%{"code" => 200}, ""} = expect(socket, :close)
    assert Task.yield(task, 100) == nil
    send_frame(socket, :close, %{code: 200})
    assert Task.await(task) == :ok
    refute Process.alive?(conn)
    assert FireMQ.close(conn) == :ok
  end

  test "the connection closes when its owner exits" do
    parent = self()

    owner =
      spawn(fn ->
        {conn, socket, _} = connect()
        :ok = :gen_tcp.controlling_process(socket, parent)
        send(parent, {:conn, conn, socket})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:conn, conn, socket}
    ref = Process.monitor(conn)
    send(owner, :stop)

    assert {%{"code" => 200}, ""} = expect(socket, :close)
    assert_receive {:DOWN, ^ref, :process, ^conn, :normal}
  end

  test "a server-initiated socket close stops the connection" do
    {conn, socket, _} = connect()
    ref = Process.monitor(conn)
    :gen_tcp.close(socket)
    assert_receive {:DOWN, ^ref, :process, ^conn, {:shutdown, %Error{kind: :closed}}}
  end

  # The caller of FireMQ.subscribe/3 is the subscriber, so the broker side runs in a task.
  defp subscribe(conn, socket, queue, opts) do
    broker =
      Task.async(fn ->
        {header, ""} = expect(socket, :subscribe)
        send_frame(socket, :ok, %{rid: header["rid"]})
        header
      end)

    {:ok, sub} = FireMQ.subscribe(conn, queue, opts)
    header = Task.await(broker)
    assert header["queue"] == queue
    assert header["credit"] == Keyword.get(opts, :prefetch, 100)
    {sub, header["sub"]}
  end
end
