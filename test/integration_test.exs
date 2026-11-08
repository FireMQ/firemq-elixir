defmodule FireMQ.IntegrationTest do
  # Runs against a real broker. Set FIREMQ_URL, for example
  # FIREMQ_URL=fire://guest:guest@localhost:5673 mix test --only integration
  use ExUnit.Case, async: true

  alias FireMQ.{Error, Message, QueueInfo}

  @moduletag :integration

  setup context do
    url = System.fetch_env!("FIREMQ_URL")
    {:ok, conn} = FireMQ.connect(url)
    on_exit(fn -> FireMQ.close(conn) end)

    # A queue name unique to this test.
    name = "it.#{:erlang.phash2(context.test)}.ex"
    {:ok, conn: conn, url: url, name: name}
  end

  test "connects and reports the server", %{conn: conn} do
    assert {:ok, %{server: %{"name" => "firemq"}, caps: caps}} = FireMQ.server_info(conn)
    assert "confirms" in caps
  end

  test "a wrong password fails with 403", %{url: url} do
    uri = URI.parse(url)
    bad = URI.to_string(%{uri | userinfo: "guest:wrong"})
    assert {:error, %Error{kind: :server, code: 403}} = FireMQ.connect(bad)
  end

  test "publish and get round-trip body, properties and headers", %{conn: conn, name: q} do
    assert {:ok, %QueueInfo{name: ^q, messages: 0}} = FireMQ.declare_queue(conn, q)
    on_exit(fn -> cleanup(q) end)

    assert {:ok, nil} = FireMQ.get(conn, q, no_ack: true)

    properties = %{
      content_type: "application/json",
      content_encoding: "gzip",
      correlation_id: "c-1",
      reply_to: "replies",
      expiration: 600_000,
      message_id: "m-1",
      timestamp: 1_767_225_600,
      type: "order.created",
      app_id: "shop",
      priority: 3
    }

    headers = %{
      "string" => "s",
      "int" => 42,
      "negative" => -7,
      "float" => 1.5,
      "bool" => true,
      "nil" => nil,
      "list" => [1, "two"],
      "map" => %{"k" => "v"}
    }

    assert :ok =
             FireMQ.publish(conn, "", q, ~s({"id":1}), properties: properties, headers: headers)

    assert {:ok, %Message{} = message} = FireMQ.get(conn, q)
    assert message.body == ~s({"id":1})
    assert message.exchange == ""
    assert message.routing_key == q
    assert message.properties == properties
    assert message.headers == headers
    assert :ok = FireMQ.ack(message)
  end

  test "binary and 1 MiB bodies arrive intact", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)

    binary = for(i <- 0..255, into: <<>>, do: <<i>>)
    large = :binary.copy("x", 1024 * 1024)
    :ok = FireMQ.publish!(conn, "", q, binary)
    :ok = FireMQ.publish!(conn, "", q, large)

    assert %Message{body: ^binary} = FireMQ.get!(conn, q, no_ack: true)
    assert %Message{body: ^large} = FireMQ.get!(conn, q, no_ack: true)
  end

  test "100 messages are consumed in order and the queue ends empty", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)

    for i <- 1..100, do: :ok = FireMQ.publish(conn, "", q, "m#{i}")
    {:ok, sub} = FireMQ.subscribe(conn, q, prefetch: 10)

    for i <- 1..100 do
      assert_receive {:firemq, ^sub, %Message{body: body} = message}, 2_000
      assert body == "m#{i}"
      :ok = FireMQ.ack(message)
    end

    :ok = FireMQ.unsubscribe(conn, sub)
    assert {:ok, %QueueInfo{messages: 0}} = FireMQ.declare_queue(conn, q, passive: true)
  end

  test "prefetch holds back deliveries until a message is settled", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)

    for i <- 1..3, do: :ok = FireMQ.publish(conn, "", q, "m#{i}")
    {:ok, sub} = FireMQ.subscribe(conn, q, prefetch: 2)

    assert_receive {:firemq, ^sub, first}, 2_000
    assert_receive {:firemq, ^sub, _second}, 2_000
    refute_receive {:firemq, ^sub, _}, 300

    :ok = FireMQ.ack(first)
    assert_receive {:firemq, ^sub, %Message{body: "m3"}}, 2_000
  end

  test "a nacked message comes back redelivered", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)

    :ok = FireMQ.publish(conn, "", q, "again")
    {:ok, sub} = FireMQ.subscribe(conn, q, prefetch: 1)

    assert_receive {:firemq, ^sub, %Message{redelivered: false} = message}, 2_000
    :ok = FireMQ.nack(message, true)
    assert_receive {:firemq, ^sub, %Message{body: "again", redelivered: true} = again}, 2_000
    :ok = FireMQ.ack(again)
  end

  test "a message nacked without requeue is dead-lettered", %{conn: conn, name: q} do
    dlx = q <> ".dlx"
    dead = q <> ".dead"
    :ok = FireMQ.declare_exchange(conn, dlx, :fanout)
    FireMQ.declare_queue!(conn, dead)
    :ok = FireMQ.bind_queue(conn, dead, dlx)
    FireMQ.declare_queue!(conn, q, args: %{"x-dead-letter-exchange" => dlx})

    on_exit(fn ->
      cleanup(q)
      cleanup(dead)
      cleanup_exchange(dlx)
    end)

    :ok = FireMQ.publish(conn, "", q, "bad")
    message = FireMQ.get!(conn, q)
    :ok = FireMQ.nack(message, false)

    assert %Message{body: "bad", exchange: ^dlx, headers: %{"x-death" => _}} =
             eventually(fn -> FireMQ.get!(conn, dead, no_ack: true) end)
  end

  test "a mandatory publish to nowhere is unroutable", %{conn: conn, name: q} do
    assert {:error, %Error{kind: :unroutable, code: 312}} =
             FireMQ.publish(conn, "", q <> ".nowhere", "lost", mandatory: true)

    assert :ok = FireMQ.publish(conn, "", q <> ".nowhere", "dropped")
  end

  test "a publish to a full reject-publish queue is nacked", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q, args: [{"x-max-length", 1}, {"x-overflow", "reject-publish"}])
    on_exit(fn -> cleanup(q) end)

    assert :ok = FireMQ.publish(conn, "", q, "fits")
    assert {:error, %Error{kind: :nacked, code: 406}} = FireMQ.publish(conn, "", q, "rejected")
  end

  test "topic patterns and exchange-to-exchange bindings route", %{conn: conn, name: q} do
    up = q <> ".up"
    down = q <> ".down"
    :ok = FireMQ.declare_exchange(conn, up, "topic")
    :ok = FireMQ.declare_exchange(conn, down, "fanout")
    FireMQ.declare_queue!(conn, q)
    :ok = FireMQ.bind_queue(conn, q, down)
    :ok = FireMQ.bind_exchange(conn, down, up, "*.critical.#")

    on_exit(fn ->
      cleanup(q)
      cleanup_exchange(up)
      cleanup_exchange(down)
    end)

    for key <- ~w(kern.critical kern.critical.disk kern.info critical) do
      :ok = FireMQ.publish(conn, up, key, key)
    end

    assert %Message{body: "kern.critical", exchange: ^up} = FireMQ.get!(conn, q, no_ack: true)
    assert %Message{body: "kern.critical.disk"} = FireMQ.get!(conn, q, no_ack: true)
    assert FireMQ.get!(conn, q, no_ack: true) == nil

    :ok = FireMQ.unbind_queue(conn, q, down)
    :ok = FireMQ.publish(conn, up, "kern.critical", "unbound")
    assert FireMQ.get!(conn, q, no_ack: true) == nil
  end

  test "errors carry codes and leave the connection usable", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)

    assert {:error, %Error{kind: :server, code: 406}} =
             FireMQ.declare_queue(conn, q, durable: false)

    assert {:error, %Error{kind: :server, code: 404}} =
             FireMQ.bind_queue(conn, q, q <> ".missing", "")

    assert_raise Error, ~r/code 404/, fn ->
      FireMQ.declare_queue!(conn, q <> ".missing", passive: true)
    end

    assert :ok = FireMQ.publish(conn, "", q, "still works")
  end

  test "purge and delete report message counts", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    for _ <- 1..3, do: :ok = FireMQ.publish(conn, "", q, "x")
    assert {:ok, 3} = FireMQ.purge_queue(conn, q)
    :ok = FireMQ.publish(conn, "", q, "y")
    assert {:ok, 1} = FireMQ.delete_queue(conn, q)
  end

  test "unacknowledged messages return when the connection closes", %{url: url, name: q} do
    {:ok, conn} = FireMQ.connect(url)
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)
    :ok = FireMQ.publish(conn, "", q, "work")
    {:ok, sub} = FireMQ.subscribe(conn, q, prefetch: 1)
    assert_receive {:firemq, ^sub, _message}, 2_000
    :ok = FireMQ.close(conn)

    {:ok, other} = FireMQ.connect(url)

    assert %Message{body: "work", redelivered: true} =
             eventually(fn -> FireMQ.get!(other, q, no_ack: true) end)

    FireMQ.close(other)
  end

  test "unsettled messages return when the subscriber exits", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)
    :ok = FireMQ.publish(conn, "", q, "work")
    test = self()

    spawn(fn ->
      {:ok, sub} = FireMQ.subscribe(conn, q)
      receive do: ({:firemq, ^sub, _} -> send(test, :received))
    end)

    assert_receive :received, 2_000

    assert %Message{body: "work", redelivered: true} =
             eventually(fn -> FireMQ.get!(conn, q, no_ack: true) end)
  end

  test "a temporary queue disappears with its connection", %{url: url, conn: conn} do
    {:ok, owner} = FireMQ.connect(url)

    assert {:ok, %QueueInfo{name: "amq.gen-" <> _ = name}} =
             FireMQ.declare_queue(owner, "", temporary: true)

    :ok = FireMQ.close(owner)

    assert {:error, %Error{code: 404}} =
             eventually(
               fn -> FireMQ.declare_queue(conn, name, passive: true) end,
               &match?({:error, %Error{code: 404}}, &1)
             )
  end

  test "no deliveries after unsubscribe", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    on_exit(fn -> cleanup(q) end)
    {:ok, sub} = FireMQ.subscribe(conn, q, prefetch: 10, no_ack: true)

    :ok = FireMQ.publish(conn, "", q, "before")
    assert_receive {:firemq, ^sub, %Message{body: "before"}}, 2_000
    :ok = FireMQ.unsubscribe(conn, sub)

    :ok = FireMQ.publish(conn, "", q, "after")
    refute_receive {:firemq, ^sub, _}, 200
    assert %Message{body: "after"} = FireMQ.get!(conn, q, no_ack: true)
  end

  test "deleting the queue cancels its subscriptions", %{conn: conn, name: q} do
    FireMQ.declare_queue!(conn, q)
    {:ok, sub} = FireMQ.subscribe(conn, q)
    {:ok, _} = FireMQ.delete_queue(conn, q)
    assert_receive {:firemq_cancelled, ^sub, %Error{}}, 2_000
  end

  test "a supervised connection is reachable by name", %{url: url, name: q} do
    name = :"firemq_#{q}"
    children = [{FireMQ.Connection, url: url, name: name, connection_name: "supervised"}]
    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)

    assert {:ok, %QueueInfo{}} = FireMQ.declare_queue(name, q)
    assert :ok = FireMQ.publish(name, "", q, "via name")
    assert %Message{body: "via name"} = FireMQ.get!(name, q, no_ack: true)
    {:ok, _} = FireMQ.delete_queue(name, q)

    Supervisor.stop(sup)
    assert {:error, %Error{kind: :closed}} = FireMQ.purge_queue(name, q)
  end

  test "start_link reports a refused login without crashing the caller", %{url: url} do
    uri = URI.parse(url)
    bad = URI.to_string(%{uri | userinfo: "guest:wrong"})
    assert {:error, %Error{kind: :server, code: 403}} = FireMQ.Connection.start_link(url: bad)
  end

  # Opens a short-lived connection to remove a queue a test created.
  defp cleanup(queue), do: with_conn(&FireMQ.delete_queue(&1, queue))
  defp cleanup_exchange(exchange), do: with_conn(&FireMQ.delete_exchange(&1, exchange))

  defp with_conn(fun) do
    {:ok, conn} = FireMQ.connect(System.fetch_env!("FIREMQ_URL"))
    fun.(conn)
    FireMQ.close(conn)
  end

  # Retries `fun` until `done?` accepts its result, for effects the broker applies
  # asynchronously such as dead-lettering and requeueing.
  defp eventually(fun, done? \\ &(&1 != nil), attempts \\ 50) do
    result = fun.()

    cond do
      done?.(result) ->
        result

      attempts == 0 ->
        result

      true ->
        Process.sleep(20)
        eventually(fun, done?, attempts - 1)
    end
  end
end
