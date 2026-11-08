defmodule FireMQ.Test.FakeBroker do
  @moduledoc """
  A scripted broker for connection tests. It accepts one connection, answers the
  handshake, and then lets the test read and write frames on the socket.
  """

  alias FireMQ.Frame

  @timeout 2_000

  @doc """
  Connects a client to a fresh fake broker. Returns `{conn, socket, hello}`, where `hello`
  is the client's hello header. `welcome` overrides fields of the welcome frame.
  """
  def connect(opts \\ [], welcome \\ %{}) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    test = self()

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listen, @timeout)
        {:ok, "FMQ1"} = :gen_tcp.recv(socket, 4, @timeout)
        :ok = :inet.setopts(socket, packet: 4)
        {:hello, hello, ""} = recv(socket)

        welcome =
          Map.merge(
            %{
              "ver" => 1,
              "server" => %{"name" => "fake", "version" => "0"},
              "conn_id" => "test",
              "heartbeat" => 0,
              "max_frame" => 1_048_576,
              "caps" => ["confirms", "credit", "mandatory"]
            },
            welcome
          )

        send_frame(socket, :welcome, welcome)
        :ok = :gen_tcp.controlling_process(socket, test)
        {socket, hello}
      end)

    {:ok, conn} = FireMQ.connect("fire://guest:guest@127.0.0.1:#{port}/test", opts)
    {socket, hello} = Task.await(server)
    :gen_tcp.close(listen)
    {conn, socket, hello}
  end

  @doc "Reads the next frame the client sent."
  def recv(socket, timeout \\ @timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} ->
        {:ok, frame} = Frame.decode_body(data)
        frame

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Reads the next frame and asserts its op."
  def expect(socket, op, timeout \\ @timeout) do
    case recv(socket, timeout) do
      {^op, header, payload} -> {header, payload}
      other -> raise "expected #{inspect(op)}, got #{inspect(other)}"
    end
  end

  @doc "Sends a frame to the client."
  def send_frame(socket, op, header, payload \\ "") do
    :ok = :gen_tcp.send(socket, Frame.encode_body(op, header, payload))
  end
end
