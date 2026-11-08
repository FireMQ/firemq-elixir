defmodule FireMQ.FrameTest do
  use ExUnit.Case, async: true

  alias FireMQ.Frame

  @vectors "test/fixtures/frames.json" |> File.read!() |> Jason.decode!() |> Map.fetch!("vectors")

  test "the fixture covers every op" do
    ops = @vectors |> Enum.map(& &1["op_name"]) |> MapSet.new()
    assert ops == Frame.ops() |> Enum.map(&Atom.to_string/1) |> MapSet.new()
  end

  for vector <- @vectors do
    @vector vector

    test "decodes the #{vector["name"]} vector" do
      frame = Base.decode16!(@vector["frame_hex"], case: :lower)

      assert {:ok, {op, header, payload}, ""} = Frame.decode(frame)
      assert Frame.op_code(op) == @vector["op"]
      assert Atom.to_string(op) == @vector["op_name"]
      assert header == @vector["header"]
      assert Base.encode16(payload, case: :lower) == @vector["payload_hex"]
    end

    test "round-trips the #{vector["name"]} vector" do
      op = String.to_existing_atom(@vector["op_name"])
      payload = Base.decode16!(@vector["payload_hex"], case: :lower)
      encoded = op |> Frame.encode(@vector["header"], payload) |> IO.iodata_to_binary()

      assert {:ok, {^op, header, ^payload}, ""} = Frame.decode(encoded)
      assert header == @vector["header"]

      <<_len::32, body::binary>> = encoded
      assert {:ok, {^op, ^header, ^payload}} = Frame.decode_body(body)
    end
  end

  test "frames split across reads come out whole and in order" do
    stream =
      IO.iodata_to_binary([
        Frame.encode(:publish, %{"seq" => 1}, "hello"),
        Frame.encode(:ping, %{})
      ])

    frames =
      stream
      |> :binary.bin_to_list()
      |> Enum.reduce({"", []}, fn byte, {buffer, frames} ->
        drain(buffer <> <<byte>>, frames)
      end)
      |> elem(1)
      |> Enum.reverse()

    assert frames == [{:publish, %{"seq" => 1}, "hello"}, {:ping, %{}, ""}]
  end

  test "an empty header encodes as zero bytes" do
    assert IO.iodata_to_binary(Frame.encode(:ping, %{})) == <<0, 0, 0, 5, 3, 0, 0, 0, 0>>
  end

  test "omitted fields stay omitted and unknown keys are kept for the caller to ignore" do
    header = %{"tag" => 5, "future_field" => [1, 2]}
    encoded = IO.iodata_to_binary(Frame.encode(:ack, header))
    assert {:ok, {:ack, ^header, ""}, ""} = Frame.decode(encoded)
  end

  test "oversized frames are rejected" do
    frame = IO.iodata_to_binary(Frame.encode(:publish, %{}, :binary.copy("x", 100)))
    assert {:error, "frame of " <> _} = Frame.decode(frame, 50)
  end

  test "malformed frames are rejected" do
    assert {:error, "malformed frame: frame shorter than 5 bytes"} =
             Frame.decode(<<0, 0, 0, 2, 1, 2>>)

    assert {:error, "malformed frame: header length exceeds frame"} =
             Frame.decode_body(<<0x20, 0, 0, 0, 9, 1>>)

    assert {:error, "malformed frame: header is not a MessagePack map"} =
             Frame.decode_body(<<0x20, 0, 0, 0, 1, 0x01>>)
  end

  test "unknown ops decode so the connection can ignore them" do
    assert {:ok, {{:unknown, 0x7F}, %{}, ""}} = Frame.decode_body(<<0x7F, 0, 0, 0, 0>>)
  end

  test "an incomplete buffer asks for more" do
    frame = IO.iodata_to_binary(Frame.encode(:publish, %{"seq" => 1}, "hello"))
    assert Frame.decode(binary_part(frame, 0, 3)) == :more
    assert Frame.decode(binary_part(frame, 0, byte_size(frame) - 1)) == :more
  end

  defp drain(buffer, frames) do
    case Frame.decode(buffer) do
      {:ok, frame, rest} -> drain(rest, [frame | frames])
      :more -> {buffer, frames}
    end
  end
end
