defmodule FireMQ.Frame do
  @moduledoc """
  Encodes and decodes FireMQ protocol frames.

  On TCP and TLS a frame is:

      len : u32 | op : u8 | hlen : u32 | header (MessagePack map) | payload

  `len` counts every byte after itself. The header is a MessagePack map with string keys
  and the payload is the raw message body. All integers are big-endian.

  The connection reads frames with the socket option `packet: 4`, so the runtime strips
  the length prefix and `decode_body/1` sees `op | hlen | header | payload`. `decode/2`
  handles a byte stream that still carries the prefix.
  """

  @typedoc "A decoded frame: the op name, its header map (string keys) and the payload."
  @type t :: {op :: atom() | {:unknown, byte()}, header :: map(), payload :: binary()}

  @magic "FMQ1"

  @ops %{
    0x01 => :hello,
    0x02 => :welcome,
    0x03 => :ping,
    0x04 => :pong,
    0x05 => :close,
    0x06 => :ok,
    0x07 => :error,
    0x08 => :flow,
    0x10 => :declare_exchange,
    0x11 => :delete_exchange,
    0x12 => :declare_queue,
    0x13 => :delete_queue,
    0x14 => :purge_queue,
    0x15 => :bind,
    0x16 => :unbind,
    0x20 => :publish,
    0x21 => :confirm,
    0x22 => :return,
    0x30 => :subscribe,
    0x31 => :unsubscribe,
    0x32 => :credit,
    0x33 => :deliver,
    0x34 => :cancelled,
    0x35 => :ack,
    0x36 => :nack,
    0x37 => :get,
    0x38 => :get_ok
  }

  @codes Map.new(@ops, fn {code, name} -> {name, code} end)

  @doc "The 4 bytes a TCP or TLS client sends before its first frame."
  def magic, do: @magic

  @doc "Largest frame accepted before `welcome`, counted from the op byte."
  def max_hello_frame, do: 65_536

  @doc "Every op name, in op-code order."
  def ops, do: @ops |> Enum.sort() |> Enum.map(&elem(&1, 1))

  @doc "The op code for an op name."
  def op_code(name) when is_map_key(@codes, name), do: Map.fetch!(@codes, name)

  @doc "The op name for an op code, or `{:unknown, code}`."
  def op_name(code) when is_map_key(@ops, code), do: Map.fetch!(@ops, code)
  def op_name(code), do: {:unknown, code}

  @doc """
  Encodes a frame with its length prefix, as sent on TCP.
  """
  @spec encode(atom(), map(), iodata()) :: iodata()
  def encode(op, header, payload \\ "") do
    body = encode_body(op, header, payload)
    [<<IO.iodata_length(body)::32>> | body]
  end

  @doc """
  Encodes a frame without its length prefix: the form used on WebSocket and with the
  socket option `packet: 4`.
  """
  @spec encode_body(atom(), map(), iodata()) :: iodata()
  def encode_body(op, header, payload \\ "") do
    header = encode_header(header)
    [<<op_code(op), IO.iodata_length(header)::32>>, header, payload]
  end

  defp encode_header(header) when map_size(header) == 0, do: []
  defp encode_header(header), do: Msgpax.pack!(header)

  @doc """
  Splits one frame off the front of a byte stream that carries length prefixes.

  Returns `{:ok, frame, rest}`, `:more` when the buffer does not hold a whole frame yet,
  or `{:error, reason}`. `max_frame` bounds the value of the length prefix.
  """
  @spec decode(binary(), pos_integer()) :: {:ok, t(), binary()} | :more | {:error, String.t()}
  def decode(buffer, max_frame \\ 0xFFFFFFFF)

  def decode(<<len::32, _::binary>>, max_frame) when len > max_frame,
    do: {:error, "frame of #{len} bytes exceeds the maximum of #{max_frame} bytes"}

  def decode(<<len::32, _::binary>>, _max_frame) when len < 5,
    do: {:error, "malformed frame: frame shorter than 5 bytes"}

  def decode(<<len::32, body::binary-size(len), rest::binary>>, _max_frame) do
    with {:ok, frame} <- decode_body(body), do: {:ok, frame, rest}
  end

  def decode(_buffer, _max_frame), do: :more

  @doc """
  Decodes a frame without its length prefix.
  """
  @spec decode_body(binary()) :: {:ok, t()} | {:error, String.t()}
  def decode_body(<<op, hlen::32, rest::binary>>) when byte_size(rest) >= hlen do
    <<header::binary-size(hlen), payload::binary>> = rest

    with {:ok, header} <- decode_header(header) do
      {:ok, {op_name(op), header, payload}}
    end
  end

  def decode_body(<<_op, _hlen::32, _::binary>>),
    do: {:error, "malformed frame: header length exceeds frame"}

  def decode_body(_), do: {:error, "malformed frame: frame shorter than 5 bytes"}

  defp decode_header(""), do: {:ok, %{}}

  defp decode_header(bytes) do
    case Msgpax.unpack(bytes) do
      {:ok, header} when is_map(header) -> {:ok, header}
      {:ok, _} -> {:error, "malformed frame: header is not a MessagePack map"}
      {:error, error} -> {:error, "malformed frame: " <> Exception.message(error)}
    end
  end
end
