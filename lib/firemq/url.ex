defmodule FireMQ.URL do
  @moduledoc """
  Parses connection URLs.

      fire://user:password@host:5673/vhost

  The schemes are `fire` (TCP, port 5673), `fires` (TLS, port 5674), `ws` (port 15672)
  and `wss` (port 15671). User and password default to `guest`. For `fire` and `fires`
  the vhost is the path without its leading `/`, percent-decoded; an empty path means
  vhost `/`. For `ws` and `wss` the path is the WebSocket endpoint (`/fire` when empty)
  and the vhost is the `vhost` query parameter.

  This client connects over `fire` and `fires`. WebSocket URLs parse, but `FireMQ.connect/2`
  rejects them.
  """

  @type transport :: :tcp | :tls | :ws | :wss

  @type t :: %__MODULE__{
          transport: transport(),
          host: String.t(),
          port: :inet.port_number(),
          vhost: String.t(),
          user: String.t(),
          pass: String.t(),
          path: String.t()
        }

  @derive {Inspect, except: [:pass]}
  defstruct transport: :tcp,
            host: "localhost",
            port: 5673,
            vhost: "/",
            user: "guest",
            pass: "guest",
            path: ""

  @defaults %{
    "fire" => {:tcp, 5673},
    "fires" => {:tls, 5674},
    "ws" => {:ws, 15672},
    "wss" => {:wss, 15671}
  }

  @doc """
  Parses a URL and fills in the defaults.

      iex> {:ok, url} = FireMQ.URL.parse("fire://localhost")
      iex> {url.transport, url.port, url.vhost, url.user}
      {:tcp, 5673, "/", "guest"}
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, FireMQ.Error.t()}
  def parse(input) when is_binary(input) do
    with [scheme, rest] <- String.split(input, "://", parts: 2),
         {:ok, {transport, default_port}} <- Map.fetch(@defaults, String.downcase(scheme)) do
      # URI fills in default ports for schemes it knows, such as ws (80). Parse under a
      # neutral scheme so a missing port stays missing.
      {:ok, build(URI.parse("firemq://" <> rest), transport, default_port)}
    else
      _ ->
        {:error, invalid("unsupported scheme in #{inspect(input)}; use fire, fires, ws or wss")}
    end
  end

  defp build(uri, transport, default_port) do
    {user, pass} = userinfo(uri.userinfo)
    query_vhost = query_vhost(uri.query)

    {vhost, path} =
      case transport do
        t when t in [:tcp, :tls] ->
          case String.trim_leading(uri.path || "", "/") do
            "" -> {query_vhost || "/", ""}
            raw -> {URI.decode(raw), ""}
          end

        _ ->
          path = if uri.path in [nil, "", "/"], do: "/fire", else: uri.path
          {query_vhost || "/", path}
      end

    %__MODULE__{
      transport: transport,
      host: if(uri.host in [nil, ""], do: "localhost", else: uri.host),
      port: uri.port || default_port,
      vhost: vhost,
      user: user,
      pass: pass,
      path: path
    }
  end

  defp userinfo(nil), do: {"guest", "guest"}

  defp userinfo(info) do
    case String.split(info, ":", parts: 2) do
      [user] -> {decode_user(user), "guest"}
      [user, pass] -> {decode_user(user), URI.decode(pass)}
    end
  end

  defp decode_user(""), do: "guest"
  defp decode_user(user), do: URI.decode(user)

  defp query_vhost(nil), do: nil
  defp query_vhost(query), do: query |> URI.decode_query() |> Map.get("vhost")

  defp invalid(message), do: %FireMQ.Error{kind: :url, message: message}

  @doc false
  # Host in the form `:gen_tcp.connect/4` and `:ssl.connect/4` accept.
  def connect_host(%__MODULE__{host: host}) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> address
      {:error, _} -> String.to_charlist(host)
    end
  end
end
