defmodule FireMQ.Error do
  @moduledoc """
  An error returned by the client.

  `kind` says what failed:

    * `:server` - the broker refused a request. `code` is an AMQP reply code such as
      `404` (not found), `403` (access refused) or `406` (precondition failed).
    * `:unroutable` - a `mandatory` publish reached no queue. `code` is `312`.
    * `:nacked` - the broker rejected a publish, for example because a queue with
      `x-overflow: reject-publish` is full. `code` is the broker's reason code.
    * `:closed` - the connection is closed, or could not be opened. `code` is the close
      code when the broker closed it, and `nil` otherwise.
    * `:timeout` - the broker did not answer in time.
    * `:protocol` - the broker sent something this client cannot read, or a frame would
      exceed the broker's size limit.
    * `:url` - the connection URL is invalid.

  `FireMQ.Error` is an exception, so the bang functions such as `FireMQ.publish!/5` raise it.
  """

  @type kind :: :server | :unroutable | :nacked | :closed | :timeout | :protocol | :url

  @type t :: %__MODULE__{kind: kind(), code: non_neg_integer() | nil, message: String.t()}

  defexception kind: :closed, code: nil, message: ""

  @impl true
  def message(%__MODULE__{code: nil, message: message}), do: message
  def message(%__MODULE__{code: code, message: message}), do: "#{message} (code #{code})"

  @doc false
  def closed(message, code \\ nil), do: %__MODULE__{kind: :closed, code: code, message: message}
end
