defmodule FireMQ.Message do
  @moduledoc """
  A message received with `FireMQ.subscribe/3` or `FireMQ.get/3`.

    * `body` - the raw body, a binary.
    * `exchange` and `routing_key` - where the message was published.
    * `properties` - a map with atom keys, holding only the properties that are set:
      `:content_type`, `:content_encoding`, `:correlation_id`, `:reply_to`, `:expiration`
      (milliseconds), `:message_id`, `:timestamp` (seconds since the Unix epoch), `:type`,
      `:user_id`, `:app_id` and `:priority`.
    * `headers` - application headers, a map with string keys.
    * `redelivered` - `true` when the message was delivered before and not acknowledged.
    * `delivery_tag` - the broker's tag for this delivery.

  Settle a message with `FireMQ.ack/1` or `FireMQ.nack/2`, unless it was received with
  `no_ack: true`.
  """

  @type t :: %__MODULE__{
          body: binary(),
          exchange: String.t(),
          routing_key: String.t(),
          properties: map(),
          headers: map(),
          redelivered: boolean(),
          delivery_tag: non_neg_integer(),
          no_ack: boolean(),
          conn: pid() | nil
        }

  defstruct body: "",
            exchange: "",
            routing_key: "",
            properties: %{},
            headers: %{},
            redelivered: false,
            delivery_tag: 0,
            no_ack: false,
            conn: nil

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(message, opts) do
      fields = [
        exchange: message.exchange,
        routing_key: message.routing_key,
        delivery_tag: message.delivery_tag,
        redelivered: message.redelivered,
        properties: message.properties,
        headers: message.headers,
        body: message.body
      ]

      concat(["#FireMQ.Message<", to_doc(fields, opts), ">"])
    end
  end
end
