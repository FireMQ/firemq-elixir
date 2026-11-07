defmodule FireMQ.QueueInfo do
  @moduledoc """
  What `FireMQ.declare_queue/3` reports: the queue's name (useful when the broker picked
  it), how many messages are ready, and how many consumers it has.
  """

  @type t :: %__MODULE__{
          name: String.t(),
          messages: non_neg_integer(),
          consumers: non_neg_integer()
        }

  defstruct name: "", messages: 0, consumers: 0
end
