defmodule KafkaManager.Kafka.Message do
  @moduledoc """
  A single message read from a partition. `key == nil` means a null key:
  brod normalises a NULL key to `<<>>` on read, and `Client` maps `<<>>`,
  `:undefined` and `:null` to `nil` at the boundary.
  """

  @enforce_keys [:offset, :key, :value, :timestamp, :headers]
  defstruct [:offset, :key, :value, :timestamp, :headers]

  @type header :: {String.t(), String.t()}

  @type t :: %__MODULE__{
          offset: non_neg_integer(),
          key: String.t() | nil,
          value: String.t(),
          timestamp: DateTime.t(),
          headers: [header()]
        }
end
