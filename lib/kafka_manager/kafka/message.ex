defmodule KafkaManager.Kafka.Message do
  @moduledoc """
  A single message read from a partition. `key == nil` means a null key:
  brod normalises a NULL key to `<<>>` on read, and `Client` maps `<<>>`,
  `:undefined` and `:null` to `nil` at the boundary.

  `partition` (2026-09-11) is set by `Client.fetch/5`, which knows the
  partition it asked for. It is not in `@enforce_keys`, so existing test
  fixtures that build `%Message{}` without it keep compiling.
  """

  @enforce_keys [:offset, :key, :value, :timestamp, :headers]
  defstruct [:offset, :key, :value, :timestamp, :headers, :partition]

  @type header :: {String.t(), String.t()}

  @type t :: %__MODULE__{
          offset: non_neg_integer(),
          key: String.t() | nil,
          value: String.t(),
          timestamp: DateTime.t(),
          headers: [header()],
          partition: non_neg_integer() | nil
        }
end
