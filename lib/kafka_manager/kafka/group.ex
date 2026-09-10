defmodule KafkaManager.Kafka.Group do
  @moduledoc """
  A consumer group as shown by the group list and group detail pages: its raw
  Kafka state, member count, total lag and (when fetched) its per-partition
  committed offsets and lag.
  """

  @enforce_keys [:id, :state, :protocol_type, :member_count, :total_lag]
  defstruct [
    :id,
    :state,
    :protocol_type,
    :member_count,
    :total_lag,
    partitions: [],
    expanded?: false
  ]

  @type partition_lag :: %{
          topic: String.t(),
          partition: non_neg_integer(),
          committed_offset: integer(),
          latest_offset: integer(),
          lag: non_neg_integer()
        }

  @type t :: %__MODULE__{
          id: String.t(),
          state: String.t(),
          protocol_type: String.t(),
          member_count: non_neg_integer(),
          total_lag: non_neg_integer(),
          partitions: [partition_lag()],
          expanded?: boolean()
        }
end
