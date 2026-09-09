defmodule KafkaManager.Kafka.Topic do
  @moduledoc """
  A topic as shown by the topic list and topic detail pages: its partition
  count, replication factor, total message count and (when fetched) its
  per-partition breakdown and broker configuration.
  """

  alias KafkaManager.Kafka.Partition

  @enforce_keys [:name, :partition_count, :replication_factor, :message_count]
  defstruct [
    :name,
    :partition_count,
    :replication_factor,
    :message_count,
    partitions: [],
    config: []
  ]

  @type t :: %__MODULE__{
          name: String.t(),
          partition_count: non_neg_integer(),
          replication_factor: non_neg_integer(),
          message_count: non_neg_integer(),
          partitions: [Partition.t()],
          config: [%{name: String.t(), value: String.t(), source: String.t()}]
        }
end
