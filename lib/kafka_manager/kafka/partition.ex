defmodule KafkaManager.Kafka.Partition do
  @moduledoc """
  One partition of a topic: its leader and replica set, plus the earliest and
  latest offsets used to compute `message_count`.
  """

  @enforce_keys [:id, :leader, :replicas, :earliest, :latest, :message_count]
  defstruct [:id, :leader, :replicas, :earliest, :latest, :message_count]

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          leader: integer(),
          replicas: [integer()],
          earliest: integer(),
          latest: integer(),
          message_count: non_neg_integer()
        }
end
