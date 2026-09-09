defmodule KafkaManager.BrokerHelpers do
  @moduledoc """
  Test-only ways to reach past the app's own boundaries: producing into
  `scratch` (the only topic tests may write to) through the app's own
  `Kafka.produce/3`, never through `rpk`/docker directly.
  """

  alias KafkaManager.Kafka

  @doc """
  A thin wrapper over `Kafka.produce/3`, used by AC-9's test to write the
  `tail-probe` message.
  """
  @spec produce_probe(String.t(), non_neg_integer(), map()) ::
          {:ok, map()} | {:error, KafkaManager.Kafka.BrokerError.t()}
  def produce_probe(topic, partition, attrs) when is_binary(topic) and is_map(attrs) do
    Kafka.produce(topic, partition, attrs)
  end
end
