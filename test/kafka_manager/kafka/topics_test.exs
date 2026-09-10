defmodule KafkaManager.Kafka.TopicsTest do
  @moduledoc """
  Regression: a `{topic, partition}` missing from a batched offsets map
  (dropped because that partition came back from `Client.list_offsets/3`
  with an error) must become a `%BrokerError{}` instead of raising via
  `Map.fetch!/2` in the calling process, outside `Client`'s own crash
  containment.

  The `attach_partition_offsets/5` tests below exercise the actual caller of
  `fetch_offset/3`, not just the leaf helper: temporarily reverting
  `attach_partition_offsets/5` in `lib/kafka_manager/kafka/topics.ex` to use
  `Map.fetch!/2` directly makes those two tests raise instead of pass.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{BrokerError, Partition, Topics}

  setup do
    {:ok, config: KafkaManager.Kafka.config()}
  end

  test "a missing offset key surfaces as a BrokerError instead of raising", %{config: config} do
    assert {:error, %BrokerError{reason: :missing_offset, message: message}} =
             Topics.fetch_offset(config, %{}, {"orders", 0})

    assert message =~ "orders/0"
  end

  test "a present offset key is returned as-is", %{config: config} do
    assert {:ok, 42} = Topics.fetch_offset(config, %{{"orders", 0} => 42}, {"orders", 0})
  end

  test "attach_partition_offsets/5 surfaces a missing earliest key as a BrokerError",
       %{config: config} do
    partition = %Partition{
      id: 0,
      leader: 1,
      replicas: [1],
      earliest: 0,
      latest: 0,
      message_count: 0
    }

    latest = %{{"orders", 0} => 42}

    assert {:error, %BrokerError{reason: :missing_offset}} =
             Topics.attach_partition_offsets(config, partition, "orders", %{}, latest)
  end

  test "attach_partition_offsets/5 surfaces a missing latest key as a BrokerError",
       %{config: config} do
    partition = %Partition{
      id: 0,
      leader: 1,
      replicas: [1],
      earliest: 0,
      latest: 0,
      message_count: 0
    }

    earliest = %{{"orders", 0} => 10}

    assert {:error, %BrokerError{reason: :missing_offset}} =
             Topics.attach_partition_offsets(config, partition, "orders", earliest, %{})
  end
end
