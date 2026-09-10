defmodule KafkaManager.Kafka.TopicsTest do
  @moduledoc """
  Regression: a `{topic, partition}` missing from a batched offsets map
  (dropped because that partition came back from `Client.list_offsets/3`
  with an error) must become a `%BrokerError{}` instead of raising via
  `Map.fetch!/2` in the calling process, outside `Client`'s own crash
  containment.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{BrokerError, Topics}

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
end
