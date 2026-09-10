defmodule KafkaManager.Kafka.MessagesTest do
  @moduledoc """
  Regression: a `{topic, partition}` missing from a batched offsets map
  (dropped because that partition came back from `Client.list_offsets/3`
  with an error) must become a `%BrokerError{}` instead of raising via
  `Map.fetch!/2` in the calling process, outside `Client`'s own crash
  containment.

  The `resolve_bounds/4` tests below exercise the actual call site inside
  `fetch_messages/5`, not just the leaf helper: temporarily reverting
  `resolve_bounds/4` in `lib/kafka_manager/kafka/messages.ex` to use
  `Map.fetch!/2` directly makes those two tests raise instead of pass.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{BrokerError, Messages}

  setup do
    {:ok, config: KafkaManager.Kafka.config()}
  end

  test "a missing offset key surfaces as a BrokerError instead of raising", %{config: config} do
    assert {:error, %BrokerError{reason: :missing_offset, message: message}} =
             Messages.fetch_offset(config, %{}, {"orders", 0})

    assert message =~ "orders/0"
  end

  test "a present offset key is returned as-is", %{config: config} do
    assert {:ok, 42} = Messages.fetch_offset(config, %{{"orders", 0} => 42}, {"orders", 0})
  end

  test "resolve_bounds/4 surfaces a missing earliest key as a BrokerError", %{config: config} do
    pair = {"orders", 0}
    latest_offsets = %{pair => 42}

    assert {:error, %BrokerError{reason: :missing_offset}} =
             Messages.resolve_bounds(config, pair, %{}, latest_offsets)
  end

  test "resolve_bounds/4 surfaces a missing latest key as a BrokerError", %{config: config} do
    pair = {"orders", 0}
    earliest_offsets = %{pair => 10}

    assert {:error, %BrokerError{reason: :missing_offset}} =
             Messages.resolve_bounds(config, pair, earliest_offsets, %{})
  end
end
