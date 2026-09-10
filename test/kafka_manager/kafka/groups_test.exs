defmodule KafkaManager.Kafka.GroupsTest do
  @moduledoc """
  Regressions in `KafkaManager.Kafka.Groups`:

  - a `describe_groups/3` result that comes back empty (the group vanished
    between listing and describing) must become a `%BrokerError{}`, not the
    bare `{:ok, []}` that used to fall through the `with` in `get_group/2`
    and crash `GroupLive.Show` with a `KeyError`;
  - a `describe_groups/3` result carrying two or more entries for a single
    requested group id (an unexpected response shape) must also become a
    `%BrokerError{}` rather than a `FunctionClauseError`;
  - a `{topic, partition}` missing from a batched offsets map (dropped
    because that partition came back from `Client.list_offsets/3` with an
    error) must become a `%BrokerError{}` instead of raising via
    `Map.fetch!/2` outside `Client`'s own crash containment.

  The `partition_entry/5` tests below exercise the actual caller of
  `fetch_offset/3`, not just the leaf helper: temporarily reverting
  `partition_entry/5` in `lib/kafka_manager/kafka/groups.ex` to use
  `Map.fetch!/2` directly makes those two tests raise instead of pass.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{BrokerError, Groups}

  setup do
    {:ok, config: KafkaManager.Kafka.config()}
  end

  test "an empty describe_groups result surfaces as a BrokerError", %{config: config} do
    assert {:error, %BrokerError{reason: :unknown_group, message: message}} =
             Groups.require_described(config, "orders-service", [])

    assert message =~ "orders-service"
  end

  test "a single described group passes through unchanged", %{config: config} do
    described = %{id: "orders-service", state: "Stable"}

    assert {:ok, ^described} = Groups.require_described(config, "orders-service", [described])
  end

  test "two or more described entries surface as a BrokerError, not a FunctionClauseError",
       %{config: config} do
    described = [
      %{id: "orders-service", state: "Stable"},
      %{id: "orders-service", state: "Stable"}
    ]

    assert {:error, %BrokerError{reason: :unexpected_response, message: message}} =
             Groups.require_described(config, "orders-service", described)

    assert message =~ "orders-service"
  end

  test "a missing offset key surfaces as a BrokerError instead of raising", %{config: config} do
    assert {:error, %BrokerError{reason: :missing_offset, message: message}} =
             Groups.fetch_offset(config, %{}, {"orders", 0})

    assert message =~ "orders/0"
  end

  test "a present offset key is returned as-is", %{config: config} do
    assert {:ok, 42} = Groups.fetch_offset(config, %{{"orders", 0} => 42}, {"orders", 0})
  end

  test "partition_entry/5 surfaces a missing earliest key as a BrokerError", %{config: config} do
    key = {"orders", 0}
    latest = %{key => 42}

    assert {:error, %BrokerError{reason: :missing_offset}} =
             Groups.partition_entry(config, %{}, %{}, latest, key)
  end

  test "partition_entry/5 surfaces a missing latest key as a BrokerError", %{config: config} do
    key = {"orders", 0}
    commits = %{key => 10}

    assert {:error, %BrokerError{reason: :missing_offset}} =
             Groups.partition_entry(config, commits, %{}, %{}, key)
  end
end
