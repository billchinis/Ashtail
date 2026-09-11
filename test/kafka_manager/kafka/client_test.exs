defmodule KafkaManager.Kafka.ClientTest do
  @moduledoc """
  Regression: a partition-level `error_code` in a `ListOffsets` response
  must never surface as `offset: -1` for a caller to do arithmetic on; a
  pre-epoch timestamp sent to `list_offsets/3` must not collide with the
  ListOffsets `-1` ("latest") sentinel; and a replica whose broker is
  missing from metadata must not raise inside `describe_log_dirs/2`.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.{BrokerError, Client}

  setup do
    {:ok, config: KafkaManager.Kafka.config()}
  end

  test "a partition-level error is dropped instead of leaking offset: -1" do
    response = %{
      responses: [
        %{
          topic: "orders",
          partition_responses: [
            %{partition: 0, error_code: :no_error, offset: 42},
            %{partition: 1, error_code: :unknown_topic_or_partition, offset: -1}
          ]
        }
      ]
    }

    assert Client.parse_list_offsets_response(response) == %{{"orders", 0} => 42}
  end

  test "a pre-epoch timestamp is clamped to 0 instead of colliding with the -1 sentinel", %{
    config: config
  } do
    pair = {"orders", 0}

    assert {:ok, earliest} = Client.list_offsets(config, [pair], :earliest)
    assert {:ok, latest} = Client.list_offsets(config, [pair], :latest)
    assert earliest[pair] != latest[pair]

    # 1969-12-31T23:59:59.999Z is ms = -1, the broker's own sentinel for
    # "latest". Sent unclamped, "the first offset at or after this instant"
    # would come back as the partition's latest offset instead of its
    # earliest, even though every message on the partition qualifies.
    assert {:ok, at_pre_epoch} = Client.list_offsets(config, [pair], {:timestamp, -1})
    assert at_pre_epoch[pair] == earliest[pair]
  end

  test "a replica's broker missing from metadata is a readable BrokerError, not a raise", %{
    config: config
  } do
    assert {:error, %BrokerError{reason: :missing_broker, message: message}} =
             Client.log_dirs_from_broker(config, %{}, 7, "orders", [0])

    assert message =~ "Broker 7"
  end
end
