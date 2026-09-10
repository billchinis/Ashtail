defmodule KafkaManager.Kafka.ClientTest do
  @moduledoc """
  Regression: a partition-level `error_code` in a `ListOffsets` response
  must never surface as `offset: -1` for a caller to do arithmetic on.
  """

  use ExUnit.Case, async: true

  alias KafkaManager.Kafka.Client

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
end
