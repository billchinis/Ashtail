defmodule Ashtail.Kafka.TopicsSortTest do
  @moduledoc """
  Sorting the topic list by name, partition count, replication factor or
  message count, in either direction, with the name as the tie-breaker.
  """

  use ExUnit.Case, async: true

  alias Ashtail.Kafka.{Topic, Topics}

  defp topic(name, partitions, rf, messages) do
    %Topic{
      name: name,
      partition_count: partitions,
      replication_factor: rf,
      message_count: messages,
      partitions: []
    }
  end

  defp topics do
    [
      topic("b", 3, 1, 10),
      topic("a", 3, 2, 30),
      topic("c", 1, 3, 20)
    ]
  end

  defp names(topics), do: Enum.map(topics, & &1.name)

  defp sorted(key, dir), do: topics() |> Topics.sort_topics(key, dir) |> names()

  describe "sort_topics/3" do
    test "sorts by name in both directions" do
      assert sorted(:name, :asc) == ["a", "b", "c"]
      assert sorted(:name, :desc) == ["c", "b", "a"]
    end

    test "sorts by partition count, breaking ties by name ascending" do
      assert sorted(:partitions, :asc) == ["c", "a", "b"]
      assert sorted(:partitions, :desc) == ["a", "b", "c"]
    end

    test "sorts by replication factor" do
      assert sorted(:replication, :asc) == ["b", "a", "c"]
      assert sorted(:replication, :desc) == ["c", "a", "b"]
    end

    test "sorts by message count" do
      assert sorted(:messages, :asc) == ["b", "c", "a"]
      assert sorted(:messages, :desc) == ["a", "c", "b"]
    end
  end

  describe "list_topics/2 against the seeded broker" do
    setup do
      {:ok, config: Ashtail.Kafka.config()}
    end

    test "defaults to name ascending", %{config: config} do
      {:ok, %{topics: topics}} = Topics.list_topics(config, page_size: 50)
      assert names(topics) == Enum.sort(names(topics))
    end

    test "sorting by messages ranks every topic, not just the first page", %{config: config} do
      {:ok, %{topics: topics}} =
        Topics.list_topics(config, page_size: 20, sort: :messages, dir: :desc)

      counts = Enum.map(topics, & &1.message_count)
      assert counts == Enum.sort(counts, :desc)

      # Alphabetically these sit on page 1, but `orders` (600) must outrank
      # every empty topic, so no zero may appear before it.
      assert "orders" in names(topics)
      assert Enum.find_index(topics, &(&1.name == "orders")) < Enum.find_index(counts, &(&1 == 0))
    end

    test "sorting by partitions puts the 12-partition topic first", %{config: config} do
      {:ok, %{topics: [first | _]}} =
        Topics.list_topics(config, sort: :partitions, dir: :desc)

      assert first.name == "notifications"
      assert first.message_count == 48
    end
  end
end
