defmodule KafkaManager.Kafka.Topics do
  @moduledoc """
  Topic listing and detail. Search and pagination happen here, in Elixir,
  against the full metadata fetched from the broker: Kafka itself has no
  server-side topic paging. Offsets are only fetched for the topics on the
  visible page, never for the whole cluster.
  """

  alias KafkaManager.Kafka.{BrokerError, Client, Config, Partition, Topic}

  @doc """
  Lists topics, filtered by a case-insensitive substring search on the name,
  sorted by name ascending, paginated, with per-partition offsets (and so
  `message_count`) fetched only for the returned page.
  """
  @spec list_topics(Config.t(), keyword()) ::
          {:ok,
           %{
             topics: [Topic.t()],
             total: non_neg_integer(),
             page: pos_integer(),
             page_size: pos_integer(),
             page_count: pos_integer()
           }}
          | {:error, BrokerError.t()}
  def list_topics(%Config{} = config, opts \\ []) do
    search = Keyword.get(opts, :search)
    page_size = Keyword.get(opts, :page_size, 20)
    requested_page = Keyword.get(opts, :page, 1)

    with {:ok, metadata} <- Client.metadata(config) do
      topics =
        metadata
        |> topics_from_metadata()
        |> filter_by_search(search)
        |> Enum.sort_by(& &1.name)

      total = length(topics)
      page_count = max(1, ceil_div(total, page_size))
      page = clamp(requested_page, 1, page_count)
      page_topics = Enum.slice(topics, (page - 1) * page_size, page_size)

      with {:ok, topics_with_offsets} <- attach_offsets(config, page_topics) do
        {:ok,
         %{
           topics: topics_with_offsets,
           total: total,
           page: page,
           page_size: page_size,
           page_count: page_count
         }}
      end
    end
  end

  defp topics_from_metadata(%{topics: topics}) do
    topics
    |> Enum.reject(& &1[:is_internal])
    |> Enum.map(&topic_from_metadata/1)
  end

  defp topic_from_metadata(%{name: name, partitions: partitions}) do
    replication_factor =
      case partitions do
        [%{replica_nodes: replicas} | _] -> length(replicas)
        [] -> 0
      end

    %Topic{
      name: name,
      partition_count: length(partitions),
      replication_factor: replication_factor,
      message_count: 0,
      partitions:
        Enum.map(partitions, fn %{partition_index: id, leader_id: leader, replica_nodes: replicas} ->
          %Partition{
            id: id,
            leader: leader,
            replicas: replicas,
            earliest: 0,
            latest: 0,
            message_count: 0
          }
        end)
    }
  end

  defp filter_by_search(topics, nil), do: topics
  defp filter_by_search(topics, ""), do: topics

  defp filter_by_search(topics, search) do
    needle = String.downcase(search)
    Enum.filter(topics, &String.contains?(String.downcase(&1.name), needle))
  end

  defp attach_offsets(_config, []), do: {:ok, []}

  defp attach_offsets(config, topics) do
    pairs = for topic <- topics, partition <- topic.partitions, do: {topic.name, partition.id}

    with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
         {:ok, latest} <- Client.list_offsets(config, pairs, :latest) do
      {:ok, Enum.map(topics, &attach_topic_offsets(&1, earliest, latest))}
    end
  end

  defp attach_topic_offsets(topic, earliest, latest) do
    partitions =
      Enum.map(topic.partitions, fn partition ->
        key = {topic.name, partition.id}
        partition_earliest = Map.fetch!(earliest, key)
        partition_latest = Map.fetch!(latest, key)

        %Partition{
          partition
          | earliest: partition_earliest,
            latest: partition_latest,
            message_count: partition_latest - partition_earliest
        }
      end)

    %Topic{
      topic
      | partitions: partitions,
        message_count: Enum.sum(Enum.map(partitions, & &1.message_count))
    }
  end

  defp ceil_div(total, page_size), do: div(total + page_size - 1, page_size)

  defp clamp(value, min, max), do: value |> max(min) |> min(max)
end
