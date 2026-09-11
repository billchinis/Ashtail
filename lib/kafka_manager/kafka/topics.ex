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

  @doc """
  A single topic's per-partition offsets and broker configuration.
  """
  @spec get_topic(Config.t(), String.t()) :: {:ok, Topic.t()} | {:error, BrokerError.t()}
  def get_topic(%Config{} = config, name) when is_binary(name) do
    with {:ok, metadata} <- Client.metadata(config),
         {:ok, topic} <- find_topic(metadata, config, name),
         {:ok, [topic]} <- attach_offsets(config, [topic]),
         {:ok, config_entries} <- Client.describe_topic_config(config, name) do
      {:ok, %Topic{topic | config: config_entries}}
    end
  end

  @doc """
  A single topic's per-partition offsets only, with no broker configuration
  fetched. This is `get_topic/2` minus `DescribeConfigs`: the summary every
  topic sub-menu header renders (docs/PLAN.md P6).
  """
  @spec topic_summary(Config.t(), String.t()) :: {:ok, Topic.t()} | {:error, BrokerError.t()}
  def topic_summary(%Config{} = config, name) when is_binary(name) do
    with {:ok, metadata} <- Client.metadata(config),
         {:ok, topic} <- find_topic(metadata, config, name),
         {:ok, [topic]} <- attach_offsets(config, [topic]) do
      {:ok, topic}
    end
  end

  defp find_topic(metadata, config, name) do
    case metadata |> topics_from_metadata() |> Enum.find(&(&1.name == name)) do
      nil -> {:error, unknown_topic_error(config, name)}
      topic -> {:ok, topic}
    end
  end

  defp unknown_topic_error(config, name) do
    %BrokerError{
      address: Config.address(config),
      reason: :unknown_topic,
      message: "Topic #{name} does not exist on this cluster."
    }
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
      reduce_ok(topics, &attach_topic_offsets(config, &1, earliest, latest))
    end
  end

  # A `{topic, partition}` missing from `earliest`/`latest` (dropped by
  # `Client.list_offsets/3` because that partition came back with a
  # partition-level error, e.g. the topic was deleted mid-request) must not
  # raise via `Map.fetch!/2` here in the calling process, outside `Client`'s
  # own crash containment. It becomes a `%BrokerError{}` instead.
  defp attach_topic_offsets(config, topic, earliest, latest) do
    topic.partitions
    |> reduce_ok(&attach_partition_offsets(config, &1, topic.name, earliest, latest))
    |> case do
      {:ok, partitions} ->
        {:ok,
         %Topic{
           topic
           | partitions: partitions,
             message_count: Enum.sum(Enum.map(partitions, & &1.message_count))
         }}

      {:error, _} = error ->
        error
    end
  end

  # Exposed (`@doc false`) so the "missing key becomes an error, never a
  # raise" translation is unit-testable at the actual call site of
  # `fetch_offset/3`, with a fabricated offsets map, not just on the leaf
  # helper itself.
  @doc false
  @spec attach_partition_offsets(Config.t(), Partition.t(), String.t(), map(), map()) ::
          {:ok, Partition.t()} | {:error, BrokerError.t()}
  def attach_partition_offsets(config, partition, topic_name, earliest, latest) do
    key = {topic_name, partition.id}

    with {:ok, partition_earliest} <- fetch_offset(config, earliest, key),
         {:ok, partition_latest} <- fetch_offset(config, latest, key) do
      {:ok,
       %Partition{
         partition
         | earliest: partition_earliest,
           latest: partition_latest,
           message_count: partition_latest - partition_earliest
       }}
    end
  end

  # Runs `fun` (returning `{:ok, _} | {:error, _}`) over every item,
  # stopping at the first error, otherwise collecting the results in order.
  defp reduce_ok(items, fun) do
    items
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      case fun.(item) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      {:error, _} = error -> error
    end
  end

  # Exposed (`@doc false`) so the "missing key becomes an error, never a
  # raise" translation is unit-testable without forcing a live topic
  # deletion mid-request against the broker.
  @doc false
  @spec fetch_offset(Config.t(), map(), {String.t(), non_neg_integer()}) ::
          {:ok, integer()} | {:error, BrokerError.t()}
  def fetch_offset(config, offsets, {topic, partition} = key) do
    case Map.fetch(offsets, key) do
      {:ok, offset} -> {:ok, offset}
      :error -> {:error, missing_offset_error(config, topic, partition)}
    end
  end

  defp missing_offset_error(config, topic, partition) do
    %BrokerError{
      address: Config.address(config),
      reason: :missing_offset,
      message:
        "No offset was returned for #{topic}/#{partition}. It may have changed " <>
          "since the topic's metadata was fetched."
    }
  end

  defp ceil_div(total, page_size), do: div(total + page_size - 1, page_size)

  defp clamp(value, min, max), do: value |> max(min) |> min(max)
end
