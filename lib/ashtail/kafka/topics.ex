defmodule Ashtail.Kafka.Topics do
  @moduledoc """
  Topic listing and detail. Search, sorting and pagination happen here, in
  Elixir, against the full metadata fetched from the broker: Kafka itself has
  no server-side topic paging. Offsets are fetched only for the topics on the
  visible page, except when sorting by message count, which needs the count of
  every topic that matches the search.
  """

  alias Ashtail.Kafka.{BrokerError, Client, Config, Listing, Partition, Topic}

  @sort_keys [:name, :partitions, :replication, :messages]

  @doc "The columns `list_topics/2` can sort by."
  @spec sort_keys() :: [atom()]
  def sort_keys, do: @sort_keys

  @doc """
  Lists topics, filtered by a case-insensitive substring search on the name,
  sorted (see `sort_topics/3`), and paginated.

  Options: `:search`, `:page` (default 1), `:page_size` (default 20),
  `:sort` (one of `sort_keys/0`, default `:name`) and `:dir` (`:asc` or
  `:desc`, default `:asc`).
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
    sort = Keyword.get(opts, :sort, :name)
    dir = Keyword.get(opts, :dir, :asc)

    with {:ok, metadata} <- Client.metadata(config),
         topics = metadata |> topics_from_metadata() |> filter_by_search(search),
         {:ok, topics} <- sort_with_offsets(config, topics, sort, dir),
         %{items: page_topics} = page <- Listing.paginate(topics, requested_page, page_size),
         {:ok, page_topics} <- ensure_offsets(config, page_topics, sort) do
      {:ok, page |> Map.delete(:items) |> Map.put(:topics, page_topics)}
    end
  end

  @doc """
  Sorts topics by `key` (one of `sort_keys/0`) in direction `dir`. Ties are
  always broken by name ascending, whatever the direction.
  """
  @spec sort_topics([Topic.t()], atom(), :asc | :desc) :: [Topic.t()]
  def sort_topics(topics, key, dir) when key in @sort_keys do
    Listing.sort(topics, sort_value(key), & &1.name, dir)
  end

  defp sort_value(:name), do: & &1.name
  defp sort_value(:partitions), do: & &1.partition_count
  defp sort_value(:replication), do: & &1.replication_factor
  defp sort_value(:messages), do: & &1.message_count

  # Message counts come from offsets, so that sort needs them for every
  # matching topic up front. The other sorts only use metadata.
  defp sort_with_offsets(config, topics, :messages, dir) do
    with {:ok, topics} <- attach_offsets(config, topics) do
      {:ok, sort_topics(topics, :messages, dir)}
    end
  end

  defp sort_with_offsets(_config, topics, sort, dir), do: {:ok, sort_topics(topics, sort, dir)}

  defp ensure_offsets(_config, topics, :messages), do: {:ok, topics}
  defp ensure_offsets(config, topics, _sort), do: attach_offsets(config, topics)

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
  topic sub-menu header renders.
  """
  @spec topic_summary(Config.t(), String.t()) :: {:ok, Topic.t()} | {:error, BrokerError.t()}
  def topic_summary(%Config{} = config, name) when is_binary(name) do
    with {:ok, metadata} <- Client.metadata(config),
         {:ok, topic} <- find_topic(metadata, config, name),
         {:ok, [topic]} <- attach_offsets(config, [topic]) do
      {:ok, topic}
    end
  end

  @doc """
  Log directory usage for every partition replica of a topic. See
  `Ashtail.Kafka.Client.describe_log_dirs/2`.
  """
  @spec topic_log_dirs(Config.t(), String.t()) ::
          {:ok,
           [
             %{
               partition: non_neg_integer(),
               broker: integer(),
               log_dir: String.t(),
               size_bytes: non_neg_integer(),
               offset_lag: integer()
             }
           ]}
          | {:error, BrokerError.t()}
  def topic_log_dirs(%Config{} = config, name) when is_binary(name) do
    Client.describe_log_dirs(config, name)
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
end
