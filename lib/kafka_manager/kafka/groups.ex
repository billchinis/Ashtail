defmodule KafkaManager.Kafka.Groups do
  @moduledoc """
  Consumer group listing and detail. Lists every group via
  `Client.list_groups/1`, batches `Client.describe_groups/3` by the
  coordinator each group was found on, then fetches each group's committed
  offsets and combines them with a single batched `Client.list_offsets/3`
  call (per direction) to compute per-partition and total lag.
  """

  alias KafkaManager.Kafka.{BrokerError, Client, Config, Group}

  @doc """
  Lists every consumer group on the cluster with its raw state, member
  count, total lag and per-partition lag breakdown.
  """
  @spec list_groups(Config.t()) :: {:ok, [Group.t()]} | {:error, BrokerError.t()}
  def list_groups(%Config{} = config) do
    with {:ok, groups} <- Client.list_groups(config),
         {:ok, described} <- describe_by_coordinator(config, groups),
         {:ok, metadata} <- Client.metadata(config),
         {:ok, commits_by_group} <- fetch_all_committed(config, described) do
      build_groups(config, metadata, described, commits_by_group)
    end
  end

  @doc """
  A single consumer group's raw state, member count, total lag and
  per-partition lag breakdown, via the same coordinator/describe/committed
  offsets pipeline as `list_groups/1`, scoped to one group id.
  """
  @spec get_group(Config.t(), String.t()) :: {:ok, Group.t()} | {:error, BrokerError.t()}
  def get_group(%Config{} = config, group_id) when is_binary(group_id) do
    with {:ok, groups} <- Client.list_groups(config),
         {:ok, summary} <- find_group_summary(config, groups, group_id),
         {:ok, [described]} <- Client.describe_groups(config, summary.coordinator, [group_id]),
         {:ok, commits} <- Client.fetch_committed_offsets(config, group_id),
         {:ok, metadata} <- Client.metadata(config) do
      partitions_by_topic = partitions_by_topic(metadata)
      pairs = committed_partition_pairs(commits, partitions_by_topic)

      with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
           {:ok, latest} <- Client.list_offsets(config, pairs, :latest) do
        {:ok, build_group(described, commits, partitions_by_topic, earliest, latest)}
      end
    end
  end

  defp find_group_summary(config, groups, group_id) do
    case Enum.find(groups, &(&1.id == group_id)) do
      nil -> {:error, unknown_group_error(config, group_id)}
      summary -> {:ok, summary}
    end
  end

  defp unknown_group_error(config, group_id) do
    %BrokerError{
      address: Config.address(config),
      reason: :unknown_group,
      message: "Consumer group #{group_id} does not exist on this cluster."
    }
  end

  defp committed_partition_pairs(commits, partitions_by_topic) do
    commits
    |> topics_committed()
    |> Enum.uniq()
    |> Enum.flat_map(fn topic ->
      Enum.map(Map.get(partitions_by_topic, topic, []), &{topic, &1})
    end)
    |> Enum.uniq()
  end

  defp describe_by_coordinator(_config, []), do: {:ok, []}

  defp describe_by_coordinator(config, groups) do
    groups
    |> Enum.group_by(& &1.coordinator, & &1.id)
    |> Enum.reduce_while({:ok, []}, fn {coordinator, ids}, {:ok, acc} ->
      case Client.describe_groups(config, coordinator, ids) do
        {:ok, described} -> {:cont, {:ok, acc ++ described}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp fetch_all_committed(config, described) do
    Enum.reduce_while(described, {:ok, %{}}, fn %{id: id}, {:ok, acc} ->
      case Client.fetch_committed_offsets(config, id) do
        {:ok, commits} -> {:cont, {:ok, Map.put(acc, id, commits)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp build_groups(config, metadata, described, commits_by_group) do
    partitions_by_topic = partitions_by_topic(metadata)

    pairs =
      commits_by_group
      |> Map.values()
      |> Enum.flat_map(&topics_committed/1)
      |> Enum.uniq()
      |> Enum.flat_map(fn topic ->
        Enum.map(Map.get(partitions_by_topic, topic, []), &{topic, &1})
      end)
      |> Enum.uniq()

    with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
         {:ok, latest} <- Client.list_offsets(config, pairs, :latest) do
      groups =
        Enum.map(described, fn group ->
          commits = Map.get(commits_by_group, group.id, %{})
          build_group(group, commits, partitions_by_topic, earliest, latest)
        end)

      {:ok, groups}
    end
  end

  defp topics_committed(commits), do: commits |> Map.keys() |> Enum.map(&elem(&1, 0))

  defp build_group(group, commits, partitions_by_topic, earliest, latest) do
    topics = commits |> topics_committed() |> Enum.uniq()

    partitions =
      for topic <- topics,
          partition <- Map.get(partitions_by_topic, topic, []) do
        key = {topic, partition}
        committed = Map.get(commits, key, Map.fetch!(earliest, key))
        latest_offset = Map.fetch!(latest, key)

        %{
          topic: topic,
          partition: partition,
          committed_offset: committed,
          latest_offset: latest_offset,
          lag: max(0, latest_offset - committed)
        }
      end
      |> Enum.sort_by(&{&1.topic, &1.partition})

    %Group{
      id: group.id,
      state: group.state,
      protocol_type: group.protocol_type,
      member_count: group.member_count,
      total_lag: Enum.sum(Enum.map(partitions, & &1.lag)),
      partitions: partitions,
      expanded?: false
    }
  end

  defp partitions_by_topic(%{topics: topics}) do
    for %{name: name, partitions: partitions} <- topics, into: %{} do
      {name, Enum.map(partitions, & &1.partition_index)}
    end
  end
end
