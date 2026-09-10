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
         {:ok, described_list} <-
           Client.describe_groups(config, summary.coordinator, [group_id]),
         {:ok, described} <- require_described(config, group_id, described_list),
         {:ok, commits} <- Client.fetch_committed_offsets(config, group_id),
         {:ok, metadata} <- Client.metadata(config) do
      partitions_by_topic = partitions_by_topic(metadata)
      pairs = committed_partition_pairs(commits, partitions_by_topic)

      with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
           {:ok, latest} <- Client.list_offsets(config, pairs, :latest) do
        build_group(config, described, commits, partitions_by_topic, earliest, latest)
      end
    end
  end

  # `describe_groups/3` is expected to answer with exactly one description
  # per requested id. If the group vanished between `list_groups/1` and this
  # call, some brokers answer with an empty list rather than a "Dead" entry;
  # either way that must not fall through the `with` in `get_group/2` as a
  # bare `{:ok, []}` (which does not have the shape `GroupLive.Show` expects
  # and crashes it with a `KeyError`). Exposed (`@doc false`) so this exact
  # translation is unit-testable without needing to force the race live.
  @doc false
  @spec require_described(Config.t(), String.t(), [map()]) ::
          {:ok, map()} | {:error, BrokerError.t()}
  def require_described(_config, _group_id, [described]), do: {:ok, described}
  def require_described(config, group_id, []), do: {:error, unknown_group_error(config, group_id)}

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
      reduce_ok(described, fn group ->
        commits = Map.get(commits_by_group, group.id, %{})
        build_group(config, group, commits, partitions_by_topic, earliest, latest)
      end)
    end
  end

  defp topics_committed(commits), do: commits |> Map.keys() |> Enum.map(&elem(&1, 0))

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

  # A `{topic, partition}` missing from `earliest`/`latest` (dropped by
  # `Client.list_offsets/3` because that partition came back with a
  # partition-level error) must not raise via `Map.fetch!/2` here in the
  # calling process, outside `Client`'s own crash containment. It becomes a
  # `%BrokerError{}` instead.
  defp build_group(config, group, commits, partitions_by_topic, earliest, latest) do
    pairs =
      for topic <- commits |> topics_committed() |> Enum.uniq(),
          partition <- Map.get(partitions_by_topic, topic, []),
          do: {topic, partition}

    pairs
    |> reduce_ok(&partition_entry(config, commits, earliest, latest, &1))
    |> case do
      {:ok, entries} -> {:ok, group_from_partitions(group, entries)}
      {:error, _} = error -> error
    end
  end

  defp partition_entry(config, commits, earliest, latest, {topic, partition} = key) do
    with {:ok, committed} <- committed_or_earliest(config, commits, earliest, key),
         {:ok, latest_offset} <- fetch_offset(config, latest, key) do
      {:ok,
       %{
         topic: topic,
         partition: partition,
         committed_offset: committed,
         latest_offset: latest_offset,
         lag: max(0, latest_offset - committed)
       }}
    end
  end

  defp committed_or_earliest(config, commits, earliest, key) do
    case Map.fetch(commits, key) do
      {:ok, committed} -> {:ok, committed}
      :error -> fetch_offset(config, earliest, key)
    end
  end

  # Exposed (`@doc false`) so the "missing key becomes an error, never a
  # raise" translation is unit-testable without forcing a live topic/
  # partition inconsistency against the broker.
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
          "since the group's membership was fetched."
    }
  end

  defp group_from_partitions(group, partitions) do
    partitions = Enum.sort_by(partitions, &{&1.topic, &1.partition})

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
