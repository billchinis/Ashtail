defmodule KafkaManager.Kafka.Groups do
  @moduledoc """
  Consumer group listing and detail. Lists every group via
  `Client.list_groups/1`, batches `Client.describe_groups/3` by the
  coordinator each group was found on, then fetches each group's committed
  offsets and combines them with a single batched `Client.list_offsets/3`
  call (per direction) to compute per-partition and total lag.

  `list_groups/1` and `topic_groups/2` are the same private pipeline
  (`fetch_groups/2`) with a scope argument, `:all` or `{:topic, name}`, so the
  two cannot drift apart. In topic scope, a group is kept only if it has
  committed an offset on that topic or has a member currently assigned to it,
  and its `partitions`/`total_lag` are computed over that topic's partitions
  only.
  """

  alias KafkaManager.Kafka.{BrokerError, Client, Config, Group}

  @doc """
  Lists every consumer group on the cluster with its raw state, member
  count, total lag and per-partition lag breakdown.
  """
  @spec list_groups(Config.t()) :: {:ok, [Group.t()]} | {:error, BrokerError.t()}
  def list_groups(%Config{} = config), do: fetch_groups(config, :all)

  @doc """
  Every consumer group that reads the given topic — committed on it, or
  with a member currently assigned to it — with its state and its lag on
  that topic only. Sorted by group id.
  """
  @spec topic_groups(Config.t(), String.t()) :: {:ok, [Group.t()]} | {:error, BrokerError.t()}
  def topic_groups(%Config{} = config, topic) when is_binary(topic) do
    with {:ok, groups} <- fetch_groups(config, {:topic, topic}) do
      {:ok, Enum.sort_by(groups, & &1.id)}
    end
  end

  defp fetch_groups(config, scope) do
    with {:ok, groups} <- Client.list_groups(config),
         {:ok, described} <- describe_by_coordinator(config, groups),
         {:ok, metadata} <- Client.metadata(config),
         {:ok, commits_by_group} <- fetch_all_committed(config, described) do
      build_groups(config, metadata, described, commits_by_group, scope)
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
        topics = commits |> topics_committed() |> Enum.uniq()
        build_group(config, described, commits, partitions_by_topic, earliest, latest, topics)
      end
    end
  end

  # `describe_groups/3` is expected to answer with exactly one description
  # per requested id. If the group vanished between `list_groups/1` and this
  # call, some brokers answer with an empty list rather than a "Dead" entry;
  # either way that must not fall through the `with` in `get_group/2` as a
  # bare `{:ok, []}` (which does not have the shape `GroupLive.Show` expects
  # and crashes it with a `KeyError`). A broker answering with two or more
  # entries for one requested id is just as unexpected and must not raise a
  # `FunctionClauseError` in the LiveView process either. Exposed
  # (`@doc false`) so this exact translation is unit-testable without
  # needing to force the race live.
  @doc false
  @spec require_described(Config.t(), String.t(), [map()]) ::
          {:ok, map()} | {:error, BrokerError.t()}
  def require_described(_config, _group_id, [described]), do: {:ok, described}
  def require_described(config, group_id, []), do: {:error, unknown_group_error(config, group_id)}

  def require_described(config, group_id, described) when is_list(described) do
    {:error, unexpected_describe_response_error(config, group_id, described)}
  end

  defp unexpected_describe_response_error(config, group_id, described) do
    %BrokerError{
      address: Config.address(config),
      reason: :unexpected_response,
      message:
        "The broker returned #{length(described)} descriptions for consumer group " <>
          "#{group_id}, expected exactly one."
    }
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

  defp build_groups(config, metadata, described, commits_by_group, scope) do
    partitions_by_topic = partitions_by_topic(metadata)

    described =
      Enum.filter(described, &group_in_scope?(&1, Map.get(commits_by_group, &1.id, %{}), scope))

    pairs =
      described
      |> Enum.flat_map(fn group ->
        commits = Map.get(commits_by_group, group.id, %{})

        group
        |> scoped_topics(commits, scope)
        |> Enum.flat_map(fn topic ->
          Enum.map(Map.get(partitions_by_topic, topic, []), &{topic, &1})
        end)
      end)
      |> Enum.uniq()

    with {:ok, earliest} <- Client.list_offsets(config, pairs, :earliest),
         {:ok, latest} <- Client.list_offsets(config, pairs, :latest) do
      reduce_ok(described, fn group ->
        commits = Map.get(commits_by_group, group.id, %{})
        topics = scoped_topics(group, commits, scope)
        build_group(config, group, commits, partitions_by_topic, earliest, latest, topics)
      end)
    end
  end

  # A group is in `:all` scope unconditionally. In `{:topic, name}` scope it
  # is kept only if it has committed an offset on that topic or currently
  # has a member assigned to it.
  defp group_in_scope?(_group, _commits, :all), do: true

  defp group_in_scope?(group, commits, {:topic, topic}) do
    topic in topics_committed(commits) or topic in Map.get(group, :assigned_topics, [])
  end

  # The topics a group's `partitions`/`total_lag` are computed over: every
  # topic it committed to in `:all` scope, or only the scoped topic in
  # `{:topic, name}` scope.
  defp scoped_topics(_group, commits, :all), do: commits |> topics_committed() |> Enum.uniq()
  defp scoped_topics(_group, _commits, {:topic, topic}), do: [topic]

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
  defp build_group(config, group, commits, partitions_by_topic, earliest, latest, topics) do
    pairs =
      for topic <- topics,
          partition <- Map.get(partitions_by_topic, topic, []),
          do: {topic, partition}

    pairs
    |> reduce_ok(&partition_entry(config, commits, earliest, latest, &1))
    |> case do
      {:ok, entries} -> {:ok, group_from_partitions(group, entries)}
      {:error, _} = error -> error
    end
  end

  # Exposed (`@doc false`) so the "missing key becomes an error, never a
  # raise" translation is unit-testable at the actual call site of
  # `fetch_offset/3`, with a fabricated offsets map, not just on the leaf
  # helper itself.
  @doc false
  @spec partition_entry(Config.t(), map(), map(), map(), {String.t(), non_neg_integer()}) ::
          {:ok, map()} | {:error, BrokerError.t()}
  def partition_entry(config, commits, earliest, latest, {topic, partition} = key) do
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
