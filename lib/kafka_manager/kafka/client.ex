defmodule KafkaManager.Kafka.Client do
  @moduledoc """
  All broker I/O for the app: `:brod`'s high-level API plus raw `:kpro`
  requests for what `:brod` does not expose. This is the **only** module in
  the repo permitted to name `:brod`, `:kpro`, `:kpro_req_lib` or a
  `brod.hrl`/`kpro.hrl` record.

  There is no long-lived client process: every function here opens the
  connections it needs, uses them, and closes them, inside a supervised,
  unlinked task (`run/2`) so a LiveView process never touches `:brod`/`:kpro`
  directly and a hung or crashed connection cannot take the LiveView down
  with it.
  """

  require Record

  alias KafkaManager.Kafka.{BrokerError, Config, Message}

  Record.defrecordp(
    :kpro_rsp,
    Record.extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl")
  )

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  )

  Record.defrecordp(
    :brod_cg,
    Record.extract(:brod_cg, from_lib: "brod/include/brod.hrl")
  )

  @task_supervisor KafkaManager.Kafka.TaskSupervisor
  @deadline_slack_ms 1_000
  @list_offsets_vsn 1
  @describe_configs_vsn 1
  @config_resource_type_topic 2
  @produce_vsn 7
  @describe_log_dirs_vsn 1

  @doc """
  Runs `fun` in a supervised, unlinked task and waits for it, converting a
  timeout or a crash into a `%BrokerError{}`. `fun` must return
  `{:ok, term()} | {:error, %BrokerError{}}`.
  """
  @spec run(Config.t(), (-> {:ok, term()} | {:error, BrokerError.t()})) ::
          {:ok, term()} | {:error, BrokerError.t()}
  def run(%Config{} = config, fun) when is_function(fun, 0) do
    deadline = config.connect_timeout + config.request_timeout + @deadline_slack_ms
    task = Task.Supervisor.async_nolink(@task_supervisor, fun)

    case Task.yield(task, deadline) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, broker_error(config, reason)}
      nil -> {:error, broker_error(config, :timeout)}
    end
  end

  @doc """
  Broker metadata (brokers, topics, partitions, leaders, replicas) for every
  topic on the cluster.
  """
  @spec metadata(Config.t()) :: {:ok, map()} | {:error, BrokerError.t()}
  def metadata(%Config{} = config) do
    run(config, fn -> fetch_metadata(config, :all) end)
  end

  @doc """
  Earliest, latest, or (2026-09-11, AC-20) the offset of the first message
  at or after a given UTC millisecond timestamp, for the given
  `{topic, partition}` pairs, batched into one `ListOffsets` request per
  partition leader (never one connection per partition).

  For `{:timestamp, ms}`, a partition with no message at or after `ms`
  comes back as `nil` in the map, never as the broker's raw `-1` (1.4).
  `:earliest` and `:latest` are unaffected.
  """
  @spec list_offsets(
          Config.t(),
          [{String.t(), non_neg_integer()}],
          :earliest | :latest | {:timestamp, integer()}
        ) ::
          {:ok, %{{String.t(), non_neg_integer()} => integer() | nil}} | {:error, BrokerError.t()}
  def list_offsets(%Config{}, [], _which), do: {:ok, %{}}

  def list_offsets(%Config{} = config, partitions, which)
      when is_list(partitions) and which in [:earliest, :latest] do
    run(config, fn -> fetch_list_offsets(config, partitions, which) end)
  end

  def list_offsets(%Config{} = config, partitions, {:timestamp, ms})
      when is_list(partitions) and is_integer(ms) do
    # A negative millisecond timestamp (a `from`/`to` before the Unix epoch)
    # must never reach the wire as-is: `-1` and `-2` are the ListOffsets
    # sentinels for "latest" and "earliest" (1.4), so an unclamped `-1` here
    # would silently be read as "latest" instead of "the start of time".
    which = {:timestamp, max(ms, 0)}
    run(config, fn -> fetch_list_offsets(config, partitions, which) end)
  end

  @doc """
  Fetches one message set starting at `offset`, capped at `max_bytes`. On
  success returns the converted `%Message{}` list plus the high watermark
  offset. A `max_bytes` too small for the available messages returns an
  `:ok` result with an empty message list rather than an error; `Messages`
  is responsible for retrying with a larger `max_bytes`.
  """
  @spec fetch(Config.t(), String.t(), non_neg_integer(), integer(), pos_integer()) ::
          {:ok, %{messages: [Message.t()], high_watermark: integer()}}
          | {:error, BrokerError.t()}
  def fetch(%Config{} = config, topic, partition, offset, max_bytes)
      when is_binary(topic) and is_integer(partition) and is_integer(offset) and
             is_integer(max_bytes) do
    run(config, fn -> do_fetch(config, topic, partition, offset, max_bytes) end)
  end

  defp do_fetch(config, topic, partition, offset, max_bytes) do
    opts = %{max_wait_time: config.request_timeout, min_bytes: 0, max_bytes: max_bytes}

    case :brod.fetch({endpoints(config), conn_config(config)}, topic, partition, offset, opts) do
      {:ok, {high_watermark, messages}} ->
        {:ok,
         %{
           messages: Enum.map(messages, &to_message(&1, partition)),
           high_watermark: high_watermark
         }}

      {:error, reason} ->
        {:error, broker_error(config, reason)}
    end
  end

  # `partition` (2026-09-11) is the partition this fetch asked for, not
  # something the message record itself carries; the merged Data view needs
  # it on every `%Message{}` for ordering, row identity and the "partition 7"
  # label (docs/PLAN.md 1.2).
  defp to_message(record, partition) do
    %Message{
      offset: kafka_message(record, :offset),
      key: normalize_key(kafka_message(record, :key)),
      value: kafka_message(record, :value),
      timestamp: DateTime.from_unix!(kafka_message(record, :ts), :millisecond),
      headers: kafka_message(record, :headers),
      partition: partition
    }
  end

  defp normalize_key(<<>>), do: nil
  defp normalize_key(:undefined), do: nil
  defp normalize_key(:null), do: nil
  defp normalize_key(key), do: key

  @doc """
  Produces one message to a chosen topic/partition, with an optional key,
  headers and timestamp. Not available in `:brod`'s high-level API without a
  started client: goes through `:kpro.connect_partition_leader/4` plus
  `:kpro_req_lib.produce/4` plus `:kpro.request_sync/3`.
  """
  @spec produce(Config.t(), String.t(), non_neg_integer(), map()) ::
          {:ok, %{partition: non_neg_integer(), offset: integer()}}
          | {:error, BrokerError.t()}
  def produce(%Config{} = config, topic, partition, attrs)
      when is_binary(topic) and is_integer(partition) and is_map(attrs) do
    run(config, fn -> do_produce(config, topic, partition, attrs) end)
  end

  defp do_produce(config, topic, partition, attrs) do
    case :kpro.connect_partition_leader(endpoints(config), conn_config(config), topic, partition) do
      {:ok, connection} ->
        try do
          request = produce_request(topic, partition, attrs)

          case :kpro.request_sync(connection, request, config.request_timeout) do
            {:ok, response} ->
              parse_produce_response(config, kpro_rsp(response, :msg))

            {:error, reason} ->
              {:error, broker_error(config, reason)}
          end
        after
          :kpro.close_connection(connection)
        end

      {:error, reason} ->
        {:error, broker_error(config, reason)}
    end
  end

  defp produce_request(topic, partition, attrs) do
    message = produce_message_input(attrs)
    :kpro_req_lib.produce(@produce_vsn, topic, partition, [message])
  end

  defp produce_message_input(attrs) do
    base = %{key: denormalize_key(attrs[:key]), value: Map.fetch!(attrs, :value)}

    base
    |> maybe_put(:headers, attrs[:headers])
    |> maybe_put(:ts, produce_timestamp(attrs[:timestamp]))
  end

  defp denormalize_key(nil), do: <<>>
  defp denormalize_key(key), do: key

  defp produce_timestamp(nil), do: nil
  defp produce_timestamp(%DateTime{} = timestamp), do: DateTime.to_unix(timestamp, :millisecond)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp parse_produce_response(config, %{
         responses: [%{partition_responses: [partition_response | _]} | _]
       }) do
    case partition_response do
      %{error_code: :no_error, partition: partition, base_offset: offset} ->
        {:ok, %{partition: partition, offset: offset}}

      %{error_code: error_code} ->
        {:error, broker_error(config, error_code)}
    end
  end

  @doc """
  The topic-level broker configuration (name/value pairs), via a raw
  `DescribeConfigs` request. Not available in `:brod`'s high-level API.
  """
  @spec describe_topic_config(Config.t(), String.t()) ::
          {:ok, [%{name: String.t(), value: String.t() | nil, source: String.t()}]}
          | {:error, BrokerError.t()}
  def describe_topic_config(%Config{} = config, topic) when is_binary(topic) do
    run(config, fn -> fetch_topic_config(config, topic) end)
  end

  defp fetch_topic_config(config, topic) do
    case :kpro.connect_any(endpoints(config), conn_config(config)) do
      {:ok, connection} ->
        try do
          request = describe_configs_request(topic)

          case :kpro.request_sync(connection, request, config.request_timeout) do
            {:ok, response} ->
              case parse_describe_configs_response(kpro_rsp(response, :msg)) do
                {:ok, entries} -> {:ok, entries}
                {:error, reason} -> {:error, broker_error(config, reason)}
              end

            {:error, reason} ->
              {:error, broker_error(config, reason)}
          end
        after
          :kpro.close_connection(connection)
        end

      {:error, reason} ->
        {:error, broker_error(config, reason)}
    end
  end

  defp describe_configs_request(topic) do
    resources = [
      %{
        resource_type: @config_resource_type_topic,
        resource_name: topic,
        config_names: :undefined
      }
    ]

    fields = [{:resources, resources}, {:include_synonyms, false}]

    :kpro.make_request(:describe_configs, @describe_configs_vsn, fields)
  end

  defp parse_describe_configs_response(%{resources: [%{error_code: :no_error} = resource]}) do
    entries = resource.config_entries

    {:ok,
     Enum.map(entries, fn %{config_name: name, config_value: value, config_source: source} ->
       %{name: name, value: value, source: to_string(source)}
     end)}
  end

  defp parse_describe_configs_response(%{resources: [%{error_code: :unknown_topic_or_partition}]}) do
    {:error, :unknown_topic}
  end

  defp parse_describe_configs_response(%{resources: [%{error_code: error_code}]}) do
    {:error, error_code}
  end

  @doc """
  Log directory usage for every partition replica of a topic, via a raw
  `DescribeLogDirs` request. Not available in `:brod`'s high-level API.
  Each broker only reports its own disks, so this sends one request per
  broker that hosts a replica of the topic (from metadata's `replica_nodes`,
  never per partition). The request/response version is negotiated per
  connection with `:kpro.get_api_vsn_range/2`, capped at 1: `kpro_schema`
  only knows how to encode/decode versions 0 and 1. Future replicas
  (`is_future`) are dropped; the result is sorted by partition, then broker.
  """
  @spec describe_log_dirs(Config.t(), String.t()) ::
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
  def describe_log_dirs(%Config{} = config, topic) when is_binary(topic) do
    run(config, fn -> fetch_log_dirs(config, topic) end)
  end

  defp fetch_log_dirs(config, topic) do
    with {:ok, metadata} <- fetch_metadata(config, [topic]),
         {:ok, partitions} <- log_dir_topic_partitions(config, metadata, topic) do
      brokers = broker_map(metadata)
      replica_groups = replica_broker_groups(partitions)

      case log_dirs_from_brokers(config, brokers, replica_groups, topic) do
        {:ok, rows} -> {:ok, Enum.sort_by(rows, &{&1.partition, &1.broker})}
        {:error, _} = error -> error
      end
    end
  end

  defp log_dirs_from_brokers(config, brokers, replica_groups, topic) do
    Enum.reduce_while(replica_groups, {:ok, []}, fn {broker_id, partition_ids}, {:ok, acc} ->
      case log_dirs_from_broker(config, brokers, broker_id, topic, partition_ids) do
        {:ok, rows} -> {:cont, {:ok, acc ++ rows}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp log_dir_topic_partitions(config, %{topics: topics}, topic) do
    case Enum.find(topics, &(&1.name == topic)) do
      nil -> {:error, unknown_topic_error(config, topic)}
      %{partitions: partitions} -> {:ok, partitions}
    end
  end

  defp unknown_topic_error(config, topic) do
    %BrokerError{
      address: Config.address(config),
      reason: :unknown_topic,
      message: "Topic #{topic} does not exist on this cluster."
    }
  end

  defp replica_broker_groups(partitions) do
    for %{partition_index: id, replica_nodes: replicas} <- partitions,
        broker_id <- replicas,
        reduce: %{} do
      acc -> Map.update(acc, broker_id, [id], &[id | &1])
    end
  end

  # A replica can name a broker id that metadata's own `brokers` list does not
  # carry (e.g. it went offline between the partition metadata and this
  # lookup). `Map.fetch!/2` would raise in that case; this is a readable
  # `%BrokerError{}` instead. Exposed (`@doc false`) so the translation is
  # unit-testable with a fabricated `brokers` map: the live single-broker
  # Redpanda in dev can never produce a replica whose broker is missing from
  # its own metadata.
  @doc false
  @spec log_dirs_from_broker(Config.t(), map(), integer(), String.t(), [non_neg_integer()]) ::
          {:ok, list()} | {:error, BrokerError.t()}
  def log_dirs_from_broker(config, brokers, broker_id, topic, partition_ids) do
    case Map.fetch(brokers, broker_id) do
      {:ok, endpoint} ->
        fetch_log_dirs_from_broker(config, endpoint, broker_id, topic, partition_ids)

      :error ->
        {:error, missing_broker_error(config, broker_id)}
    end
  end

  defp missing_broker_error(config, broker_id) do
    %BrokerError{
      address: Config.address(config),
      reason: :missing_broker,
      message:
        "Broker #{broker_id} hosts a replica of this topic but is missing from the " <>
          "cluster metadata. It may have gone offline; try again."
    }
  end

  defp fetch_log_dirs_from_broker(config, endpoint, broker_id, topic, partition_ids) do
    case :kpro.connect(endpoint, conn_config(config)) do
      {:ok, connection} ->
        try do
          request = log_dirs_request(connection, topic, Enum.uniq(partition_ids))

          case :kpro.request_sync(connection, request, config.request_timeout) do
            {:ok, response} ->
              parse_log_dirs_response(config, kpro_rsp(response, :msg), broker_id, topic)

            {:error, reason} ->
              {:error, broker_error(config, reason)}
          end
        after
          :kpro.close_connection(connection)
        end

      {:error, reason} ->
        {:error, broker_error(config, reason)}
    end
  end

  defp log_dirs_request(connection, topic, partition_ids) do
    fields = [{:topics, [%{topic: topic, partitions: partition_ids}]}]
    :kpro.make_request(:describe_log_dirs, log_dirs_vsn(connection), fields)
  end

  defp log_dirs_vsn(connection) do
    case :kpro.get_api_vsn_range(connection, :describe_log_dirs) do
      {:ok, {_min_vsn, max_vsn}} -> min(max_vsn, @describe_log_dirs_vsn)
      {:error, _reason} -> @describe_log_dirs_vsn
    end
  end

  defp parse_log_dirs_response(config, %{log_dirs: log_dirs}, broker_id, topic) do
    Enum.reduce_while(log_dirs, {:ok, []}, fn log_dir, {:ok, acc} ->
      case log_dir do
        %{error_code: :no_error} = entry ->
          {:cont, {:ok, acc ++ log_dir_rows(entry, broker_id, topic)}}

        %{error_code: error_code} ->
          {:halt, {:error, broker_error(config, error_code)}}
      end
    end)
  end

  defp log_dir_rows(%{log_dir: log_dir, topics: topics}, broker_id, topic) do
    case Enum.find(topics, &(&1.topic == topic)) do
      nil ->
        []

      %{partitions: partitions} ->
        partitions
        |> Enum.reject(& &1.is_future)
        |> Enum.map(fn %{partition: partition, size: size, offset_lag: offset_lag} ->
          %{
            partition: partition,
            broker: broker_id,
            log_dir: log_dir,
            size_bytes: size,
            offset_lag: offset_lag
          }
        end)
    end
  end

  @doc """
  Lists every consumer group on the cluster, via `:brod.list_all_groups/2`.
  Each entry carries the coordinator endpoint that reported it (that broker
  is where the group is actually coordinated), which `describe_groups/3` and
  `fetch_committed_offsets/3` need next.
  """
  @spec list_groups(Config.t()) ::
          {:ok, [%{id: String.t(), protocol_type: String.t(), coordinator: term()}]}
          | {:error, BrokerError.t()}
  def list_groups(%Config{} = config) do
    run(config, fn -> fetch_list_groups(config) end)
  end

  defp fetch_list_groups(config) do
    config
    |> endpoints()
    |> :brod.list_all_groups(conn_config(config))
    |> collect_groups(config)
  end

  defp collect_groups(results, config) do
    Enum.reduce_while(results, {:ok, []}, fn {endpoint, groups_or_error}, {:ok, acc} ->
      case groups_or_error do
        {:error, reason} ->
          {:halt, {:error, broker_error(config, reason)}}

        groups when is_list(groups) ->
          {:cont, {:ok, acc ++ Enum.map(groups, &group_summary(&1, endpoint))}}
      end
    end)
  end

  defp group_summary(cg, endpoint) do
    %{
      id: brod_cg(cg, :id),
      protocol_type: brod_cg(cg, :protocol_type),
      coordinator: endpoint
    }
  end

  @doc """
  Describes the given group ids against the coordinator endpoint they were
  found on (the `coordinator` hint from `list_groups/1`), via
  `:brod.describe_groups/3`. `assigned_topics` is decoded from each live
  member's `member_assignment` bytes (brod's DescribeGroups only
  auto-decodes a field literally named `assignment`, and this one is named
  `member_assignment`) — used by AC-16 to keep a group with a live member on
  a topic it has not committed to yet.
  """
  @spec describe_groups(Config.t(), term(), [String.t()]) ::
          {:ok,
           [
             %{
               id: String.t(),
               state: String.t(),
               protocol_type: String.t(),
               member_count: non_neg_integer(),
               assigned_topics: [String.t()]
             }
           ]}
          | {:error, BrokerError.t()}
  def describe_groups(%Config{} = config, coordinator, group_ids) when is_list(group_ids) do
    run(config, fn -> fetch_describe_groups(config, coordinator, group_ids) end)
  end

  defp fetch_describe_groups(config, coordinator, group_ids) do
    case :brod.describe_groups(coordinator, conn_config(config), group_ids) do
      {:ok, groups} -> {:ok, Enum.map(groups, &described_group/1)}
      {:error, reason} -> {:error, broker_error(config, reason)}
    end
  end

  defp described_group(%{
         group_id: id,
         group_state: state,
         protocol_type: protocol_type,
         members: members
       }) do
    %{
      id: id,
      state: state,
      protocol_type: protocol_type,
      member_count: length(members),
      assigned_topics: assigned_topics(protocol_type, members)
    }
  end

  defp assigned_topics("consumer", members) do
    members |> Enum.flat_map(&member_assigned_topics/1) |> Enum.uniq()
  end

  defp assigned_topics(_protocol_type, _members), do: []

  defp member_assigned_topics(%{member_assignment: bytes}) do
    case decode_member_assignment(bytes) do
      {:ok, topics} -> topics
      :error -> []
    end
  end

  # Bytes that fail to decode (an empty assignment, a member mid-rebalance
  # with no assignment yet, or a protocol shape this app does not model)
  # mean "no assignment", never a raise.
  defp decode_member_assignment(bytes) when is_binary(bytes) and byte_size(bytes) > 0 do
    schema = :kpro_lib.get_prelude_schema(:cg_memeber_assignment, 0)

    try do
      {%{topic_partitions: topic_partitions}, _rest} =
        :kpro_rsp_lib.dec_struct(schema, %{}, [], bytes)

      {:ok, Enum.map(topic_partitions, & &1.topic)}
    rescue
      _ -> :error
    catch
      _, _ -> :error
    end
  end

  defp decode_member_assignment(_bytes), do: :error

  @doc """
  Committed offsets for every partition a group has committed against, via
  `:brod.fetch_committed_offsets/3`. A partition the group never committed
  to is simply absent from the returned map — the caller decides how to
  treat that (AC-11/AC-12 use the partition's earliest offset).
  """
  @spec fetch_committed_offsets(Config.t(), String.t()) ::
          {:ok, %{{String.t(), non_neg_integer()} => integer()}} | {:error, BrokerError.t()}
  def fetch_committed_offsets(%Config{} = config, group_id) when is_binary(group_id) do
    run(config, fn -> do_fetch_committed_offsets(config, group_id) end)
  end

  defp do_fetch_committed_offsets(config, group_id) do
    case :brod.fetch_committed_offsets(endpoints(config), conn_config(config), group_id) do
      {:ok, topics} -> {:ok, committed_offsets_map(topics)}
      {:error, reason} -> {:error, broker_error(config, reason)}
    end
  end

  defp committed_offsets_map(topics) do
    for %{name: topic, partitions: partitions} <- topics,
        %{partition_index: partition, error_code: :no_error, committed_offset: offset} <-
          partitions,
        offset >= 0,
        into: %{} do
      {{topic, partition}, offset}
    end
  end

  defp fetch_metadata(config, topics) do
    case :brod.get_metadata(endpoints(config), topics, conn_config(config)) do
      {:ok, metadata} -> {:ok, metadata}
      {:error, reason} -> {:error, broker_error(config, reason)}
    end
  end

  defp fetch_list_offsets(config, partitions, which) do
    topics = partitions |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

    with {:ok, metadata} <- fetch_metadata(config, topics) do
      leaders = leader_map(metadata)
      brokers = broker_map(metadata)

      with {:ok, grouped} <- group_partitions_by_leader(config, leaders, partitions),
           {:ok, offsets} <- list_offsets_from_leaders(config, brokers, grouped, which) do
        {:ok, normalize_timestamp_offsets(offsets, which)}
      end
    end
  end

  # A partition missing from `leaders` (e.g. it moved, or was dropped,
  # between `metadata/1` and this call) would raise via `Map.fetch!/2`; this
  # is a readable `%BrokerError{}` instead, the same crash class the
  # log-dirs fix removed (fix run item 6). Exposed (`@doc false`) so the
  # translation is unit-testable with a fabricated `leaders` map: the live
  # single-broker Redpanda in dev can never produce a partition missing from
  # its own metadata's leader list.
  @doc false
  @spec group_partitions_by_leader(Config.t(), map(), [{String.t(), non_neg_integer()}]) ::
          {:ok, map()} | {:error, BrokerError.t()}
  def group_partitions_by_leader(config, leaders, partitions) do
    Enum.reduce_while(partitions, {:ok, %{}}, fn partition, {:ok, acc} ->
      case Map.fetch(leaders, partition) do
        {:ok, leader_id} ->
          {:cont, {:ok, Map.update(acc, leader_id, [partition], &[partition | &1])}}

        :error ->
          {:halt, {:error, missing_partition_leader_error(config, partition)}}
      end
    end)
  end

  defp missing_partition_leader_error(config, {topic, partition}) do
    %BrokerError{
      address: Config.address(config),
      reason: :missing_partition_leader,
      message:
        "No leader was returned for #{topic}/#{partition}. It may have changed " <>
          "since the topic's metadata was fetched."
    }
  end

  # A `-1` offset (1.4, "no message at or after this timestamp") only means
  # something for a `{:timestamp, ms}` lookup. `:earliest`/`:latest` offsets
  # are left exactly as the broker returned them.
  defp normalize_timestamp_offsets(offsets, {:timestamp, _ms}) do
    Map.new(offsets, fn {key, offset} -> {key, if(offset == -1, do: nil, else: offset)} end)
  end

  defp normalize_timestamp_offsets(offsets, _which), do: offsets

  defp list_offsets_from_leaders(config, brokers, grouped_by_leader, which) do
    Enum.reduce_while(grouped_by_leader, {:ok, %{}}, fn {leader_id, leader_partitions},
                                                        {:ok, acc} ->
      case list_offsets_from_leader(config, brokers, leader_id, leader_partitions, which) do
        {:ok, offsets} -> {:cont, {:ok, Map.merge(acc, offsets)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  # A leader id missing from `brokers` (e.g. it went offline between the
  # partition metadata and this request) would raise via `Map.fetch!/2`;
  # this is a readable `%BrokerError{}` instead, the same crash class the
  # log-dirs fix removed (fix run item 6). Exposed (`@doc false`) so the
  # translation is unit-testable with a fabricated `brokers` map.
  @doc false
  @spec list_offsets_from_leader(
          Config.t(),
          map(),
          integer(),
          [{String.t(), non_neg_integer()}],
          :earliest | :latest | {:timestamp, integer()}
        ) :: {:ok, map()} | {:error, BrokerError.t()}
  def list_offsets_from_leader(config, brokers, leader_id, partitions, which) do
    case Map.fetch(brokers, leader_id) do
      {:ok, endpoint} ->
        fetch_list_offsets_from_leader(config, endpoint, partitions, which)

      :error ->
        {:error, missing_leader_broker_error(config, leader_id)}
    end
  end

  defp missing_leader_broker_error(config, leader_id) do
    %BrokerError{
      address: Config.address(config),
      reason: :missing_broker,
      message:
        "Broker #{leader_id} is the leader for a requested partition but is missing from " <>
          "the cluster metadata. It may have gone offline; try again."
    }
  end

  defp fetch_list_offsets_from_leader(config, endpoint, partitions, which) do
    case :kpro.connect(endpoint, conn_config(config)) do
      {:ok, connection} ->
        try do
          request = list_offsets_request(partitions, which)

          case :kpro.request_sync(connection, request, config.request_timeout) do
            {:ok, response} -> {:ok, parse_list_offsets_response(kpro_rsp(response, :msg))}
            {:error, reason} -> {:error, broker_error(config, reason)}
          end
        after
          :kpro.close_connection(connection)
        end

      {:error, reason} ->
        {:error, broker_error(config, reason)}
    end
  end

  defp list_offsets_request(partitions, which) do
    topics =
      partitions
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.map(fn {topic, partition_ids} ->
        %{
          topic: topic,
          partitions:
            Enum.map(partition_ids, fn partition_id ->
              %{partition: partition_id, timestamp: offset_time(which), current_leader_epoch: -1}
            end)
        }
      end)

    fields = [
      {:replica_id, -1},
      {:isolation_level, :read_committed},
      {:topics, topics}
    ]

    :kpro.make_request(:list_offsets, @list_offsets_vsn, fields)
  end

  defp offset_time(:latest), do: -1
  defp offset_time(:earliest), do: -2
  defp offset_time({:timestamp, ms}), do: ms

  # A partition-level `error_code` (e.g. the partition moved leaders between
  # `metadata/1` and this request) must never surface as an `offset: -1` for
  # arithmetic to run on. Such a partition is simply absent from the
  # returned map; callers (`Topics`, `Messages`, `Groups`) turn a missing
  # key into a `%BrokerError{}` instead of computing on it.
  @doc false
  @spec parse_list_offsets_response(map()) :: %{{String.t(), non_neg_integer()} => integer()}
  def parse_list_offsets_response(%{responses: responses}) do
    for %{topic: topic, partition_responses: partition_responses} <- responses,
        %{partition: partition, error_code: :no_error, offset: offset} <- partition_responses,
        into: %{} do
      {{topic, partition}, offset}
    end
  end

  defp leader_map(%{topics: topics}) do
    for %{name: name, partitions: partitions} <- topics,
        %{partition_index: index, leader_id: leader_id} <- partitions,
        into: %{} do
      {{name, index}, leader_id}
    end
  end

  defp broker_map(%{brokers: brokers}) do
    for %{node_id: node_id, host: host, port: port} <- brokers, into: %{} do
      {node_id, {host, port}}
    end
  end

  defp endpoints(%Config{brokers: brokers}), do: brokers

  defp conn_config(config), do: Config.conn_config(config)

  defp broker_error(config, reason) do
    %BrokerError{
      address: Config.address(config),
      reason: reason,
      message: broker_error_message(config, reason)
    }
  end

  defp broker_error_message(config, :timeout) do
    "Timed out waiting for a response from #{Config.address(config)}. " <>
      "Check that the broker is reachable and KAFKA_BROKERS is correct."
  end

  defp broker_error_message(config, reason) do
    "Could not complete the request against #{Config.address(config)}: #{inspect(reason)}. " <>
      "Check that the broker is reachable and KAFKA_BROKERS is correct."
  end
end
