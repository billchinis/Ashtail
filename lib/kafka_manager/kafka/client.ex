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

  alias KafkaManager.Kafka.{BrokerError, Config}

  Record.defrecordp(
    :kpro_rsp,
    Record.extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl")
  )

  @task_supervisor KafkaManager.Kafka.TaskSupervisor
  @deadline_slack_ms 1_000
  @list_offsets_vsn 1

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
  Earliest or latest offsets for the given `{topic, partition}` pairs,
  batched into one `ListOffsets` request per partition leader (never one
  connection per partition).
  """
  @spec list_offsets(Config.t(), [{String.t(), non_neg_integer()}], :earliest | :latest) ::
          {:ok, %{{String.t(), non_neg_integer()} => integer()}} | {:error, BrokerError.t()}
  def list_offsets(%Config{}, [], _which), do: {:ok, %{}}

  def list_offsets(%Config{} = config, partitions, which)
      when is_list(partitions) and which in [:earliest, :latest] do
    run(config, fn -> fetch_list_offsets(config, partitions, which) end)
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
      grouped = Enum.group_by(partitions, &Map.fetch!(leaders, &1))
      list_offsets_from_leaders(config, brokers, grouped, which)
    end
  end

  defp list_offsets_from_leaders(config, brokers, grouped_by_leader, which) do
    Enum.reduce_while(grouped_by_leader, {:ok, %{}}, fn {leader_id, leader_partitions},
                                                        {:ok, acc} ->
      case list_offsets_from_leader(config, brokers, leader_id, leader_partitions, which) do
        {:ok, offsets} -> {:cont, {:ok, Map.merge(acc, offsets)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp list_offsets_from_leader(config, brokers, leader_id, partitions, which) do
    endpoint = Map.fetch!(brokers, leader_id)

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

  defp parse_list_offsets_response(%{responses: responses}) do
    for %{topic: topic, partition_responses: partition_responses} <- responses,
        %{partition: partition, offset: offset} <- partition_responses,
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
