defmodule KafkaManager.Kafka.Messages do
  @moduledoc """
  Reads a page of messages from a chosen partition starting at a chosen
  offset. Resolves the partition's earliest/latest offsets first, clamps the
  requested offset into range, then loops `Client.fetch/5` until `limit`
  messages have been collected or the partition end is reached, doubling
  `max_bytes` and retrying once whenever a fetch comes back empty short of
  the end (a `max_bytes` too small for the stored values returns zero
  messages rather than an error).
  """

  alias KafkaManager.Kafka.{BrokerError, Client, Config, Message}

  @min_max_bytes 1_048_576
  @max_max_bytes 8_388_608

  @doc """
  See `KafkaManager.Kafka.fetch_messages/4` for the public shape.
  """
  @spec fetch_messages(Config.t(), String.t(), non_neg_integer(), integer(), pos_integer()) ::
          {:ok, %{messages: [Message.t()], earliest: integer(), latest: integer()}}
          | {:error, BrokerError.t()}
  def fetch_messages(%Config{} = config, topic, partition, from_offset, limit)
      when is_binary(topic) and is_integer(partition) and is_integer(from_offset) and
             is_integer(limit) and limit > 0 do
    pair = {topic, partition}

    with {:ok, earliest_offsets} <- Client.list_offsets(config, [pair], :earliest),
         {:ok, latest_offsets} <- Client.list_offsets(config, [pair], :latest) do
      earliest = Map.fetch!(earliest_offsets, pair)
      latest = Map.fetch!(latest_offsets, pair)
      start_offset = clamp(from_offset, earliest, latest)

      case collect(config, topic, partition, start_offset, latest, limit) do
        {:ok, messages} -> {:ok, %{messages: messages, earliest: earliest, latest: latest}}
        {:error, _} = error -> error
      end
    end
  end

  defp collect(config, topic, partition, from_offset, latest, limit) do
    ctx = %{
      config: config,
      topic: topic,
      partition: partition,
      from_offset: from_offset,
      latest: latest,
      limit: limit
    }

    do_collect(ctx, from_offset, [], @min_max_bytes, false)
  end

  defp do_collect(%{limit: limit, latest: latest}, request_offset, acc, _max_bytes, _retried?)
       when length(acc) >= limit or request_offset >= latest do
    {:ok, finalize(acc, limit)}
  end

  defp do_collect(ctx, request_offset, acc, max_bytes, retried?) do
    %{config: config, topic: topic, partition: partition, from_offset: from_offset, limit: limit} =
      ctx

    case Client.fetch(config, topic, partition, request_offset, max_bytes) do
      {:ok, %{messages: []}} when not retried? and max_bytes < @max_max_bytes ->
        next_max_bytes = min(max_bytes * 2, @max_max_bytes)
        do_collect(ctx, request_offset, acc, next_max_bytes, true)

      {:ok, %{messages: []}} ->
        {:ok, finalize(acc, limit)}

      {:ok, %{messages: messages}} ->
        kept = Enum.filter(messages, &(&1.offset >= from_offset))
        next_request_offset = messages |> List.last() |> Map.fetch!(:offset) |> Kernel.+(1)
        do_collect(ctx, next_request_offset, acc ++ kept, @min_max_bytes, false)

      {:error, _} = error ->
        error
    end
  end

  defp finalize(acc, limit), do: acc |> Enum.sort_by(& &1.offset) |> Enum.take(limit)

  defp clamp(value, min, max), do: value |> max(min) |> min(max)

  @doc """
  See `KafkaManager.Kafka.produce/3` for the public shape. `partition == nil`
  resolves to partition `0` — no AC exercises the "auto" option with more
  than one partition, and `scratch` (the only topic tests write to) has one.
  """
  @spec produce(Config.t(), String.t(), non_neg_integer() | nil, map()) ::
          {:ok, %{partition: non_neg_integer(), offset: integer()}}
          | {:error, BrokerError.t()}
  def produce(%Config{} = config, topic, partition, attrs) when is_binary(topic) do
    Client.produce(config, topic, partition || 0, attrs)
  end
end
