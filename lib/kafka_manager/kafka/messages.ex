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
  See `KafkaManager.Kafka.fetch_messages/4` for the public shape. Resolves
  `earliest`/`latest`, clamps `from_offset` into range, then reads
  `read_range/5` up to `min(latest, from_offset + limit)`.
  """
  @spec fetch_messages(Config.t(), String.t(), non_neg_integer(), integer(), pos_integer()) ::
          {:ok, %{messages: [Message.t()], earliest: integer(), latest: integer()}}
          | {:error, BrokerError.t()}
  def fetch_messages(%Config{} = config, topic, partition, from_offset, limit)
      when is_binary(topic) and is_integer(partition) and is_integer(from_offset) and
             is_integer(limit) and limit > 0 do
    pair = {topic, partition}

    with {:ok, earliest_offsets} <- Client.list_offsets(config, [pair], :earliest),
         {:ok, latest_offsets} <- Client.list_offsets(config, [pair], :latest),
         {:ok, {earliest, latest}} <-
           resolve_bounds(config, pair, earliest_offsets, latest_offsets) do
      start_offset = clamp(from_offset, earliest, latest)
      to = min(latest, start_offset + limit)

      case read_range(config, topic, partition, start_offset, to) do
        {:ok, messages} -> {:ok, %{messages: messages, earliest: earliest, latest: latest}}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Reads messages in the half-open offset range `[from, to)` for one partition,
  in ascending offset order. Drops anything below `from` and anything at or
  above `to`, because a fetch may return the start or the tail of a whole batch.
  Doubles `max_bytes` (starting at 1 MB, capped at 8 MB) and retries once
  whenever a fetch comes back empty short of `to`; if it is still empty after
  the retry, the range is treated as read to its end, which is correct for
  compaction gaps. This is the only read path `KafkaManager.Kafka.TopicReader`
  uses (it never calls `Client.fetch/5` directly).
  """
  @spec read_range(Config.t(), String.t(), non_neg_integer(), integer(), integer()) ::
          {:ok, [Message.t()]} | {:error, BrokerError.t()}
  def read_range(%Config{} = config, topic, partition, from, to)
      when is_binary(topic) and is_integer(partition) and is_integer(from) and is_integer(to) do
    if from >= to do
      {:ok, []}
    else
      collect_range(config, topic, partition, from, to)
    end
  end

  # The actual call site of `fetch_offset/3` inside `fetch_messages/5`.
  # Exposed (`@doc false`) so the "missing key becomes an error, never a
  # raise" translation is unit-testable with a fabricated offsets map at the
  # caller, not just on the leaf helper itself.
  @doc false
  @spec resolve_bounds(Config.t(), {String.t(), non_neg_integer()}, map(), map()) ::
          {:ok, {integer(), integer()}} | {:error, BrokerError.t()}
  def resolve_bounds(config, pair, earliest_offsets, latest_offsets) do
    with {:ok, earliest} <- fetch_offset(config, earliest_offsets, pair),
         {:ok, latest} <- fetch_offset(config, latest_offsets, pair) do
      {:ok, {earliest, latest}}
    end
  end

  # A `{topic, partition}` missing from a batched offsets map (dropped by
  # `Client.list_offsets/3` because that partition came back with a
  # partition-level error, e.g. the topic was deleted mid-request) must not
  # raise via `Map.fetch!/2` here in the calling process, outside `Client`'s
  # own crash containment. It becomes a `%BrokerError{}` instead. Exposed
  # (`@doc false`) so this translation is unit-testable without forcing a
  # live topic deletion mid-request against the broker.
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
          "since the partition's offsets were fetched."
    }
  end

  defp collect_range(config, topic, partition, from, to) do
    ctx = %{config: config, topic: topic, partition: partition, from: from, to: to}
    do_collect_range(ctx, from, [], @min_max_bytes, false)
  end

  defp do_collect_range(%{to: to}, request_offset, acc, _max_bytes, _retried?)
       when request_offset >= to do
    {:ok, finalize_range(acc)}
  end

  defp do_collect_range(ctx, request_offset, acc, max_bytes, retried?) do
    %{config: config, topic: topic, partition: partition, from: from, to: to} = ctx

    case Client.fetch(config, topic, partition, request_offset, max_bytes) do
      {:ok, %{messages: []}} when not retried? and max_bytes < @max_max_bytes ->
        next_max_bytes = min(max_bytes * 2, @max_max_bytes)
        do_collect_range(ctx, request_offset, acc, next_max_bytes, true)

      {:ok, %{messages: []}} ->
        {:ok, finalize_range(acc)}

      {:ok, %{messages: messages}} ->
        kept = Enum.filter(messages, &(&1.offset >= from and &1.offset < to))
        next_request_offset = messages |> List.last() |> Map.fetch!(:offset) |> Kernel.+(1)

        if next_request_offset >= to do
          {:ok, finalize_range(acc ++ kept)}
        else
          do_collect_range(ctx, next_request_offset, acc ++ kept, @min_max_bytes, false)
        end

      {:error, _} = error ->
        error
    end
  end

  defp finalize_range(acc), do: Enum.sort_by(acc, & &1.offset)

  defp clamp(value, min, max), do: value |> max(min) |> min(max)

  @doc """
  See `KafkaManager.Kafka.produce/3` for the public shape. `partition == nil`
  resolves to partition `0` — no test exercises the "auto" option with more
  than one partition, and `scratch` (the only topic tests write to) has one.
  """
  @spec produce(Config.t(), String.t(), non_neg_integer() | nil, map()) ::
          {:ok, %{partition: non_neg_integer(), offset: integer()}}
          | {:error, BrokerError.t()}
  def produce(%Config{} = config, topic, partition, attrs) when is_binary(topic) do
    Client.produce(config, topic, partition || 0, attrs)
  end
end
