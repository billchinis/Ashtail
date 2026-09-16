defmodule KafkaManagerWeb.TopicLive.Logs do
  @moduledoc """
  The Logs sub-menu, showing log directory usage for every partition replica via
  a raw DescribeLogDirs request. See `KafkaManager.Kafka.topic_log_dirs/1`.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @units ["B", "KB", "MB", "GB", "TB", "PB"]

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, topic: nil)
      |> stream_configure(:log_dirs, dom_id: &"log-#{&1.partition}-#{&1.broker}")
      |> stream(:log_dirs, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => name}, _uri, socket) do
    socket =
      socket
      |> assign(topic_name: name)
      |> fetch(name)

    {:noreply, socket}
  end

  # Two fetches: `broker_error` becomes the first of the two that fails, or
  # `nil` when both succeed. One success does not clear the other's error.
  defp fetch(socket, name) do
    topic_result = Kafka.topic_summary(name)
    log_dirs_result = Kafka.topic_log_dirs(name)

    socket = assign(socket, topic: ok_or_nil(topic_result))

    case {topic_result, log_dirs_result} do
      {{:error, %BrokerError{} = error}, _} ->
        assign(socket, broker_error: error)

      {{:ok, _topic}, {:error, %BrokerError{} = error}} ->
        assign(socket, broker_error: error)

      {{:ok, _topic}, {:ok, log_dirs}} ->
        socket
        |> assign(broker_error: nil)
        |> stream(:log_dirs, log_dirs, reset: true)
    end
  end

  defp ok_or_nil({:ok, value}), do: value
  defp ok_or_nil({:error, _}), do: nil

  # A human-readable size (base 1024, one decimal), for the line above the
  # exact "{n} bytes".
  defp human_size(bytes) when is_integer(bytes) and bytes < 1024, do: "#{bytes} B"

  defp human_size(bytes) when is_integer(bytes) do
    {value, unit} = scale(bytes / 1, 0)
    :erlang.float_to_binary(value, decimals: 1) <> " " <> unit
  end

  defp scale(value, index) when value >= 1024 and index < length(@units) - 1 do
    scale(value / 1024, index + 1)
  end

  defp scale(value, index), do: {value, Enum.at(@units, index)}
end
