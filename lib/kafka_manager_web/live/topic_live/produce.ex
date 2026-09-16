defmodule KafkaManagerWeb.TopicLive.Produce do
  @moduledoc """
  A form for producing one message to a chosen partition of a topic, targeting a
  specific partition (or letting the broker choose), a key (or an explicit null
  key), a value, headers and a timestamp. On success it renders a confirmation
  naming the partition and the assigned offset. The produce path itself is
  `Kafka.produce/3`; this module adds only the form.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @impl true
  def mount(_params, _session, socket) do
    socket =
      assign(socket,
        broker_error: nil,
        topic_name: nil,
        partition_count: nil,
        last_produced: nil,
        form_error: nil,
        header_rows: [0],
        next_header_id: 1,
        form: to_form(%{}, as: :message)
      )

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"topic" => topic}, _uri, socket) do
    socket =
      socket
      |> assign(topic_name: topic)
      |> fetch_topic(topic)

    {:noreply, socket}
  end

  @impl true
  def handle_event("add_header", _params, socket) do
    id = socket.assigns.next_header_id

    {:noreply,
     assign(socket,
       header_rows: socket.assigns.header_rows ++ [id],
       next_header_id: id + 1
     )}
  end

  def handle_event("remove_header", %{"row" => row}, socket) do
    {:noreply, remove_header_row(socket, row)}
  end

  def handle_event("produce", %{"message" => params}, socket) do
    {:noreply, produce(socket, params)}
  end

  # A client-supplied row id (`phx-value-row`) that is not a valid integer
  # must not crash the page via `String.to_integer/1`: it simply cannot
  # match any row, the same as a row id that is a real integer but not
  # currently in `header_rows`.
  defp remove_header_row(socket, row) do
    case Integer.parse(row) do
      {id, ""} ->
        rows = socket.assigns.header_rows
        rows = if length(rows) > 1, do: List.delete(rows, id), else: rows
        assign(socket, header_rows: rows)

      _ ->
        socket
    end
  end

  defp fetch_topic(socket, topic) do
    case Kafka.get_topic(topic) do
      {:ok, %{partition_count: count}} ->
        assign(socket, broker_error: nil, partition_count: count)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error)
    end
  end

  defp produce(socket, params) do
    case parse_timestamp(params["timestamp"]) do
      {:ok, timestamp} ->
        attrs = %{
          key: produce_key(params),
          value: params["value"] || "",
          headers: parse_headers(params["headers"]),
          timestamp: timestamp
        }

        case Kafka.produce(socket.assigns.topic_name, parse_partition(params["partition"]), attrs) do
          {:ok, %{partition: partition, offset: offset}} ->
            assign(socket,
              broker_error: nil,
              form_error: nil,
              last_produced: %{partition: partition, offset: offset}
            )

          {:error, %BrokerError{} = error} ->
            assign(socket, broker_error: error)
        end

      :error ->
        assign(socket, form_error: "Timestamp must be ISO 8601, e.g. 2026-01-02T03:04:05Z.")
    end
  end

  defp produce_key(%{"null_key" => "true"}), do: nil
  defp produce_key(params), do: blank_to_nil(params["key"])

  defp parse_partition(value) when value in [nil, "", "auto"], do: nil

  defp parse_partition(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp parse_headers(nil), do: []

  defp parse_headers(rows) when is_map(rows) do
    rows
    |> Map.values()
    |> Enum.map(fn row -> {blank_to_nil(row["name"]), row["value"] || ""} end)
    |> Enum.reject(fn {name, _value} -> is_nil(name) end)
  end

  defp parse_timestamp(value) when value in [nil, ""], do: {:ok, nil}

  defp parse_timestamp(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, _offset} -> {:ok, timestamp}
      {:error, _reason} -> :error
    end
  end

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  @doc false
  def partition_options(nil), do: []
  def partition_options(count), do: 0..(count - 1)
end
