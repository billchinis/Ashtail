defmodule KafkaManagerWeb.TopicLive.Groups do
  @moduledoc """
  AC-16: the Consumer Groups sub-menu, listing every consumer group that
  reads this topic (committed an offset on it, or has a live member
  assigned to it) with its state and its lag on this topic only. See
  `KafkaManager.Kafka.topic_groups/1`.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, topic: nil)
      |> stream_configure(:topic_groups, dom_id: &("topic-group-" <> slug(&1.id)))
      |> stream(:topic_groups, [])

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

  # Two fetches per docs/PLAN.md P3: `broker_error` becomes the first of
  # the two that fails, or `nil` when both succeed. One success does not
  # clear the other's error.
  defp fetch(socket, name) do
    topic_result = Kafka.topic_summary(name)
    groups_result = Kafka.topic_groups(name)

    socket = assign(socket, topic: ok_or_nil(topic_result))

    case {topic_result, groups_result} do
      {{:error, %BrokerError{} = error}, _} ->
        assign(socket, broker_error: error)

      {{:ok, _topic}, {:error, %BrokerError{} = error}} ->
        assign(socket, broker_error: error)

      {{:ok, _topic}, {:ok, groups}} ->
        socket
        |> assign(broker_error: nil)
        |> stream(:topic_groups, groups, reset: true)
    end
  end

  defp ok_or_nil({:ok, value}), do: value
  defp ok_or_nil({:error, _}), do: nil

  defp slug(name), do: String.replace(name, ~r/[^a-zA-Z0-9]+/, "-")

  defp state_class("Stable"), do: "state-stable"
  defp state_class("PreparingRebalance"), do: "state-preparing-rebalance"
  defp state_class("CompletingRebalance"), do: "state-completing-rebalance"
  defp state_class("Empty"), do: "state-empty"
  defp state_class("Dead"), do: "state-dead"
  defp state_class(_other), do: "state-unknown"
end
