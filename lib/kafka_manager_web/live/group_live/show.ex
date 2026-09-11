defmodule KafkaManagerWeb.GroupLive.Show do
  @moduledoc """
  AC-12: consumer group detail, showing a partition row for every partition
  the group has committed against, with its committed offset, latest offset
  and lag.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, group: nil)
      |> stream_configure(:group_partitions,
        dom_id: &("gp-" <> slug(&1.topic) <> "-" <> to_string(&1.partition))
      )
      |> stream(:group_partitions, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"group" => group_id}, _uri, socket) do
    socket =
      socket
      |> assign(group_id: group_id)
      |> fetch_group(group_id)

    {:noreply, socket}
  end

  defp fetch_group(socket, group_id) do
    case Kafka.get_group(group_id) do
      {:ok, group} ->
        socket
        |> assign(broker_error: nil, group: group)
        |> stream(:group_partitions, group.partitions, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error)
    end
  end

  defp slug(name), do: String.replace(name, ~r/[^a-zA-Z0-9]+/, "-")

  @doc """
  The DECISIONS.md colour mapping for a group's raw Kafka state, as a
  daisyUI badge modifier class (same mapping as `GroupLive.Index`).
  """
  def badge_class("Stable"), do: "badge-success"
  def badge_class("PreparingRebalance"), do: "badge-warning"
  def badge_class("CompletingRebalance"), do: "badge-warning"
  def badge_class("Empty"), do: "badge-neutral"
  def badge_class("Dead"), do: "badge-error"
  def badge_class(_other), do: "badge-ghost"

  @doc """
  Lag colour rule (DESIGN.md): non-zero lag is a warning, zero lag is muted.
  """
  def lag_class(lag) when is_integer(lag) and lag > 0, do: "text-warning font-semibold"
  def lag_class(_lag), do: "text-base-content/50"
end
