defmodule KafkaManagerWeb.GroupLive.Index do
  @moduledoc """
  AC-11: the consumer group list, showing every group's raw state, member
  count and total lag, with an inline expander (PLAN 4.1, P4) revealing
  per-partition lag.
  """

  use KafkaManagerWeb, :live_view

  alias KafkaManager.Kafka
  alias KafkaManager.Kafka.BrokerError

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(broker_error: nil, group_index: %{})
      |> stream_configure(:groups, dom_id: &("group-" <> slug(&1.id)))
      |> stream(:groups, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(_params, _uri, socket) do
    {:noreply, fetch_groups(socket)}
  end

  @impl true
  def handle_event("expand_group", %{"group" => id}, socket) do
    {:noreply, toggle_expanded(socket, id, true)}
  end

  def handle_event("collapse_group", %{"group" => id}, socket) do
    {:noreply, toggle_expanded(socket, id, false)}
  end

  defp fetch_groups(socket) do
    case Kafka.list_groups() do
      {:ok, groups} ->
        index = Map.new(groups, &{&1.id, &1})
        rows = Enum.map(groups, &to_row(&1, false))

        socket
        |> assign(broker_error: nil, group_index: index)
        |> stream(:groups, rows, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error)
    end
  end

  # P4 (PLAN 4.1): the toggle handler re-inserts only the affected row,
  # looked up from the bounded `:group_index` map built at fetch time.
  defp toggle_expanded(socket, id, expanded?) do
    case socket.assigns.group_index[id] do
      nil -> socket
      group -> stream_insert(socket, :groups, to_row(group, expanded?))
    end
  end

  defp to_row(group, expanded?) do
    group
    |> Map.from_struct()
    |> Map.put(:expanded?, expanded?)
  end

  @doc """
  The DECISIONS.md colour mapping for a group's raw Kafka state, as a CSS
  class: `Stable` green, `PreparingRebalance`/`CompletingRebalance` amber,
  `Empty` grey, `Dead` red.
  """
  def state_class("Stable"), do: "state-stable"
  def state_class("PreparingRebalance"), do: "state-preparing-rebalance"
  def state_class("CompletingRebalance"), do: "state-completing-rebalance"
  def state_class("Empty"), do: "state-empty"
  def state_class("Dead"), do: "state-dead"
  def state_class(_other), do: "state-unknown"

  @doc """
  The DECISIONS.md colour mapping for a group's raw Kafka state, as a
  daisyUI badge modifier class, for the coloured badge in the State cell
  (DESIGN.md).
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

  defp slug(id), do: String.replace(id, ~r/[^a-zA-Z0-9]+/, "-")
end
