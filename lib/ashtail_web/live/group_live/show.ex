defmodule AshtailWeb.GroupLive.Show do
  @moduledoc """
  Consumer group detail, showing a partition row for every partition the group
  has committed against, with its committed offset, latest offset and lag.
  """

  use AshtailWeb, :live_view

  alias Ashtail.Kafka
  alias Ashtail.Kafka.BrokerError

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
end
