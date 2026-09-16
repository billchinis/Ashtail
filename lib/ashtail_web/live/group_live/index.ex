defmodule AshtailWeb.GroupLive.Index do
  @moduledoc """
  The consumer group list, showing every group's raw state, member count and
  total lag, with an inline expander revealing per-partition lag. Paginated
  and sortable like the topic list.
  """

  use AshtailWeb, :live_view

  alias Ashtail.Kafka
  alias Ashtail.Kafka.{BrokerError, Groups}
  alias AshtailWeb.ListParams

  # {value, label} for the mobile sort menu, where there are no column headers.
  @sort_options [
    {"id:asc", "Group (A–Z)"},
    {"id:desc", "Group (Z–A)"},
    {"state:asc", "State (A–Z)"},
    {"state:desc", "State (Z–A)"},
    {"members:desc", "Most members"},
    {"members:asc", "Fewest members"},
    {"lag:desc", "Most lag"},
    {"lag:asc", "Least lag"}
  ]

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(
        broker_error: nil,
        group_index: %{},
        total: 0,
        page: 1,
        page_size: ListParams.default_page_size(),
        page_count: 1,
        sort: :id,
        dir: :asc,
        sort_options: @sort_options
      )
      |> assign_query()
      |> stream_configure(:groups, dom_id: &("group-" <> slug(&1.id)))
      |> stream(:groups, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {sort, dir} = ListParams.sort(params["sort"], params["dir"], Groups.sort_keys(), :id)

    socket =
      socket
      |> assign(
        page: ListParams.page(params["page"]),
        page_size: ListParams.page_size(params["page_size"]),
        sort: sort,
        dir: dir
      )
      |> fetch_groups()

    {:noreply, socket}
  end

  @impl true
  def handle_event("expand_group", %{"group" => id}, socket) do
    {:noreply, toggle_expanded(socket, id, true)}
  end

  def handle_event("collapse_group", %{"group" => id}, socket) do
    {:noreply, toggle_expanded(socket, id, false)}
  end

  def handle_event("page_size", %{"page_size" => page_size}, socket) do
    {:noreply, push_patch(socket, to: list_path(socket.assigns, page: 1, page_size: page_size))}
  end

  def handle_event("sort_select", %{"sort" => value}, socket) do
    {sort, dir} = ListParams.sort_option(value, Groups.sort_keys(), :id)
    {:noreply, push_patch(socket, to: list_path(socket.assigns, page: 1, sort: sort, dir: dir))}
  end

  @doc """
  The list URL for the state in `query` (`page`, `page_size`, `sort`, `dir`),
  with `changes` applied.
  """
  def list_path(query, changes \\ []) do
    q = query |> Map.take([:page, :page_size, :sort, :dir]) |> Map.merge(Map.new(changes))

    ~p"/groups?#{[page: q.page, page_size: q.page_size, sort: q.sort, dir: q.dir]}"
  end

  @doc "The URL a column header links to (see `AshtailWeb.ListParams.next_dir/4`)."
  def sort_path(query, column) do
    dir = ListParams.next_dir(column, query.sort, query.dir, [:id, :state])
    list_path(query, page: 1, sort: column, dir: dir)
  end

  defp fetch_groups(socket) do
    %{page: page, page_size: page_size, sort: sort, dir: dir} = socket.assigns

    case Kafka.list_groups(page: page, page_size: page_size, sort: sort, dir: dir) do
      {:ok, result} ->
        index = Map.new(result.groups, &{&1.id, &1})
        rows = Enum.map(result.groups, &to_row(&1, false))

        socket
        |> assign(
          broker_error: nil,
          group_index: index,
          total: result.total,
          page: result.page,
          page_count: result.page_count
        )
        |> assign_query()
        |> stream(:groups, rows, reset: true)

      {:error, %BrokerError{} = error} ->
        socket |> assign(broker_error: error) |> assign_query()
    end
  end

  # The URL state as one map, so the template can build links from `@query`.
  defp assign_query(socket) do
    assign(socket, :query, Map.take(socket.assigns, [:page, :page_size, :sort, :dir]))
  end

  # The toggle handler re-inserts only the affected row, looked up from the
  # bounded `:group_index` map built at fetch time (the current page only).
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
  The colour mapping for a group's raw Kafka state, as a CSS class: `Stable`
  green, `PreparingRebalance`/`CompletingRebalance` amber, `Empty` grey, `Dead`
  red.
  """
  def state_class("Stable"), do: "state-stable"
  def state_class("PreparingRebalance"), do: "state-preparing-rebalance"
  def state_class("CompletingRebalance"), do: "state-completing-rebalance"
  def state_class("Empty"), do: "state-empty"
  def state_class("Dead"), do: "state-dead"
  def state_class(_other), do: "state-unknown"

  defp slug(id), do: String.replace(id, ~r/[^a-zA-Z0-9]+/, "-")
end
