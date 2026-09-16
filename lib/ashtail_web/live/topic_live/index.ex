defmodule AshtailWeb.TopicLive.Index do
  @moduledoc """
  The topic list, showing every topic's partition count, replication factor and
  message count for the current page, sortable by any of those columns.
  """

  use AshtailWeb, :live_view

  alias Ashtail.Kafka
  alias Ashtail.Kafka.{BrokerError, Topics}
  alias AshtailWeb.ListParams

  # {value, label} for the mobile sort menu, where there are no column headers.
  @sort_options [
    {"name:asc", "Name (A–Z)"},
    {"name:desc", "Name (Z–A)"},
    {"partitions:desc", "Most partitions"},
    {"partitions:asc", "Fewest partitions"},
    {"replication:desc", "Highest replication"},
    {"replication:asc", "Lowest replication"},
    {"messages:desc", "Most messages"},
    {"messages:asc", "Fewest messages"}
  ]

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(
        broker_error: nil,
        total: 0,
        page: 1,
        page_size: ListParams.default_page_size(),
        page_count: 1,
        search: "",
        sort: :name,
        dir: :asc,
        sort_options: @sort_options
      )
      |> assign_query()
      |> stream_configure(:topics, dom_id: &("topic-" <> slug(&1.name)))
      |> stream(:topics, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {sort, dir} = ListParams.sort(params["sort"], params["dir"], Topics.sort_keys(), :name)

    socket =
      socket
      |> assign(
        page: ListParams.page(params["page"]),
        page_size: ListParams.page_size(params["page_size"]),
        search: params["q"] || "",
        sort: sort,
        dir: dir
      )
      |> fetch_topics()

    {:noreply, socket}
  end

  @impl true
  def handle_event("page_size", %{"page_size" => page_size}, socket) do
    {:noreply, push_patch(socket, to: list_path(socket.assigns, page: 1, page_size: page_size))}
  end

  @impl true
  def handle_event("search", %{"q" => search}, socket) do
    {:noreply, push_patch(socket, to: list_path(socket.assigns, page: 1, search: search))}
  end

  @impl true
  def handle_event("sort_select", %{"sort" => value}, socket) do
    {sort, dir} = ListParams.sort_option(value, Topics.sort_keys(), :name)
    {:noreply, push_patch(socket, to: list_path(socket.assigns, page: 1, sort: sort, dir: dir))}
  end

  @doc """
  The list URL for the state in `query` (`page`, `page_size`, `search`,
  `sort`, `dir`), with `changes` applied.
  """
  def list_path(query, changes \\ []) do
    q =
      query |> Map.take([:page, :page_size, :search, :sort, :dir]) |> Map.merge(Map.new(changes))

    ~p"/?#{[page: q.page, page_size: q.page_size, q: q.search, sort: q.sort, dir: q.dir]}"
  end

  @doc "The URL a column header links to (see `AshtailWeb.ListParams.next_dir/4`)."
  def sort_path(query, column) do
    dir = ListParams.next_dir(column, query.sort, query.dir, [:name])
    list_path(query, page: 1, sort: column, dir: dir)
  end

  defp fetch_topics(socket) do
    %{page: page, page_size: page_size, search: search, sort: sort, dir: dir} = socket.assigns

    case Kafka.list_topics(
           page: page,
           page_size: page_size,
           search: search,
           sort: sort,
           dir: dir
         ) do
      {:ok, result} ->
        socket
        |> assign(
          broker_error: nil,
          total: result.total,
          page: result.page,
          page_count: result.page_count
        )
        |> assign_query()
        |> stream(:topics, result.topics, reset: true)

      {:error, %BrokerError{} = error} ->
        socket |> assign(broker_error: error) |> assign_query()
    end
  end

  # The URL state as one map, so the template can build links from `@query`
  # without passing the whole `assigns`.
  defp assign_query(socket) do
    assign(socket, :query, Map.take(socket.assigns, [:page, :page_size, :search, :sort, :dir]))
  end

  defp slug(name), do: String.replace(name, ~r/[^a-zA-Z0-9]+/, "-")
end
