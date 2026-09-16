defmodule AshtailWeb.TopicLive.Index do
  @moduledoc """
  The topic list, showing every topic's partition count, replication factor and
  message count for the current page, sortable by any of those columns.
  """

  use AshtailWeb, :live_view

  alias Ashtail.Kafka
  alias Ashtail.Kafka.{BrokerError, Topics}

  @default_page_size 20
  @sort_names Map.new(Topics.sort_keys(), &{Atom.to_string(&1), &1})

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
        page_size: @default_page_size,
        page_count: 1,
        search: "",
        sort: :name,
        dir: :asc,
        sort_options: @sort_options,
        query: %{page: 1, page_size: @default_page_size, search: "", sort: :name, dir: :asc}
      )
      |> stream_configure(:topics, dom_id: &("topic-" <> slug(&1.name)))
      |> stream(:topics, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {sort, dir} = parse_sort(params["sort"], params["dir"])

    socket =
      socket
      |> assign(
        page: parse_page(params["page"]),
        page_size: parse_page_size(params["page_size"]),
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
    {sort, dir} =
      case String.split(value, ":") do
        [sort, dir] -> parse_sort(sort, dir)
        _ -> parse_sort(nil, nil)
      end

    {:noreply, push_patch(socket, to: list_path(socket.assigns, page: 1, sort: sort, dir: dir))}
  end

  @doc """
  The list URL for the current state in `assigns` (`page`, `page_size`,
  `search`, `sort`, `dir`), with `changes` applied.
  """
  def list_path(assigns, changes \\ []) do
    state =
      assigns
      |> Map.take([:page, :page_size, :search, :sort, :dir])
      |> Map.merge(Map.new(changes))

    ~p"/?#{[page: state.page, page_size: state.page_size, q: state.search, sort: state.sort, dir: state.dir]}"
  end

  @doc """
  The URL a column header links to: the active column flips direction; any
  other column starts ascending for names and descending for counts, and the
  page goes back to 1.
  """
  def sort_path(assigns, column) do
    dir =
      cond do
        column == assigns.sort -> flip(assigns.dir)
        column == :name -> :asc
        true -> :desc
      end

    list_path(assigns, page: 1, sort: column, dir: dir)
  end

  defp flip(:asc), do: :desc
  defp flip(:desc), do: :asc

  defp parse_sort(sort, dir) do
    case {Map.fetch(@sort_names, to_string(sort)), dir} do
      {{:ok, key}, "desc"} -> {key, :desc}
      {{:ok, key}, "asc"} -> {key, :asc}
      _ -> {:name, :asc}
    end
  end

  defp parse_page(nil), do: 1

  defp parse_page(page) do
    case Integer.parse(page) do
      {int, _} when int > 0 -> int
      _ -> 1
    end
  end

  defp parse_page_size(page_size) when page_size in ["20", "50", 20, 50],
    do: page_size |> to_string() |> String.to_integer()

  defp parse_page_size(_), do: @default_page_size

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
