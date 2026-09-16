defmodule AshtailWeb.TopicLive.Index do
  @moduledoc """
  The topic list, showing every topic's partition count, replication factor and
  message count for the current page.
  """

  use AshtailWeb, :live_view

  alias Ashtail.Kafka
  alias Ashtail.Kafka.BrokerError

  @default_page_size 20

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
        search: ""
      )
      |> stream_configure(:topics, dom_id: &("topic-" <> slug(&1.name)))
      |> stream(:topics, [])

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    page = parse_page(params["page"])
    page_size = parse_page_size(params["page_size"])
    search = params["q"] || ""

    socket =
      socket
      |> assign(page: page, page_size: page_size, search: search)
      |> fetch_topics()

    {:noreply, socket}
  end

  @impl true
  def handle_event("page_size", %{"page_size" => page_size}, socket) do
    {:noreply, push_patch(socket, to: page_path(1, page_size, socket.assigns.search))}
  end

  @impl true
  def handle_event("search", %{"q" => search}, socket) do
    {:noreply, push_patch(socket, to: page_path(1, socket.assigns.page_size, search))}
  end

  def page_path(page, page_size, search) do
    ~p"/?#{[page: page, page_size: page_size, q: search]}"
  end

  defp parse_page(nil), do: 1

  defp parse_page(page) do
    case Integer.parse(page) do
      {int, _} when int > 0 -> int
      _ -> 1
    end
  end

  defp parse_page_size(page_size) when page_size in ["20", "50"], do: String.to_integer(page_size)
  defp parse_page_size(_), do: @default_page_size

  defp fetch_topics(socket) do
    %{page: page, page_size: page_size, search: search} = socket.assigns

    case Kafka.list_topics(page: page, page_size: page_size, search: search) do
      {:ok, result} ->
        socket
        |> assign(
          broker_error: nil,
          total: result.total,
          page: result.page,
          page_count: result.page_count
        )
        |> stream(:topics, result.topics, reset: true)

      {:error, %BrokerError{} = error} ->
        assign(socket, broker_error: error)
    end
  end

  defp slug(name), do: String.replace(name, ~r/[^a-zA-Z0-9]+/, "-")
end
