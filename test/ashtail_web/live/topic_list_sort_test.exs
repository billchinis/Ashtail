defmodule AshtailWeb.TopicListSortTest do
  @moduledoc """
  The topic list sorts by any column from its header. Name ascending is the
  default, the active column is marked, and clicking it again flips the
  direction.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "defaults to name ascending with the Name column marked", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/")

    assert topic_names(html) == Enum.sort(topic_names(html))
    assert sort_state(view, "name") == "ascending"
    assert sort_state(view, "partitions") == "none"
    assert sort_state(view, "replication") == "none"
    assert sort_state(view, "messages") == "none"
  end

  test "clicking a numeric column sorts it descending, clicking again ascending", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/?page=2")

    html = view |> element("[data-sort='messages']") |> render_click()
    assert_patch(view, ~p"/?#{[page: 1, page_size: 20, q: "", sort: "messages", dir: "desc"]}")

    assert sort_state(view, "messages") == "descending"
    assert sort_state(view, "name") == "none"
    counts = message_counts(html)
    assert counts == Enum.sort(counts, :desc)
    assert Enum.max(counts) > 0

    html = view |> element("[data-sort='messages']") |> render_click()
    assert sort_state(view, "messages") == "ascending"
    counts = message_counts(html)
    assert counts == Enum.sort(counts)
  end

  test "sorting by partitions keeps the search and the page size", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/?page_size=50&q=o")

    html = view |> element("[data-sort='partitions']") |> render_click()

    assert_patch(
      view,
      ~p"/?#{[page: 1, page_size: 50, q: "o", sort: "partitions", dir: "desc"]}"
    )

    assert hd(topic_names(html)) == "notifications"
    assert Enum.all?(topic_names(html), &String.contains?(&1, "o"))
  end

  test "clicking Name while it is active flips it to descending", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    html = view |> element("[data-sort='name']") |> render_click()

    assert sort_state(view, "name") == "descending"
    assert topic_names(html) == Enum.sort(topic_names(html), :desc)
  end

  test "the mobile sort menu applies the chosen column and direction", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    html = view |> form("[data-sort-form]", %{"sort" => "partitions:desc"}) |> render_change()

    assert_patch(view, ~p"/?#{[page: 1, page_size: 20, q: "", sort: "partitions", dir: "desc"]}")
    assert sort_state(view, "partitions") == "descending"
    assert hd(topic_names(html)) == "notifications"
  end

  test "an unknown sort falls back to name ascending", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/?sort=bogus&dir=sideways")

    assert sort_state(view, "name") == "ascending"
    assert topic_names(html) == Enum.sort(topic_names(html))
  end

  test "a sort survives a reload from its URL", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/?sort=partitions&dir=asc")

    assert sort_state(view, "partitions") == "ascending"
    counts = cell_numbers(html, "data-partitions")
    assert counts == Enum.sort(counts)
  end

  defp sort_state(view, column) do
    [state] =
      view
      |> element("th[data-sort-column='#{column}']")
      |> render()
      |> then(&Regex.run(~r/aria-sort="([a-z]+)"/, &1, capture: :all_but_first))

    state
  end

  defp topic_names(html) do
    ~r/data-topic="([^"]*)"/ |> Regex.scan(html) |> Enum.map(fn [_, name] -> name end)
  end

  defp message_counts(html), do: cell_numbers(html, "data-messages")

  defp cell_numbers(html, attr) do
    ~r/#{attr}[^>]*>\s*(\d+)\s*</
    |> Regex.scan(html)
    |> Enum.map(fn [_, n] -> String.to_integer(n) end)
  end
end
