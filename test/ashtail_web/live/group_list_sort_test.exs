defmodule AshtailWeb.GroupListSortTest do
  @moduledoc """
  The consumer group list is paginated like the topic list (20 or 50 per
  page) and sorts by any column from its header, group id ascending by
  default.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  # 4 named groups plus zz-filler-group-01..21 from priv/kafka/seed.sh.
  @total 25

  test "pages 20 at a time, sorted by group id", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/groups")

    ids = group_ids(html)
    assert length(ids) == 20
    assert ids == Enum.sort(ids)
    assert hd(ids) == "lagging-analytics"
    assert html =~ "#{@total} groups"
    assert html =~ "Page 1 of 2"
    assert sort_state(view, "id") == "ascending"

    html = view |> element("[data-next-page]") |> render_click()

    assert_patch(view, ~p"/groups?#{[page: 2, page_size: 20, sort: "id", dir: "asc"]}")
    assert length(group_ids(html)) == @total - 20
    assert List.last(group_ids(html)) == "zz-filler-group-21"
    refute has_element?(view, "[data-next-page]")
  end

  test "a page size of 50 shows every group on one page", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/groups?page=2")

    html = view |> form("[data-page-size-form]", %{"page_size" => "50"}) |> render_change()

    assert_patch(view, ~p"/groups?#{[page: 1, page_size: 50, sort: "id", dir: "asc"]}")
    assert length(group_ids(html)) == @total
    refute has_element?(view, "[data-next-page]")
  end

  test "sorting by total lag puts the most lagging group first, then flips", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/groups?page=2")

    html = view |> element("[data-sort='lag']") |> render_click()

    assert_patch(view, ~p"/groups?#{[page: 1, page_size: 20, sort: "lag", dir: "desc"]}")
    assert sort_state(view, "lag") == "descending"
    assert sort_state(view, "id") == "none"
    assert hd(group_ids(html)) == "lagging-analytics"
    lags = total_lags(html)
    assert lags == Enum.sort(lags, :desc)

    html = view |> element("[data-sort='lag']") |> render_click()
    assert sort_state(view, "lag") == "ascending"
    lags = total_lags(html)
    assert lags == Enum.sort(lags)
    assert hd(lags) == 0
  end

  test "equal lags fall back to group id order", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/groups?sort=lag&dir=desc&page_size=50")

    fillers = html |> group_ids() |> Enum.filter(&String.starts_with?(&1, "zz-filler-group-"))
    assert length(fillers) == 21
    assert fillers == Enum.sort(fillers)
  end

  test "sorting by members and by state", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/groups")

    html = view |> element("[data-sort='members']") |> render_click()
    assert sort_state(view, "members") == "descending"
    assert hd(group_ids(html)) == "live-tailer"

    html = view |> element("[data-sort='state']") |> render_click()
    assert sort_state(view, "state") == "ascending"
    states = group_states(html)
    assert states == Enum.sort(states)

    html = view |> element("[data-sort='state']") |> render_click()
    assert sort_state(view, "state") == "descending"
    assert hd(group_ids(html)) == "live-tailer"
  end

  test "the mobile sort menu applies the chosen column and direction", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/groups")

    html = view |> form("[data-sort-form]", %{"sort" => "lag:desc"}) |> render_change()

    assert_patch(view, ~p"/groups?#{[page: 1, page_size: 20, sort: "lag", dir: "desc"]}")
    assert hd(group_ids(html)) == "lagging-analytics"
  end

  test "an unknown sort and page fall back to the defaults", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/groups?sort=bogus&dir=up&page=99&page_size=7")

    assert sort_state(view, "id") == "ascending"
    assert html =~ "Page 2 of 2"
    assert length(group_ids(html)) == @total - 20
  end

  defp sort_state(view, column) do
    view
    |> element("th[data-sort-column='#{column}']")
    |> render()
    |> then(&Regex.run(~r/aria-sort="([a-z]+)"/, &1, capture: :all_but_first))
    |> hd()
  end

  defp group_ids(html) do
    ~r/data-group="([^"]*)"/ |> Regex.scan(html) |> Enum.map(fn [_, id] -> id end)
  end

  defp group_states(html) do
    ~r/data-group-state="([^"]*)"/ |> Regex.scan(html) |> Enum.map(fn [_, s] -> s end)
  end

  defp total_lags(html) do
    ~r/data-total-lag="(-?\d+)"/
    |> Regex.scan(html)
    |> Enum.map(fn [_, n] -> String.to_integer(n) end)
  end
end
