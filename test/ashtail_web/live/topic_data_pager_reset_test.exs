defmodule AshtailWeb.TopicDataPagerResetTest do
  @moduledoc """
  Regression: starting a scan, and a rejected filter, must reset `older`/`newer`
  along with the stream. Otherwise the Newer/Older pager links stay on screen
  pointing at cursors from a previous, possibly differently-filtered read.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the pager resets while a scan runs and after a rejected filter", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/orders?page_size=20")

    # 600 messages, page size 20: page 1 has an Older link.
    assert html =~ "data-next-page"

    html =
      view
      |> form("#filter-form", %{"filter" => %{"partition" => "0"}})
      |> render_submit()

    # The scan never completes synchronously with the patch, so this is the
    # previous page's stale pager, still on screen unless `start_scan/6` reset
    # it.
    assert html =~ ~s(data-scan-state="running")
    refute html =~ "data-next-page"
    refute html =~ "data-prev-page"

    render_async(view, 10_000)

    html =
      view
      |> form("#filter-form", %{"filter" => %{"key" => "order-(", "key_mode" => "regex"}})
      |> render_submit()

    assert html =~ ~s(data-filter-error="key")
    refute html =~ "data-next-page"
    refute html =~ "data-prev-page"
  end
end
