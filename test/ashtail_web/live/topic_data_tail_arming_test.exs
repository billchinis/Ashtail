defmodule AshtailWeb.TopicDataTailArmingTest do
  @moduledoc """
  Regression: turning tailing on while a page-1 scan is still running must
  not arm `tail_from` from a previous, possibly differently-filtered read —
  it must wait for the running scan's own page-1 read to complete and arm
  from that read's high-water mark. A rejected filter must not tail at all,
  and the page must show the tail as off, whether the filter is rejected
  while already tailing or the toggle is clicked while a rejected filter is
  already on screen.

  The first test sets `:data_tail_interval_ms` far longer than the test itself
  can run: otherwise, if the LiveView's own 1-second timer happened to fire
  between the scan completing and the assertion below, a stale `tail_from` would
  catch itself up to the correct value on that tick, before this test ever got a
  chance to see it wrong, and the test would pass whether or not the arming
  logic was correct.
  """

  use AshtailWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Ashtail.BrokerHelpers
  alias Ashtail.LiveViewHelpers

  test "toggling tailing on mid-scan arms tail_from from the scan's own completed read", %{
    conn: conn
  } do
    Application.put_env(:ashtail, :data_tail_interval_ms, 3_600_000)
    on_exit(fn -> Application.delete_env(:ashtail, :data_tail_interval_ms) end)

    u = "midscan#{System.system_time(:nanosecond)}"

    assert {:ok, %{partition: 0}} =
             BrokerHelpers.produce_probe("scratch", 0, %{key: "#{u}-before", value: "before"})

    {:ok, view, _html} = live(conn, ~p"/topics/scratch")
    render_async(view, 10_000)

    # A second, unrelated write after the unfiltered page-1 read's
    # `page_high` was captured, so a `tail_from` armed from that stale read
    # would land strictly before the correct one.
    assert {:ok, %{partition: 0}} =
             BrokerHelpers.produce_probe("scratch", 0, %{key: "#{u}-gap", value: "gap"})

    view
    |> form("#filter-form", %{"filter" => %{"key" => u, "key_mode" => "text"}})
    |> render_submit()

    # The filtered scan runs as a linked async task and never completes
    # synchronously with the patch above, so this window is exactly where the
    # bug lived: `page_high` still holds the pre-"gap" value from the unfiltered
    # read.
    assert render(view) =~ ~s(data-scan-state="running")

    view |> element("[data-tail-toggle]") |> render_click()

    render_async(view, 10_000)
    assert has_element?(view, "[data-tail='live']")

    # `tail_from` must equal the completed filtered scan's own `page_high`,
    # not the stale value captured before "gap" was produced. There is no
    # public accessor for LiveView assigns, so this goes through a
    # documented test helper instead of an ad hoc `:sys.get_state/1` call —
    # the only way to observe the exact offset map without relying on
    # further timing, and with the timer disarmed above, deterministic.
    assigns = LiveViewHelpers.assigns(view.pid)
    # `tail_from == page_high` would pass vacuously if both were `nil`, so pin
    # `tail_from` to an actual offset map too.
    refute is_nil(assigns.tail_from)
    assert assigns.tail_from == assigns.page_high
  end

  test "a rejected filter stops tailing and never tails at all", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/orders")
    render_async(view, 10_000)

    view |> element("[data-tail-toggle]") |> render_click()
    assert has_element?(view, "[data-tail='live']")

    html =
      view
      |> form("#filter-form", %{"filter" => %{"key" => "order-(", "key_mode" => "regex"}})
      |> render_submit()

    assert html =~ ~s(data-filter-error="key")
    assert html =~ ~s(data-tail="off")
    refute html =~ ~s(data-tail="live")
  end

  test "toggling tail on while the current filter is already rejected never arms it",
       %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/orders")
    render_async(view, 10_000)

    html =
      view
      |> form("#filter-form", %{"filter" => %{"key" => "order-(", "key_mode" => "regex"}})
      |> render_submit()

    assert html =~ ~s(data-filter-error="key")
    refute html =~ ~s(data-tail="live")

    html = view |> element("[data-tail-toggle]") |> render_click()

    assert html =~ ~s(data-tail="off")
    refute html =~ ~s(data-tail="live")
  end
end
