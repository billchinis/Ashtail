defmodule KafkaManagerWeb.TopicDataTailArmingTest do
  @moduledoc """
  Regression (fix run, item 3): turning tailing on while a page-1 scan is
  still running must not arm `tail_from` from a previous, possibly
  differently-filtered read — it must wait for the running scan's own
  page-1 read to complete and arm from that read's high-water mark
  (docs/PLAN.md 4.10). A rejected filter must not tail at all, and the page
  must show the tail as off.
  """

  use KafkaManagerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias KafkaManager.BrokerHelpers

  test "toggling tailing on mid-scan arms tail_from from the scan's own completed read", %{
    conn: conn
  } do
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

    # The filtered scan runs as a linked async task (docs/PLAN.md 6.6) and
    # never completes synchronously with the patch above, so this window is
    # exactly where the bug lived: `page_high` still holds the pre-"gap"
    # value from the unfiltered read.
    assert render(view) =~ ~s(data-scan-state="running")

    view |> element("[data-tail-toggle]") |> render_click()

    render_async(view, 10_000)
    assert has_element?(view, "[data-tail='live']")

    # `tail_from` must equal the completed filtered scan's own `page_high`,
    # not the stale value captured before "gap" was produced. There is no
    # public accessor for LiveView assigns, so this reads the socket
    # directly — the only way to observe the exact offset map without
    # relying on further timing.
    assigns = :sys.get_state(view.pid).socket.assigns
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
end
