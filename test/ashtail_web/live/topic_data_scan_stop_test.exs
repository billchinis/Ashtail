defmodule AshtailWeb.TopicDataScanStopTest do
  @moduledoc """
  A running scan on the Data sub-menu shows a Stop control, and stopping it
  cancels the read, keeps the rows and the count found so far, and leaves the
  LiveView alive.

  The scan has to still be running when Stop is clicked, so this test sets
  `:data_scan_chunk` to 1: every refill then reads one offset per partition
  through its own fetch, which keeps a 600-message scan of `orders` running
  for seconds rather than milliseconds.
  """

  use AshtailWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import Ashtail.AsyncAssertions

  test "Stop cancels a running scan and keeps what it found", %{conn: conn} do
    Application.put_env(:ashtail, :data_scan_chunk, 1)
    on_exit(fn -> Application.delete_env(:ashtail, :data_scan_chunk) end)

    # `order-0200`, `order-0400` and `order-0600` sit at the newest end of
    # their partitions, so the first refill rounds already find rows.
    {:ok, view, html} = live(conn, ~p"/topics/orders?key=00$&key_mode=regex")
    assert scan_state(html) == "running"
    assert html =~ "data-scan-stop"

    eventually(fn ->
      html = render(view)
      assert scan_state(html) == "running"
      assert scanned(html) > 0
      assert rows(html) != []
    end)

    html =
      view
      |> element("[data-scan-stop]")
      |> render_click()

    assert scan_state(html) == "stopped"
    refute html =~ "data-scan-stop"
    refute html =~ "data-scan-more"
    checked = scanned(html)
    assert checked > 0 and checked < 600
    found = rows(html)
    assert found != []
    assert Enum.all?(found, fn %{offset: offset} -> rem(offset + 1, 100) == 0 end)

    # Nothing keeps arriving from the cancelled read.
    Process.sleep(500)
    html = render(view)
    assert Process.alive?(view.pid)
    assert scan_state(html) == "stopped"
    assert scanned(html) == checked
    assert rows(html) == found
    refute html =~ "data-broker-error"
  end

  defp rows(html) do
    ~r/<li\b(?=[^>]*\bdata-partition="(\d+)")(?=[^>]*\bdata-offset="(\d+)")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.map(fn [_full, partition, offset, _body] ->
      %{partition: String.to_integer(partition), offset: String.to_integer(offset)}
    end)
  end

  defp scan_state(html) do
    [_, state] = Regex.run(~r/data-scan-state="([^"]*)"/, html)
    state
  end

  defp scanned(html) do
    [_, n] = Regex.run(~r/data-scanned="(\d+)"/, html)
    String.to_integer(n)
  end
end
