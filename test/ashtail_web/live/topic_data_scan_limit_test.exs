defmodule AshtailWeb.TopicDataScanLimitTest do
  @moduledoc """
  A filtered scan on the Data sub-menu stops at the scan limit and a "Scan more"
  control continues it from where it stopped, keeping the rows already found and
  the running count. The limit is set to 100 messages for this test through
  `:data_scan_budget`, since no seed topic is anywhere near the production
  default.

  `orders` has 6 partitions x 100 messages. With a limit of 100 every read
  gives each partition a fair share of 16 offsets, so a read checks 96
  messages and stops, and the whole topic takes 7 reads.
  """

  use AshtailWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  @default_filter_params %{
    "key" => "",
    "key_mode" => "text",
    "value" => "",
    "value_mode" => "text",
    "header" => "",
    "header_value" => "",
    "header_mode" => "text",
    "partition" => ""
  }

  test "a scan stops at the limit and Scan more continues it, keeping found rows", %{
    conn: conn
  } do
    Application.put_env(:ashtail, :data_scan_budget, 100)
    on_exit(fn -> Application.delete_env(:ashtail, :data_scan_budget) end)

    {:ok, view, _html} = live(conn, ~p"/topics/orders")

    # 10 matches, all in partition 0 at offsets 49..58: found across the
    # third and fourth reads, then the scan keeps going because the page is
    # not full.
    html = apply_filter(view, %{"key" => "ORDER-005"})
    assert scan_state(html) == "complete"
    assert scanned(html) == 96
    assert rows(html) == []
    assert html =~ "data-scan-more"
    refute html =~ "data-next-page"
    refute html =~ "data-prev-page"

    {html, reads} = scan_until_done(view, html)
    assert reads == 7
    # Partition 0's matches wait, buffered, until the other partitions
    # reach their floor, and every continuation reads them again, so the
    # count exceeds the 600 messages of the topic.
    assert scanned(html) >= 600
    assert keys(html) == Enum.map(59..50//-1, &order_key/1)
    assert Enum.map(rows(html), & &1.offset) == Enum.to_list(58..49//-1)
    refute html =~ "data-scan-more"
    refute html =~ "data-next-page"
    refute html =~ "data-prev-page"
    refute html =~ "data-broker-error"

    # One match at the very start of partition 0: every read but the last
    # stops at the limit with nothing found.
    html = apply_filter(view, %{"key" => "^order-0001$", "key_mode" => "regex"})
    assert scan_state(html) == "complete"
    assert scanned(html) == 96
    assert rows(html) == []
    assert html =~ "data-scan-more"

    {html, reads} = scan_until_done(view, html)
    assert reads == 7
    assert scanned(html) == 600
    assert keys(html) == ["order-0001"]
    assert [%{partition: 0, offset: 0}] = rows(html)
    refute html =~ "data-scan-more"

    # A scan that fills its page within the limit shows no Scan more: one
    # scoped partition gets the whole limit, every one of its 100 keys
    # matches, and the page of 50 fills.
    html = apply_filter(view, %{"key" => "order-", "partition" => "0"})
    assert scan_state(html) == "complete"
    assert scanned(html) == 100
    assert length(rows(html)) == 50
    refute html =~ "data-scan-more"
    assert html =~ "data-next-page"
  end

  defp scan_until_done(view, html, reads \\ 1) do
    if html =~ "data-scan-more" and reads < 20 do
      before = scanned(html)

      html =
        view
        |> element("[data-scan-more]")
        |> render_click()

      assert scan_state(html) == "running"
      assert scanned(html) == before

      html = render_async(view, 10_000)
      assert scan_state(html) == "complete"
      assert scanned(html) > before

      scan_until_done(view, html, reads + 1)
    else
      {html, reads}
    end
  end

  defp order_key(n), do: "order-" <> String.pad_leading(Integer.to_string(n), 4, "0")

  defp apply_filter(view, overrides) do
    params = Map.merge(@default_filter_params, overrides)

    view
    |> form("#filter-form", %{"filter" => params})
    |> render_submit()

    render_async(view, 10_000)
  end

  defp rows(html) do
    ~r/<li\b(?=[^>]*\bdata-partition="(\d+)")(?=[^>]*\bdata-offset="(\d+)")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.map(fn [_full, partition, offset, body] ->
      %{partition: String.to_integer(partition), offset: String.to_integer(offset), body: body}
    end)
  end

  defp keys(html) do
    Enum.map(rows(html), fn %{body: body} ->
      case Regex.run(~r/title="([^"]*)"/, body) do
        [_, key] -> key
        nil -> nil
      end
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
