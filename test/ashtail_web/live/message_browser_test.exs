defmodule AshtailWeb.MessageBrowserTest do
  @moduledoc """
  The message browser reads a chosen partition from a chosen offset.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "browsing orders partition 0 from offset 10 shows 50 rows ascending", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/orders/partitions/0?offset=10")

    offsets = extract_offsets(html)

    assert length(offsets) == 50
    assert offsets == Enum.to_list(10..59)

    for offset <- offsets do
      row = view |> element("[data-offset='#{offset}']") |> render()

      assert row =~ ~r/order-\d{4}/
      assert row =~ "&quot;status&quot;"
      assert row =~ "content-type=application/json"
      assert row =~ "source=seed"
      assert row =~ ~r/\d{4}-\d{2}-\d{2}/
    end
  end

  defp extract_offsets(html) do
    ~r/data-offset="(\d+)"/
    |> Regex.scan(html)
    |> Enum.map(fn [_, offset] -> String.to_integer(offset) end)
  end
end
