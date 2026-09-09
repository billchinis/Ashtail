defmodule KafkaManagerWeb.MessageBrowserPagingTest do
  @moduledoc """
  AC-7: the message browser pages forwards and backwards and changes page size.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "paging forwards and backwards and changing page size", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/topics/orders/partitions/0?offset=10")

    assert extract_offsets(html) == Enum.to_list(10..59)

    html = view |> element("[data-next-page]") |> render_click()
    assert extract_offsets(html) == Enum.to_list(60..99)

    html = view |> element("[data-prev-page]") |> render_click()
    assert extract_offsets(html) == Enum.to_list(10..59)

    html =
      view
      |> element("[data-page-size-form]")
      |> render_change(%{"page_size" => "20"})

    assert extract_offsets(html) == Enum.to_list(10..29)
  end

  defp extract_offsets(html) do
    ~r/data-offset="(\d+)"/
    |> Regex.scan(html)
    |> Enum.map(fn [_, offset] -> String.to_integer(offset) end)
  end
end
