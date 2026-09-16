defmodule AshtailWeb.MessageEmptyValueTest do
  @moduledoc """
  A message with an empty value renders an empty marker.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  # Page 1 of the Data view lists pay-120 .. pay-071 newest first.
  @page_1_keys for n <- 120..71//-1, do: "pay-#{String.pad_leading(to_string(n), 3, "0")}"

  test "empty-valued rows render the (empty) marker, others render their value", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/topics/payments")

    rows = rows_by_key(html)

    # Pin the page 1 row set so a row regex that starts matching only the
    # named rows below (instead of every row) fails loudly rather than
    # silently shrinking the "no row is empty-but-unmarked" coverage.
    assert map_size(rows) == 50
    assert Enum.sort(Map.keys(rows)) == Enum.sort(@page_1_keys)

    assert value_block(rows["pay-120"]) == {"(empty)", empty: true}
    assert value_block(rows["pay-090"]) == {"(empty)", empty: true}

    {text, empty?} = value_block(rows["pay-119"])
    assert empty?[:empty] == false
    refute text == "(empty)"
    assert text =~ ~s("amount")

    for {key, body} <- rows do
      {text, _meta} = value_block(body)

      refute String.trim(text) == "",
             "expected row #{key} to render non-blank value block text, got: #{inspect(text)}"
    end

    # Of the 4 null-valued messages seeded (N = 30, 60, 90, 120), only 90 and
    # 120 fall on page 1. This checks the whole page, not just the two named
    # rows above, so a bug that marks another row `(empty)` (or fails to mark
    # one of these two) fails here.
    empty_marked_keys =
      for {key, body} <- rows, elem(value_block(body), 1)[:empty], do: key

    assert Enum.sort(empty_marked_keys) == ["pay-090", "pay-120"]
  end

  defp rows_by_key(html) do
    ~r/<li\b(?=[^>]*\bdata-partition="\d+")(?=[^>]*\bdata-offset="\d+")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Map.new(fn [_full, body] ->
      case Regex.run(~r/title="([^"]*)"/, body) do
        [_, key] ->
          {key, body}

        nil ->
          raise "expected every row to carry a title=\"...\" key (e.g. from message_key/1); " <>
                  "found none in row body: #{inspect(body)}"
      end
    end)
  end

  defp value_block(body) do
    # `data-value` sits on whichever of the three value spans rendered (the
    # short block, the long preview or the expanded full value). The
    # negative lookahead keeps this from also matching `data-value-preview`
    # or `data-value-full`, whose names merely start with the same text.
    [_, attrs, inner] =
      Regex.run(~r/<span([^>]*\bdata-value\b(?!-)[^>]*)>(.*?)<\/span>/s, body)

    {unescape(inner), empty: attrs =~ "data-value-empty"}
  end

  defp unescape(value) do
    value
    |> String.replace("&quot;", "\"")
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&amp;", "&")
  end
end
