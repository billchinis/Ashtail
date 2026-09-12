defmodule KafkaManagerWeb.MessageEmptyValueTest do
  @moduledoc """
  AC-25: a message with an empty value renders an empty marker.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "empty-valued rows render the (empty) marker, others render their value", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/topics/payments")

    rows = rows_by_key(html)

    assert value_block(rows["pay-120"]) == {"(empty)", empty: true}
    assert value_block(rows["pay-090"]) == {"(empty)", empty: true}

    {text, empty?} = value_block(rows["pay-119"])
    assert empty?[:empty] == false
    refute text == "(empty)"
    assert text =~ ~s("amount")

    for {key, body} <- rows do
      {text, meta} = value_block(body)

      refute String.trim(text) == "",
             "expected row #{key} to render non-blank value block text, got: #{inspect(text)}"

      if meta[:empty], do: assert(text == "(empty)")
    end
  end

  defp rows_by_key(html) do
    ~r/<li\b(?=[^>]*\bdata-partition="\d+")(?=[^>]*\bdata-offset="\d+")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Map.new(fn [_full, body] ->
      [_, key] = Regex.run(~r/title="([^"]*)"/, body)
      {key, body}
    end)
  end

  defp value_block(body) do
    [_, attrs, inner] =
      Regex.run(~r/<span([^>]*\bdata-value\b[^>]*)>(.*?)<\/span>/s, body)

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
