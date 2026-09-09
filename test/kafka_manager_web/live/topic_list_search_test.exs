defmodule KafkaManagerWeb.TopicListSearchTest do
  @moduledoc """
  AC-4: the topic list search filters topics by name.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the topic list search filters topics by name", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    html =
      view
      |> form("[data-search-form]", %{"q" => "pay"})
      |> render_change()

    assert count_topic_rows(html) == 1
    assert topic_names(html) == ["payments"]

    html =
      view
      |> form("[data-search-form]", %{"q" => "zz-filler-01"})
      |> render_change()

    assert count_topic_rows(html) == 10
    assert html =~ "10 topics"

    assert topic_names(html) ==
             for(n <- 10..19, do: "zz-filler-0#{n}")

    html =
      view
      |> form("[data-search-form]", %{"q" => ""})
      |> render_change()

    assert html =~ "60 topics"
    assert count_topic_rows(html) == 20
  end

  defp count_topic_rows(html) do
    ~r/data-topic="/ |> Regex.scan(html) |> length()
  end

  defp topic_names(html) do
    ~r/data-topic="([^"]*)"/ |> Regex.scan(html) |> Enum.map(fn [_, name] -> name end)
  end
end
