defmodule KafkaManagerWeb.TopicListPaginationTest do
  @moduledoc """
  AC-3: the topic list paginates with a selectable page size.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the topic list paginates with a selectable page size", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/")

    assert count_topic_rows(html) == 20
    assert html =~ "60 topics"

    html =
      view
      |> form("[data-page-size-form]", %{"page_size" => "50"})
      |> render_change()

    assert count_topic_rows(html) == 50

    assert first_topic_name(html) ==
             "audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

    html =
      view
      |> element("[data-next-page]")
      |> render_click()

    assert count_topic_rows(html) == 10
    assert last_topic_name(html) == "zz-filler-053"
    refute has_element?(view, "[data-next-page]")
  end

  defp count_topic_rows(html) do
    ~r/data-topic="/ |> Regex.scan(html) |> length()
  end

  defp first_topic_name(html) do
    html |> topic_names() |> List.first()
  end

  defp last_topic_name(html) do
    html |> topic_names() |> List.last()
  end

  defp topic_names(html) do
    ~r/data-topic="([^"]*)"/ |> Regex.scan(html) |> Enum.map(fn [_, name] -> name end)
  end
end
