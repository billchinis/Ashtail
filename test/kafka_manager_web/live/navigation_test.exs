defmodule KafkaManagerWeb.NavigationTest do
  @moduledoc """
  AC-15: every view is reachable by following links from the topic list, and
  every page links back to the topic list and the consumer group list.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "the topic list links to topic detail, the message browser and the produce form", %{
    conn: conn
  } do
    {:ok, index_live, _html} = live(conn, ~p"/")

    {:ok, topic_live, topic_html} =
      index_live
      |> element("[data-topic='orders'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders")

    assert topic_html =~ "Topic orders"
    assert app_nav?(topic_html)

    {:ok, _message_live, message_html} =
      topic_live
      |> element("[data-partition='0'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/partitions/0")

    assert message_html =~ "Topic orders, partition 0"
    assert app_nav?(message_html)
  end

  test "the topic detail page separately links to the produce form", %{conn: conn} do
    {:ok, index_live, _html} = live(conn, ~p"/")

    {:ok, topic_live, _topic_html} =
      index_live
      |> element("[data-topic='orders'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders")

    {:ok, _produce_live, produce_html} =
      topic_live
      |> element("[data-produce-link]")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/produce")

    assert produce_html =~ "Produce to orders"
    assert app_nav?(produce_html)
  end

  test "the group list links to group detail", %{conn: conn} do
    {:ok, groups_live, _html} = live(conn, ~p"/groups")

    {:ok, _group_live, group_html} =
      groups_live
      |> element("[data-group='lagging-analytics'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/groups/lagging-analytics")

    assert group_html =~ "Consumer group lagging-analytics"
    assert app_nav?(group_html)
  end

  defp app_nav?(html) do
    Regex.match?(~r{<a\b(?=[^>]*\shref="/")(?=[^>]*\sdata-nav-topics)[^>]*>}, html) and
      Regex.match?(~r{<a\b(?=[^>]*\shref="/groups")(?=[^>]*\sdata-nav-groups)[^>]*>}, html)
  end
end
