defmodule AshtailWeb.NavigationTest do
  @moduledoc """
  Every view is reachable by following links, and every topic page shows the
  five sub-menus with a correctly marked active tab.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  @sub_menu_hrefs [
    "/topics/orders",
    "/topics/orders/partitions",
    "/topics/orders/groups",
    "/topics/orders/configs",
    "/topics/orders/logs"
  ]

  test "the topic list, topic sub-menus, partition browser, produce form and group pages link to each other",
       %{conn: conn} do
    {:ok, index_live, _html} = live(conn, ~p"/")

    {:ok, data_live, data_html} =
      index_live
      |> element("[data-topic='orders'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders")

    assert data_html =~ "Topic orders"
    assert_sub_menu(data_html, "/topics/orders")
    assert app_nav?(data_html)

    {:ok, partitions_live, partitions_html} =
      data_live
      |> element("nav[aria-label='Topic sections'] a", "Partitions")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/partitions")

    assert partitions_html =~ "Topic orders"
    assert_sub_menu(partitions_html, "/topics/orders/partitions")
    assert app_nav?(partitions_html)

    {:ok, groups_live, groups_html} =
      partitions_live
      |> element("nav[aria-label='Topic sections'] a", "Consumer Groups")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/groups")

    assert groups_html =~ "Topic orders"
    assert_sub_menu(groups_html, "/topics/orders/groups")
    assert app_nav?(groups_html)

    {:ok, configs_live, configs_html} =
      groups_live
      |> element("nav[aria-label='Topic sections'] a", "Configs")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/configs")

    assert configs_html =~ "Topic orders"
    assert_sub_menu(configs_html, "/topics/orders/configs")
    assert app_nav?(configs_html)

    {:ok, _logs_live, logs_html} =
      configs_live
      |> element("nav[aria-label='Topic sections'] a", "Logs")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/logs")

    assert logs_html =~ "Topic orders"
    assert_sub_menu(logs_html, "/topics/orders/logs")
    assert app_nav?(logs_html)

    # From the Partitions sub-menu, follow the link to partition 0.
    {:ok, fresh_partitions_live, _fresh_partitions_html} =
      live(conn, ~p"/topics/orders/partitions")

    {:ok, _message_live, message_html} =
      fresh_partitions_live
      |> element("[data-partition='0'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/partitions/0")

    assert message_html =~ "Topic orders, partition 0"
    assert app_nav?(message_html)

    # From the Data sub-menu, follow the Produce button.
    {:ok, fresh_data_live, _fresh_data_html} = live(conn, ~p"/topics/orders")

    {:ok, _produce_live, produce_html} =
      fresh_data_live
      |> element("[data-produce-link]")
      |> render_click()
      |> follow_redirect(conn, ~p"/topics/orders/produce")

    assert produce_html =~ "Produce to orders"
    assert app_nav?(produce_html)

    # From the Consumer Groups sub-menu, follow the lagging-analytics link.
    {:ok, fresh_groups_live, _fresh_groups_html} = live(conn, ~p"/topics/orders/groups")

    {:ok, _group_live, group_html} =
      fresh_groups_live
      |> element("[data-group='lagging-analytics'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/groups/lagging-analytics")

    assert group_html =~ "Consumer group lagging-analytics"
    assert app_nav?(group_html)

    # /groups also links to lagging-analytics.
    {:ok, groups_index_live, _html} = live(conn, ~p"/groups")

    {:ok, _group_live2, group_html2} =
      groups_index_live
      |> element("[data-group='lagging-analytics'] a")
      |> render_click()
      |> follow_redirect(conn, ~p"/groups/lagging-analytics")

    assert group_html2 =~ "Consumer group lagging-analytics"
    assert app_nav?(group_html2)
  end

  defp assert_sub_menu(html, active_href) do
    links = sub_menu_links(html)

    assert Enum.map(links, & &1.href) == @sub_menu_hrefs

    for %{href: href, aria_current: aria_current} <- links do
      if href == active_href do
        assert aria_current == "page"
      else
        assert aria_current == nil
      end
    end
  end

  defp sub_menu_links(html) do
    [_, nav_inner] =
      Regex.run(~r{<nav[^>]*aria-label="Topic sections"[^>]*>(.*?)</nav>}s, html)

    ~r/<a\b([^>]*)>/
    |> Regex.scan(nav_inner)
    |> Enum.map(fn [_, attrs] ->
      [_, href] = Regex.run(~r/href="([^"]*)"/, attrs)
      aria_current = Regex.run(~r/aria-current="([^"]*)"/, attrs)

      %{href: href, aria_current: aria_current && Enum.at(aria_current, 1)}
    end)
  end

  defp app_nav?(html) do
    Regex.match?(~r{<a\b(?=[^>]*\shref="/")(?=[^>]*\sdata-nav-topics)[^>]*>}, html) and
      Regex.match?(~r{<a\b(?=[^>]*\shref="/groups")(?=[^>]*\sdata-nav-groups)[^>]*>}, html)
  end
end
