defmodule AshtailWeb.TopicDataJsonRemoveInvalidRowTest do
  @moduledoc """
  A non-binary `row` on `remove_json_condition` (a crafted payload, since
  `Integer.parse/1` requires a binary) must not crash the page. It is a no-op,
  the same as the offset handlers' `MessageBrowserInvalidInputTest` model.
  """

  use AshtailWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "a non-binary row on remove_json_condition is a no-op, not a crash", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/payments")

    render_click(view, "remove_json_condition", %{"row" => %{"evil" => "1"}})

    assert Process.alive?(view.pid)
  end
end
