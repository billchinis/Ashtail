defmodule KafkaManagerWeb.MessageBrowserInvalidInputTest do
  @moduledoc """
  Regression: unvalidated integers from the URL and from client-supplied
  event values must not crash the page. A malformed `:partition` path
  segment renders the app's normal "not found" page (`plug_status: 404`)
  instead of an unhandled 500, and a malformed `offset` in an
  `expand_value`/`collapse_value` event is a no-op instead of a crash.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias KafkaManagerWeb.MessageLive.Index.InvalidPartitionError

  test "a non-numeric partition path segment is a 404, not a 500", %{conn: conn} do
    assert_raise InvalidPartitionError, fn ->
      live(conn, ~p"/topics/orders/partitions/x")
    end
  end

  test "a negative partition path segment is a 404, not a 500", %{conn: conn} do
    assert_raise InvalidPartitionError, fn ->
      live(conn, ~p"/topics/orders/partitions/-1")
    end
  end

  test "a malformed offset on expand_value is a no-op, not a crash", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/orders/partitions/0")

    render_click(view, "expand_value", %{"offset" => "not-a-number"})

    assert Process.alive?(view.pid)
    assert has_element?(view, "[data-offset='0']")
  end

  test "a malformed offset on collapse_value is a no-op, not a crash", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/orders/partitions/0")

    render_click(view, "collapse_value", %{"offset" => "not-a-number"})

    assert Process.alive?(view.pid)
  end
end
