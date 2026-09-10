defmodule KafkaManagerWeb.MessageBrowserInvalidInputTest do
  @moduledoc """
  Regression: unvalidated integers from the URL and from client-supplied
  event values must not crash the page. A malformed `:partition` path
  segment renders the app's normal "not found" page (`plug_status: 404`)
  instead of an unhandled 500, and a malformed `offset` in an
  `expand_value`/`collapse_value` event is a no-op instead of a crash.

  The 404 is asserted, not just the exception's type: `assert_error_sent`
  on the dead (initial HTTP) render proves the endpoint actually sends a
  404 response, and `error.plug_status` on the connected LiveView mount
  proves the raised exception still carries `plug_status: 404` there too.
  Deleting `plug_status: 404` from `InvalidPartitionError` fails both.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  alias KafkaManagerWeb.MessageLive.Index.InvalidPartitionError

  test "a non-numeric partition path segment sends a 404 on the dead render", %{conn: conn} do
    assert_error_sent(404, fn -> get(conn, ~p"/topics/orders/partitions/x") end)
  end

  test "a negative partition path segment sends a 404 on the dead render", %{conn: conn} do
    assert_error_sent(404, fn -> get(conn, ~p"/topics/orders/partitions/-1") end)
  end

  test "a non-numeric partition path segment raises a 404 on the connected mount",
       %{conn: conn} do
    error =
      assert_raise InvalidPartitionError, fn ->
        live(conn, ~p"/topics/orders/partitions/x")
      end

    assert error.plug_status == 404
  end

  test "a negative partition path segment raises a 404 on the connected mount",
       %{conn: conn} do
    error =
      assert_raise InvalidPartitionError, fn ->
        live(conn, ~p"/topics/orders/partitions/-1")
      end

    assert error.plug_status == 404
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
