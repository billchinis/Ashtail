defmodule AshtailWeb.BrokerUnavailableTest do
  @moduledoc """
  An unreachable broker renders an error instead of crashing.
  """

  use AshtailWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import Phoenix.LiveViewTest

  alias Ashtail.BrokerHelpers

  test "visiting / with no broker listening renders a page-level error, not a crash", %{
    conn: conn
  } do
    BrokerHelpers.override_brokers("localhost:19099")

    capture_log(fn ->
      conn = get(conn, ~p"/")
      assert conn.status == 200

      {:ok, view, html} = live(conn)

      assert html =~ "localhost:19099"
      refute has_element?(view, "[data-topic]")
    end)
  end
end
