defmodule KafkaManagerWeb.SmokeTest do
  @moduledoc """
  Generic smoke journey: every GET route (see KafkaManagerWeb.RouteList) must
  respond 200 and, for LiveViews, survive a connected mount, without writing
  anything at [warning] or [error] level to the log.
  """
  use KafkaManagerWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import Phoenix.LiveViewTest

  alias KafkaManagerWeb.RouteList

  @params RouteList.params()
  @routes RouteList.all_routes(@params)

  test "there is at least one route to smoke" do
    refute Enum.empty?(@routes)
  end

  for %{path: path, live?: live?} <- @routes do
    @path path
    @live? live?

    test "GET #{path} responds 200 with a clean log", %{conn: conn} do
      log =
        capture_log(fn ->
          conn = get(conn, @path)
          assert conn.status == 200, "GET #{@path} returned #{conn.status}"

          if @live? do
            assert {:ok, _view, _html} = live(conn, @path)
          end
        end)

      refute log =~ "[error]", "GET #{@path} logged an error:\n#{log}"
      refute log =~ "[warning]", "GET #{@path} logged a warning:\n#{log}"
    end
  end
end
