defmodule AshtailWeb.TailBrokerLostTest do
  @moduledoc """
  A broker lost mid-tail renders an error and stops the tail.
  """

  use AshtailWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import Phoenix.LiveViewTest
  import Ashtail.AsyncAssertions

  alias Ashtail.BrokerHelpers
  alias Ashtail.TcpProxy

  test "the LiveView survives the broker vanishing mid-tail", %{conn: conn} do
    proxy = start_supervised!({TcpProxy, to: {"localhost", 19_092}})
    port = TcpProxy.port(proxy)
    address = "localhost:#{port}"

    BrokerHelpers.override_brokers(address)

    capture_log(fn ->
      {:ok, view, _html} = live(conn, ~p"/topics/scratch/partitions/0")

      view |> element("[data-tail-toggle]") |> render_click()
      assert has_element?(view, "[data-tail='live']")

      value = ~s({"probe":"tail-broker-lost"})

      assert {:ok, %{partition: 0, offset: offset}} =
               BrokerHelpers.produce_probe("scratch", 0, %{
                 key: "tail-broker-lost",
                 value: value
               })

      eventually(fn ->
        row = view |> element("[data-offset='#{offset}']") |> render()
        assert row =~ "tail-broker-lost"
      end)

      TcpProxy.cut(proxy)

      eventually(fn ->
        assert has_element?(view, "[data-broker-error]")
        assert render(view) =~ address
      end)

      refute has_element?(view, "[data-tail='live']")
      assert has_element?(view, "[data-tail='off']")

      # the row received before the broker vanished is still on screen
      assert has_element?(view, "[data-offset='#{offset}']")
    end)
  end
end
