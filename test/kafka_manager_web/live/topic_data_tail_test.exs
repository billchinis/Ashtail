defmodule KafkaManagerWeb.TopicDataTailTest do
  @moduledoc """
  The Data sub-menu tails new messages at the top, active filters apply to
  tailed rows, and a broker lost mid-tail renders the existing page-level error
  and stops the tail.
  """

  use KafkaManagerWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import Phoenix.LiveViewTest
  import KafkaManager.AsyncAssertions

  alias KafkaManager.BrokerHelpers
  alias KafkaManager.TcpProxy

  test "tailing prepends new rows through the active filter, and a lost broker stops it",
       %{conn: conn} do
    proxy = start_supervised!({TcpProxy, to: {"localhost", 19_092}})
    port = TcpProxy.port(proxy)
    address = "localhost:#{port}"

    # `System.unique_integer/1` resets on every fresh `mix test` invocation
    # (it is per-VM, not per-cluster-lifetime), so it can collide with a key
    # an earlier run already left on the shared `scratch` topic.
    # `system_time` does not.
    u = "tailfilter#{System.system_time(:nanosecond)}"

    BrokerHelpers.override_brokers(address)

    assert {:ok, %{partition: 0}} =
             BrokerHelpers.produce_probe("scratch", 0, %{key: "#{u}-old", value: "old"})

    capture_log(fn ->
      {:ok, view, _html} = live(conn, ~p"/topics/scratch")

      view
      |> form("#filter-form", %{"filter" => %{"key" => u, "key_mode" => "text"}})
      |> render_submit()

      render_async(view, 10_000)
      html = render(view)

      assert offset_rows(html) |> length() == 1
      assert Enum.at(offset_rows(html), 0) |> elem(1) =~ "#{u}-old"

      view |> element("[data-tail-toggle]") |> render_click()
      assert has_element?(view, "[data-tail='live']")

      assert {:ok, %{partition: 0}} =
               BrokerHelpers.produce_probe("scratch", 0, %{key: "tail-noise", value: "noise"})

      assert {:ok, %{partition: 0}} =
               BrokerHelpers.produce_probe("scratch", 0, %{key: "#{u}-new", value: "new"})

      eventually(fn ->
        rows = offset_rows(render(view))
        assert length(rows) == 2
        assert elem(Enum.at(rows, 0), 1) =~ "#{u}-new"
        assert elem(Enum.at(rows, 1), 1) =~ "#{u}-old"
      end)

      refute render(view) =~ "tail-noise"
      assert has_element?(view, "[data-tail='live']")

      TcpProxy.cut(proxy)

      eventually(fn ->
        assert has_element?(view, "[data-broker-error]")
        assert render(view) =~ address
      end)

      refute has_element?(view, "[data-tail='live']")
      assert has_element?(view, "[data-tail='off']")

      html = render(view)
      assert html =~ "#{u}-new"
      assert html =~ "#{u}-old"
    end)
  end

  defp offset_rows(html) do
    ~r/<li\b(?=[^>]*\bdata-offset="(\d+)")[^>]*>(.*?)<\/li>/s
    |> Regex.scan(html)
    |> Enum.map(fn [_full, offset, body] -> {String.to_integer(offset), body} end)
  end
end
