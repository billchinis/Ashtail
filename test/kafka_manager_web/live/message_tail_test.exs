defmodule KafkaManagerWeb.MessageTailTest do
  @moduledoc """
  AC-9: tailing appends messages produced after the browser is open.
  """

  use KafkaManagerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import KafkaManager.AsyncAssertions

  alias KafkaManager.BrokerHelpers

  test "a message produced while tailing is on appears without reloading", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/scratch/partitions/0")

    refute has_element?(view, "[data-tail='live']")
    assert has_element?(view, "[data-tail='off']")

    view |> element("[data-tail-toggle]") |> render_click()

    assert has_element?(view, "[data-tail='live']")

    value = ~s({"probe":"tail"})

    assert {:ok, %{partition: 0, offset: offset}} =
             BrokerHelpers.produce_probe("scratch", 0, %{key: "tail-probe", value: value})

    eventually(fn ->
      row = view |> element("[data-offset='#{offset}']") |> render()
      assert row =~ "tail-probe"
      assert row =~ "probe&quot;:&quot;tail"
    end)

    # The row arrived on the same, still-connected view: no `live/2` re-mount
    # and no browse-form resubmission were needed to see it.
    assert has_element?(view, "[data-tail='live']")
  end
end
