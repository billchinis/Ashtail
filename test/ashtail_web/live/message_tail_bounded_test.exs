defmodule AshtailWeb.MessageTailBoundedTest do
  @moduledoc """
  Regression: the tail must not grow `:message_index` (and the `:messages`
  stream it backs) without bound. Tailing in more messages than the page
  size must evict the oldest on-screen rows instead of accumulating
  forever, and the expander must keep working for whatever row is
  currently on screen.
  """

  use AshtailWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import Ashtail.AsyncAssertions

  alias Ashtail.BrokerHelpers

  @page_size 20
  @long_value String.duplicate("x", 250)

  test "tailing past the page size evicts old rows and keeps the expander working", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, ~p"/topics/scratch/partitions/0?page_size=#{@page_size}")

    view |> element("[data-tail-toggle]") |> render_click()
    assert has_element?(view, "[data-tail='live']")

    offsets =
      for i <- 1..(@page_size + 5) do
        value = "#{@long_value}-bounded-#{i}"

        assert {:ok, %{offset: offset}} =
                 BrokerHelpers.produce_probe("scratch", 0, %{key: "bounded-tail", value: value})

        offset
      end

    first_offset = List.first(offsets)
    last_offset = List.last(offsets)

    eventually(fn ->
      assert has_element?(view, "[data-offset='#{last_offset}']")
    end)

    refute has_element?(view, "[data-offset='#{first_offset}']")

    view
    |> element("[data-offset='#{last_offset}'] [data-expand-value]")
    |> render_click()

    expanded_row = view |> element("[data-offset='#{last_offset}']") |> render()
    assert expanded_row =~ "bounded-#{@page_size + 5}"
  end
end
