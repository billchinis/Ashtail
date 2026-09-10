defmodule KafkaManagerWeb.ProduceInvalidInputTest do
  @moduledoc """
  Regression: a client-supplied header row id that is not a valid integer
  must not crash the produce page via `String.to_integer/1`.
  """

  use KafkaManagerWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "a malformed row id on remove_header is a no-op, not a crash", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/scratch/produce")

    render_click(view, "remove_header", %{"row" => "not-a-number"})

    assert Process.alive?(view.pid)
    assert has_element?(view, "[data-produce-form]")
  end
end
