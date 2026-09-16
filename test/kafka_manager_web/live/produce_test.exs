defmodule KafkaManagerWeb.ProduceTest do
  @moduledoc """
  Producing a message through the form writes it to the chosen partition, and
  the offset it reports is where the message can be browsed.
  """

  use KafkaManagerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  test "producing a message writes it to the chosen partition", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/topics/scratch/produce")

    first_data = %{
      "message" => %{
        "partition" => "0",
        "key" => "produce-probe",
        "value" => ~s({"probe":true}),
        "headers" => %{"0" => %{"name" => "source", "value" => "form"}},
        "timestamp" => "2026-01-02T03:04:05Z"
      }
    }

    html1 = view |> form("[data-produce-form]", first_data) |> render_submit()

    assert html1 =~ "data-produce-result"
    offset1 = extract_offset(html1)
    assert extract_partition(html1) == 0

    second_data = %{
      "message" => %{
        "partition" => "0",
        "null_key" => "true",
        "value" => ~s({"probe":"null-key"})
      }
    }

    html2 = view |> form("[data-produce-form]", second_data) |> render_submit()

    assert extract_partition(html2) == 0
    offset2 = extract_offset(html2)
    assert offset2 == offset1 + 1

    {:ok, browse_view, _html} = live(conn, ~p"/topics/scratch/partitions/0?offset=#{offset1}")

    first_row = browse_view |> element("[data-offset='#{offset1}']") |> render()
    assert first_row =~ "produce-probe"
    assert first_row =~ "probe&quot;:true"
    assert first_row =~ "source=form"
    assert first_row =~ "2026-01-02T03:04:05"

    second_row = browse_view |> element("[data-offset='#{offset2}']") |> render()
    assert second_row =~ "data-null-key"
    assert second_row =~ "&lt;null&gt;"
    assert second_row =~ "probe&quot;:&quot;null-key"
  end

  defp extract_offset(html) do
    [_, offset] = Regex.run(~r/data-offset="(\d+)"/, html)
    String.to_integer(offset)
  end

  defp extract_partition(html) do
    [_, partition] = Regex.run(~r/data-partition="(\d+)"/, html)
    String.to_integer(partition)
  end
end
