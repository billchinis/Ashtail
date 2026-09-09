defmodule KafkaManagerWeb.MessageComponents do
  @moduledoc """
  Rendering helpers for a single message row: key, value and headers.
  """

  use Phoenix.Component

  @truncate_length 200

  @doc """
  Renders a message's key. A `nil` key (AC-8) renders a `<null>` marker
  instead of an empty cell.
  """
  attr :key, :any, default: nil

  def message_key(assigns) do
    ~H"""
    <span :if={@key == nil} data-null-key>&lt;null&gt;</span>
    {if @key != nil, do: @key}
    """
  end

  @doc """
  Renders a message's value. Values longer than the truncation length
  (AC-8) render behind a server-side expander (PLAN 4.1, P4) instead of in
  full.
  """
  attr :value, :string, required: true
  attr :expanded?, :boolean, default: false
  attr :offset, :integer, required: true

  def message_value(assigns) do
    assigns = assign(assigns, :long?, String.length(assigns.value) > @truncate_length)

    ~H"""
    <span :if={not @long?}>{@value}</span>
    <span :if={@long? and @expanded?} data-value-full>{@value}</span>
    <button
      :if={@long? and @expanded?}
      type="button"
      data-collapse-value
      phx-click="collapse_value"
      phx-value-offset={@offset}
    >
      Collapse
    </button>
    <span :if={@long? and not @expanded?} data-value-preview>{truncate(@value)}</span>
    <button
      :if={@long? and not @expanded?}
      type="button"
      data-expand-value
      phx-click="expand_value"
      phx-value-offset={@offset}
    >
      Expand
    </button>
    """
  end

  defp truncate(value), do: String.slice(value, 0, @truncate_length) <> "…"

  @doc """
  Renders a message's headers as `name=value` pairs.
  """
  attr :headers, :list, required: true

  def message_headers(assigns) do
    ~H"""
    <span :for={{name, value} <- @headers} class="message-header">{name}={value}</span>
    """
  end
end
