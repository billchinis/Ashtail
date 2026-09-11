defmodule KafkaManagerWeb.MessageComponents do
  @moduledoc """
  Rendering helpers for a single message row: key, value and headers.
  """

  use Phoenix.Component

  import KafkaManagerWeb.CoreComponents, only: [icon: 1]

  @truncate_length 200

  @doc """
  Renders a message's key. A `nil` key (AC-8) renders a `<null>` marker
  instead of an empty cell.
  """
  attr :key, :any, default: nil

  def message_key(assigns) do
    ~H"""
    <span
      :if={@key == nil}
      data-null-key
      class="badge badge-ghost badge-sm italic text-base-content/60 border border-base-300"
    >&lt;null&gt;</span>
    <span :if={@key != nil} class="break-all">{@key}</span>
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
    <span
      :if={not @long?}
      class="block bg-base-200 border border-base-300 rounded-box px-2 py-1 whitespace-pre-wrap break-all"
    >{@value}</span>
    <span
      :if={@long? and @expanded?}
      data-value-full
      class="block bg-base-200 border border-base-300 rounded-box px-2 py-1 whitespace-pre-wrap break-all"
    >{@value}</span>
    <button
      :if={@long? and @expanded?}
      type="button"
      data-collapse-value
      class="btn btn-xs btn-ghost mt-1"
      phx-click="collapse_value"
      phx-value-offset={@offset}
    >
      <.icon name="hero-chevron-up" class="size-3" /> Collapse
    </button>
    <span
      :if={@long? and not @expanded?}
      data-value-preview
      class="block bg-base-200 border border-base-300 rounded-box px-2 py-1 whitespace-pre-wrap break-all"
    >{truncate(@value)}</span>
    <button
      :if={@long? and not @expanded?}
      type="button"
      data-expand-value
      class="btn btn-xs btn-ghost mt-1"
      phx-click="expand_value"
      phx-value-offset={@offset}
    >
      <.icon name="hero-chevron-down" class="size-3" /> Expand
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
    <div class="flex flex-col gap-1 max-sm:flex-row max-sm:flex-wrap">
      <span
        :for={{name, value} <- @headers}
        class="message-header inline-block rounded border border-base-300 bg-base-200 px-1.5 py-0.5 font-mono text-xs break-all"
      >{name}={value}</span>
    </div>
    """
  end
end
