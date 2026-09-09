defmodule KafkaManagerWeb.MessageComponents do
  @moduledoc """
  Rendering helpers for a single message row: key, value and headers.
  """

  use Phoenix.Component

  @doc """
  Renders a message's key.
  """
  attr :key, :any, default: nil

  def message_key(assigns) do
    ~H"""
    {@key}
    """
  end

  @doc """
  Renders a message's value.
  """
  attr :value, :string, required: true

  def message_value(assigns) do
    ~H"""
    {@value}
    """
  end

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
