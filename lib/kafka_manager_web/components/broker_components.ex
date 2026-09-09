defmodule KafkaManagerWeb.BrokerComponents do
  @moduledoc """
  The single, uniform way a broker failure is shown on every page.
  """

  use Phoenix.Component

  @doc """
  Renders nothing for `nil`. Otherwise a page-level alert naming the broker
  address and giving advice, never an inspected error tuple.
  """
  attr :error, :any, default: nil

  def broker_error(%{error: nil} = assigns), do: ~H""

  def broker_error(assigns) do
    ~H"""
    <div id="broker-error" role="alert" data-broker-error class="alert alert-error">
      <p>
        Could not reach the broker at <strong>{@error.address}</strong>.
      </p>
      <p>{@error.message}</p>
    </div>
    """
  end
end
