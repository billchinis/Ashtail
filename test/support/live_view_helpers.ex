defmodule KafkaManager.LiveViewHelpers do
  @moduledoc """
  Test-only introspection for a mounted LiveView process. Phoenix.LiveViewTest
  has no public accessor for a view's assigns, and scattering
  `:sys.get_state/1` calls across test files couples them directly to
  LiveView's private socket shape. This centralises that one dependency in a
  single, documented place, for the rare assertion that has no rendered
  signal.
  """

  @doc """
  The assigns of the LiveView running at `pid`.
  """
  @spec assigns(pid()) :: map()
  def assigns(pid) when is_pid(pid) do
    :sys.get_state(pid).socket.assigns
  end
end
