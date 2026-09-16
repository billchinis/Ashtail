defmodule Ashtail.AsyncAssertions do
  @moduledoc """
  Waits for an assertion to stop raising instead of sending a fake tick or
  sleeping once and asserting: tests that observe the tail need to wait for the
  LiveView's own timer to fire.
  """

  @default_timeout 5_000
  @interval 100

  @doc """
  Retries `fun` every #{@interval} ms until it stops raising, or raises the
  last failure once `timeout` ms have elapsed.
  """
  @spec eventually((-> any()), pos_integer()) :: any()
  def eventually(fun, timeout \\ @default_timeout) when is_function(fun, 0) do
    deadline = System.monotonic_time(:millisecond) + timeout
    attempt(fun, deadline)
  end

  defp attempt(fun, deadline) do
    fun.()
  rescue
    error ->
      if System.monotonic_time(:millisecond) >= deadline do
        reraise error, __STACKTRACE__
      else
        Process.sleep(@interval)
        attempt(fun, deadline)
      end
  end
end
