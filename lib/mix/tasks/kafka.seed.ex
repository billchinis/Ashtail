defmodule Mix.Tasks.Kafka.Seed do
  @shortdoc "Starts the Redpanda container and loads idempotent fixtures"
  @moduledoc """
  Brings up the Redpanda broker from docker-compose.yml, waits for it to be
  healthy, then runs priv/kafka/seed.sh. Safe to run repeatedly.
  """
  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    sh!("docker", ["compose", "up", "-d", "--wait", "redpanda"])
    sh!("bash", ["priv/kafka/seed.sh"])
  end

  defp sh!(cmd, args) do
    {_, status} = System.cmd(cmd, args, into: IO.stream(:stdio, :line), stderr_to_stdout: true)

    if status != 0 do
      Mix.raise("#{cmd} #{Enum.join(args, " ")} exited with #{status}")
    end
  end
end
