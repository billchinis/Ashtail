defmodule Mix.Tasks.Screenshots do
  @shortdoc "Boots the app with seed data and screenshots every route"
  @moduledoc """
  Runs `mix kafka.seed`, builds assets, starts the endpoint on a spare port,
  then drives Playwright (scripts/screenshots.mjs) over the same route list the
  smoke test uses. PNGs land in tmp/shots/<route>-<viewport>.png at 1280x800
  and 390x844.
  """
  use Mix.Task

  @port 4004
  @out_dir "tmp/shots"

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("kafka.seed")
    Mix.Task.run("assets.build")
    ensure_playwright!()
    start_server!()

    routes = AshtailWeb.RouteList.all_paths()
    File.rm_rf!(@out_dir)

    env = [
      {"BASE_URL", "http://127.0.0.1:#{@port}"},
      {"ROUTES", Jason.encode!(routes)},
      {"OUT_DIR", @out_dir}
    ]

    sh!("node", ["scripts/screenshots.mjs"], env: env)
  end

  defp ensure_playwright! do
    unless File.dir?("node_modules/playwright") do
      sh!("npm", ["install", "--no-audit", "--no-fund"])
      sh!("npx", ["playwright", "install", "chromium"])
    end
  end

  defp start_server! do
    Mix.Task.run("app.config")

    endpoint =
      :ashtail
      |> Application.get_env(AshtailWeb.Endpoint, [])
      |> Keyword.merge(server: true, watchers: [], http: [ip: {127, 0, 0, 1}, port: @port])

    Application.put_env(:ashtail, AshtailWeb.Endpoint, endpoint)
    Mix.Task.run("app.start")
    wait_for_port!(50)
  end

  defp wait_for_port!(0), do: Mix.raise("server did not come up on port #{@port}")

  defp wait_for_port!(n) do
    case :gen_tcp.connect(~c"127.0.0.1", @port, [], 200) do
      {:ok, socket} ->
        :gen_tcp.close(socket)

      {:error, _} ->
        Process.sleep(200)
        wait_for_port!(n - 1)
    end
  end

  defp sh!(cmd, args, opts \\ []) do
    opts = Keyword.merge([into: IO.stream(:stdio, :line), stderr_to_stdout: true], opts)
    {_, status} = System.cmd(cmd, args, opts)

    if status != 0 do
      Mix.raise("#{cmd} #{Enum.join(args, " ")} exited with #{status}")
    end
  end
end
