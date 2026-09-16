defmodule AshtailWeb.Router do
  use AshtailWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {AshtailWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", AshtailWeb do
    pipe_through :browser

    live "/", TopicLive.Index, :index
    live "/topics/:topic", TopicLive.Data, :data
    live "/topics/:topic/partitions", TopicLive.Partitions, :partitions
    live "/topics/:topic/groups", TopicLive.Groups, :groups
    live "/topics/:topic/configs", TopicLive.Configs, :configs
    live "/topics/:topic/logs", TopicLive.Logs, :logs
    live "/topics/:topic/produce", TopicLive.Produce, :produce
    live "/topics/:topic/partitions/:partition", MessageLive.Index, :index
    live "/groups", GroupLive.Index, :index
    live "/groups/:group", GroupLive.Show, :show
  end

  # Other scopes may use custom stacks.
  # scope "/api", AshtailWeb do
  #   pipe_through :api
  # end

  # Enable LiveDashboard in development
  if Application.compile_env(:ashtail, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: AshtailWeb.Telemetry
    end
  end
end
