defmodule KafkaManagerWeb.Layouts do
  @moduledoc """
  This module holds layouts and related functionality
  used by your application.
  """
  use KafkaManagerWeb, :html

  # Embed all files in layouts/* within this module.
  # The default root.html.heex file contains the HTML
  # skeleton of your application, namely HTML headers
  # and other static content.
  embed_templates "layouts/*"

  @doc """
  Renders your app layout.

  This function is typically invoked from every template,
  and it often contains your application menu, sidebar,
  or similar.

  ## Examples

      <Layouts.app flash={@flash}>
        <h1>Content</h1>
      </Layouts.app>

  """
  attr :flash, :map, required: true, doc: "the map of flash messages"

  attr :current_scope, :map,
    default: nil,
    doc: "the current [scope](https://phoenix.hexdocs.pm/scopes.html)"

  attr :section, :atom,
    default: nil,
    values: [nil, :topics, :groups],
    doc: "the active sidebar/dock section, so the current item can be marked active"

  slot :inner_block, required: true

  def app(assigns) do
    ~H"""
    <div class="lg:flex lg:h-screen bg-base-100 text-base-content">
      <aside class="hidden lg:flex lg:flex-col lg:w-56 lg:shrink-0 bg-base-200 border-r border-base-300 lg:sticky lg:top-0 lg:h-screen">
        <div class="h-14 px-4 flex items-center gap-2">
          <.icon name="hero-circle-stack" class="size-5" />
          <span class="text-base font-semibold">KafkaManager</span>
        </div>
        <ul class="menu w-full">
          <li class="menu-title">Cluster</li>
          <li>
            <.link navigate={~p"/"} data-nav-topics class={[@section == :topics && "menu-active"]}>
              <.icon name="hero-queue-list" class="size-4" /> Topics
            </.link>
          </li>
          <li>
            <.link
              navigate={~p"/groups"}
              data-nav-groups
              class={[@section == :groups && "menu-active"]}
            >
              <.icon name="hero-user-group" class="size-4" /> Consumer groups
            </.link>
          </li>
        </ul>
        <div class="mt-auto p-4">
          <.theme_toggle />
        </div>
      </aside>

      <div class="flex flex-col min-w-0 lg:flex-1 lg:min-h-0">
        <div class="lg:hidden navbar min-h-12 bg-base-200 border-b border-base-300 sticky top-0 z-20 px-4">
          <div class="flex-1 flex items-center gap-2">
            <.icon name="hero-circle-stack" class="size-5" />
            <span class="text-base font-semibold">KafkaManager</span>
          </div>
          <.theme_toggle />
        </div>

        <main class="bg-base-100 px-4 py-4 pb-20 lg:px-6 lg:py-5 lg:pb-5 lg:flex-1 lg:flex lg:flex-col lg:min-h-0">
          {render_slot(@inner_block)}
        </main>

        <div class="dock dock-sm bg-base-200 border-t border-base-300 fixed bottom-0 inset-x-0 z-20 lg:hidden">
          <.link navigate={~p"/"} class={[@section == :topics && "dock-active"]}>
            <.icon name="hero-queue-list" class="size-5" />
            <span class="dock-label">Topics</span>
          </.link>
          <.link navigate={~p"/groups"} class={[@section == :groups && "dock-active"]}>
            <.icon name="hero-user-group" class="size-5" />
            <span class="dock-label">Groups</span>
          </.link>
        </div>
      </div>
    </div>

    <.flash_group flash={@flash} />
    """
  end

  @doc """
  Shows the flash group with standard titles and content.

  ## Examples

      <.flash_group flash={@flash} />
  """
  attr :flash, :map, required: true, doc: "the map of flash messages"
  attr :id, :string, default: "flash-group", doc: "the optional id of flash container"

  def flash_group(assigns) do
    ~H"""
    <div id={@id} aria-live="polite">
      <.flash kind={:info} flash={@flash} />
      <.flash kind={:error} flash={@flash} />

      <.flash
        id="client-error"
        kind={:error}
        title="We can't find the internet"
        phx-disconnected={
          show(".phx-client-error #client-error")
          |> JS.remove_attribute("hidden", to: ".phx-client-error #client-error")
        }
        phx-connected={hide("#client-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        Attempting to reconnect
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>

      <.flash
        id="server-error"
        kind={:error}
        title="Something went wrong!"
        phx-disconnected={
          show(".phx-server-error #server-error")
          |> JS.remove_attribute("hidden", to: ".phx-server-error #server-error")
        }
        phx-connected={hide("#server-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        Attempting to reconnect
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>
    </div>
    """
  end

  @doc """
  Provides dark vs light theme toggle based on themes defined in app.css.

  See <head> in root.html.heex which applies the theme before page load.
  """
  def theme_toggle(assigns) do
    ~H"""
    <div class="card relative flex flex-row items-center border-2 border-base-300 bg-base-300 rounded-full">
      <div class="absolute w-1/3 h-full rounded-full border-1 border-base-200 bg-base-100 brightness-200 left-0 [[data-theme=light]_&]:left-1/3 [[data-theme=dark]_&]:left-2/3 [[data-theme-source=system]_&]:!left-0 transition-[left]" />

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="system"
      >
        <.icon name="hero-computer-desktop-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="light"
      >
        <.icon name="hero-sun-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="dark"
      >
        <.icon name="hero-moon-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>
    </div>
    """
  end
end
