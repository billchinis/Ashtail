defmodule AshtailWeb.Layouts do
  @moduledoc """
  This module holds layouts and related functionality
  used by your application.
  """
  use AshtailWeb, :html

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
    doc: "the active nav section, so the current item can be marked active"

  slot :inner_block, required: true

  def app(assigns) do
    ~H"""
    <header class="sticky top-0 z-20 bg-base-100/95 backdrop-blur border-b border-base-300 shadow-sm before:block before:h-1 before:bg-gradient-to-r before:from-brand-violet before:to-brand-magenta">
      <nav
        aria-label="Main"
        class="max-w-7xl mx-auto px-4 sm:px-8 h-20 sm:h-24 flex items-center gap-3 sm:gap-10"
      >
        <.link navigate={~p"/"} class="order-1 shrink-0" aria-label="Ashtail home">
          <img
            src={~p"/images/ashtail-wordmark.png"}
            alt="Ashtail"
            width="186"
            height="48"
            class="h-9 sm:h-12 w-auto [[data-theme=dark]_&]:hidden"
          />
          <img
            src={~p"/images/ashtail-wordmark-dark.png"}
            alt="Ashtail"
            width="186"
            height="48"
            class="hidden h-9 sm:h-12 w-auto [[data-theme=dark]_&]:block"
          />
        </.link>

        <div class="dropdown dropdown-end order-3 sm:order-2 sm:static">
          <div
            tabindex="0"
            role="button"
            class="btn btn-ghost btn-sm btn-square sm:hidden"
            aria-label="Menu"
          >
            <.icon name="hero-bars-3" class="size-5" />
          </div>
          <ul
            tabindex="0"
            class="menu dropdown-content z-30 mt-2 w-52 gap-1 rounded-box bg-base-100 p-2 shadow-sm sm:!static sm:!flex sm:!opacity-100 sm:!scale-100 sm:mt-0 sm:flex-row sm:items-center sm:!gap-2 sm:!w-auto sm:!rounded-none sm:!bg-transparent sm:!p-0 sm:!shadow-none"
          >
            <li>
              <.link
                navigate={~p"/"}
                data-nav-topics
                aria-current={@section == :topics && "true"}
                class={nav_link_class(@section == :topics)}
              >
                Topics
              </.link>
            </li>
            <li>
              <.link
                navigate={~p"/groups"}
                data-nav-groups
                aria-current={@section == :groups && "true"}
                class={nav_link_class(@section == :groups)}
              >
                Consumer groups
              </.link>
            </li>
          </ul>
        </div>

        <div class="order-2 sm:order-3 ml-auto">
          <.theme_toggle />
        </div>
      </nav>
    </header>

    <main class="max-w-7xl mx-auto px-4 sm:px-8 pt-8 pb-16">
      {render_slot(@inner_block)}
    </main>

    <.flash_group flash={@flash} />
    """
  end

  # h-10 matches the theme toggle, so the pills and the toggle share a centre line.
  @nav_link_base "inline-flex items-center h-10 px-4 rounded-full text-base sm:text-lg transition "

  defp nav_link_class(true),
    do:
      @nav_link_base <>
        "font-semibold text-white bg-gradient-to-r from-brand-violet to-brand-magenta " <>
        "shadow-sm hover:text-white hover:brightness-110"

  defp nav_link_class(false),
    do:
      @nav_link_base <>
        "font-medium text-base-content/70 hover:bg-base-200 hover:text-base-content"

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
    <div class="card relative flex flex-row items-center h-10 bg-base-200 ring-1 ring-inset ring-base-300 rounded-full">
      <div class="absolute w-1/3 h-full rounded-full bg-gradient-to-r from-brand-violet to-brand-magenta shadow-sm left-0 [[data-theme=light]_&]:left-1/3 [[data-theme=dark]_&]:left-2/3 [[data-theme-source=system]_&]:!left-0 transition-[left]" />

      <button
        class="relative flex items-center justify-center size-10 cursor-pointer"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="system"
        aria-label="System theme"
      >
        <.icon
          name="hero-computer-desktop"
          class="size-5 opacity-75 hover:opacity-100 [[data-theme-source=system]_&]:text-white [[data-theme-source=system]_&]:opacity-100"
        />
      </button>

      <button
        class="relative flex items-center justify-center size-10 cursor-pointer"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="light"
        aria-label="Light theme"
      >
        <.icon
          name="hero-sun"
          class="size-5 opacity-75 hover:opacity-100 [[data-theme=light]:not([data-theme-source=system])_&]:text-white [[data-theme=light]:not([data-theme-source=system])_&]:opacity-100"
        />
      </button>

      <button
        class="relative flex items-center justify-center size-10 cursor-pointer"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="dark"
        aria-label="Dark theme"
      >
        <.icon
          name="hero-moon"
          class="size-5 opacity-75 hover:opacity-100 [[data-theme=dark]:not([data-theme-source=system])_&]:text-white [[data-theme=dark]:not([data-theme-source=system])_&]:opacity-100"
        />
      </button>
    </div>
    """
  end
end
