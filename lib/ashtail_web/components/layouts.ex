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
      <nav aria-label="Main" class="max-w-7xl mx-auto px-4 sm:px-8 h-16 sm:h-20 flex items-center">
        <%!-- The nav and theme controls sit with their bottom edges on the
             wordmark's bottom edge, which is where its letters end. Box edges,
             not text baselines, so it doesn't depend on font rendering. --%>
        <div class="flex flex-1 items-end gap-3 sm:gap-10">
          <.link
            navigate={~p"/"}
            class="order-1 shrink-0"
            aria-label="Ashtail home"
          >
            <img
              src={~p"/images/ashtail-wordmark.png"}
              alt="Ashtail"
              width="186"
              height="48"
              class="block h-9 sm:h-11 w-auto [[data-theme=dark]_&]:hidden"
            />
            <img
              src={~p"/images/ashtail-wordmark-dark.png"}
              alt="Ashtail"
              width="186"
              height="48"
              class="hidden h-9 sm:h-11 w-auto [[data-theme=dark]_&]:block"
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
              class="menu dropdown-content z-30 mt-2 w-52 gap-1 rounded-box bg-base-100 p-2 shadow-sm sm:!static sm:!flex sm:!opacity-100 sm:!scale-100 sm:mt-0 sm:flex-row sm:items-center sm:!gap-0 sm:!w-auto sm:!rounded-full sm:!bg-base-200 sm:!p-1 sm:!shadow-none sm:ring-1 sm:ring-inset sm:ring-base-300"
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
        </div>
      </nav>
    </header>

    <main class="max-w-7xl mx-auto px-4 sm:px-8 pt-8 pb-16">
      {render_slot(@inner_block)}
    </main>

    <.flash_group flash={@flash} />
    """
  end

  # On desktop the links are the segments of a control styled like the theme
  # toggle: 32px segments in a 40px track, the current one on a raised thumb
  # with violet text. In the mobile dropdown the current one gets a soft fill.
  @nav_link_base "text-base transition-colors " <>
                   "sm:flex sm:items-center sm:h-8 sm:!py-0 sm:!px-4 sm:rounded-full " <>
                   "sm:text-sm sm:border sm:border-transparent "

  defp nav_link_class(true),
    do:
      @nav_link_base <>
        "font-semibold text-primary bg-base-200 " <>
        "sm:bg-base-100 sm:dark:bg-base-300 sm:shadow-sm sm:border-base-300 " <>
        "sm:hover:bg-base-100 sm:dark:hover:bg-base-300 sm:focus:!bg-base-100"

  defp nav_link_class(false),
    do:
      @nav_link_base <>
        "font-medium text-base-content/60 hover:text-base-content " <>
        "sm:hover:bg-transparent sm:active:!bg-transparent sm:focus:!bg-transparent"

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
    <div class="card relative flex flex-row items-center gap-0 p-1 bg-base-200 ring-1 ring-inset ring-base-300 rounded-full">
      <%!-- The raised thumb behind the chosen option, as in a segmented control. --%>
      <div class="absolute top-1 left-1 size-8 rounded-full bg-base-100 dark:bg-base-300 shadow-sm ring-1 ring-base-300 transition-transform duration-200 [[data-theme=light]_&]:translate-x-8 [[data-theme=dark]_&]:translate-x-16 [[data-theme-source=system]_&]:!translate-x-0" />

      <button
        class="relative flex items-center justify-center size-8 rounded-full cursor-pointer"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="system"
        aria-label="System theme"
      >
        <.icon
          name="hero-computer-desktop-mini"
          class="size-4 text-base-content/50 hover:text-base-content [[data-theme-source=system]_&]:text-primary"
        />
      </button>

      <button
        class="relative flex items-center justify-center size-8 rounded-full cursor-pointer"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="light"
        aria-label="Light theme"
      >
        <.icon
          name="hero-sun-mini"
          class="size-4 text-base-content/50 hover:text-base-content [[data-theme=light]:not([data-theme-source=system])_&]:text-primary"
        />
      </button>

      <button
        class="relative flex items-center justify-center size-8 rounded-full cursor-pointer"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="dark"
        aria-label="Dark theme"
      >
        <.icon
          name="hero-moon-mini"
          class="size-4 text-base-content/50 hover:text-base-content [[data-theme=dark]:not([data-theme-source=system])_&]:text-primary"
        />
      </button>
    </div>
    """
  end
end
