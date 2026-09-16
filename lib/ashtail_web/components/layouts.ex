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
      <nav aria-label="Main" class="max-w-7xl mx-auto px-4 sm:px-8 h-16 flex items-center">
        <%!-- The nav links and the theme toggle are centred on the wordmark. --%>
        <div class="flex flex-1 items-center gap-3 sm:gap-8">
          <.link navigate={~p"/"} class="shrink-0" aria-label="Ashtail home">
            <img
              src={~p"/images/ashtail-wordmark.png"}
              alt="Ashtail"
              width="376"
              height="96"
              class="block h-7 sm:h-8 w-auto [[data-theme=dark]_&]:hidden"
            />
            <img
              src={~p"/images/ashtail-wordmark-dark.png"}
              alt="Ashtail"
              width="367"
              height="96"
              class="hidden h-7 sm:h-8 w-auto [[data-theme=dark]_&]:block"
            />
          </.link>

          <div class="hidden sm:flex items-center gap-1">
            <.link
              navigate={~p"/"}
              data-nav-topics
              aria-current={@section == :topics && "true"}
              class={nav_link_class(@section == :topics)}
            >
              Topics
            </.link>
            <.link
              navigate={~p"/groups"}
              data-nav-groups
              aria-current={@section == :groups && "true"}
              class={nav_link_class(@section == :groups)}
            >
              Consumer groups
            </.link>
          </div>

          <div class="ml-auto flex items-center gap-2">
            <.theme_toggle />

            <div class="dropdown dropdown-end sm:hidden">
              <div
                tabindex="0"
                role="button"
                class="btn btn-ghost btn-sm btn-square"
                aria-label="Menu"
              >
                <.icon name="hero-bars-3" class="size-5" />
              </div>
              <ul
                tabindex="0"
                class="menu dropdown-content z-30 mt-2 w-52 gap-1 rounded-box bg-base-100 p-2 shadow-sm"
              >
                <li>
                  <.link navigate={~p"/"} class={menu_link_class(@section == :topics)}>
                    Topics
                  </.link>
                </li>
                <li>
                  <.link navigate={~p"/groups"} class={menu_link_class(@section == :groups)}>
                    Consumer groups
                  </.link>
                </li>
              </ul>
            </div>
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

  # Desktop nav links: plain text, the current one on a soft violet pill.
  @nav_link_base "inline-block px-3.5 py-1.5 rounded-full text-base leading-none " <>
                   "transition-colors "

  defp nav_link_class(true),
    do: @nav_link_base <> "font-semibold text-primary bg-primary/10 hover:bg-primary/15"

  defp nav_link_class(false),
    do: @nav_link_base <> "font-medium text-base-content/60 hover:text-base-content"

  # Links in the mobile dropdown menu.
  defp menu_link_class(true), do: "font-semibold text-primary bg-primary/10"
  defp menu_link_class(false), do: "font-medium text-base-content/70"

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
