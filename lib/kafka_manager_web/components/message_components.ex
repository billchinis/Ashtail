defmodule KafkaManagerWeb.MessageComponents do
  @moduledoc """
  The message list shared by the per-partition browser and, from AC-18
  onward, the Data view (docs/DESIGN.md "Message list", Assumption 5):
  `message_list/1` is the sheet holding the `ul.list` stream container,
  `message_row/1` renders one row's two lines, and `message_key/1`,
  `message_value/1` and `message_headers/1` render its pieces. `pager/1`,
  `tail_control/1` and `page_size_select/1` are the shared toolbar and
  footer pieces, and `iso_timestamp/1` is the pure formatter behind
  `data-timestamp` (docs/PLAN.md 4.2).
  """

  use Phoenix.Component

  import KafkaManagerWeb.CoreComponents, only: [icon: 1]

  @truncate_length 200

  @doc """
  Renders a message's key. A `nil` key (AC-8) renders a `<null>` marker
  instead of an empty element. The clamp lifts while the row's value is
  expanded (DESIGN.md Assumption 6), so the key can be read in full
  alongside the expanded value.
  """
  attr :key, :any, default: nil
  attr :expanded?, :boolean, default: false

  def message_key(assigns) do
    ~H"""
    <span
      :if={@key == nil}
      data-null-key
      class="self-start badge badge-sm badge-ghost font-mono text-base-content/60 w-fit"
    >&lt;null&gt;</span>
    <span
      :if={@key != nil}
      class={[
        "self-start font-mono text-sm font-medium break-all",
        not @expanded? && "line-clamp-3"
      ]}
      title={@key}
    >{@key}</span>
    """
  end

  @doc """
  Renders a message's value. Values longer than the truncation length
  (AC-8) render behind a server-side expander (docs/PLAN.md 4.1, P4)
  instead of in full. `partition` is set only where the caller's expand
  and collapse events are keyed by `{partition, offset}` (the Data view). An
  empty string (fix run item 6) renders a muted `(empty)` marker instead of
  a blank block, in the same spirit as `message_key/1`'s `<null>` marker but
  without claiming the value was null: the broker gives no way to tell a
  Kafka null value apart from an empty string (docs/DECISIONS.md).
  """
  attr :value, :string, required: true
  attr :expanded?, :boolean, default: false
  attr :offset, :integer, required: true
  attr :partition, :any, default: nil

  def message_value(assigns) do
    assigns = assign(assigns, :long?, String.length(assigns.value) > @truncate_length)

    ~H"""
    <div>
      <span
        :if={not @long? and @value == ""}
        class="block bg-base-200 rounded-lg px-3 py-1 font-mono text-sm leading-5 italic text-base-content/40"
      >(empty)</span>
      <span
        :if={not @long? and @value != ""}
        class="block bg-base-200 rounded-lg px-3 py-1 font-mono text-sm leading-5 whitespace-pre-wrap break-all"
      >{@value}</span>
      <span
        :if={@long? and @expanded?}
        data-value-full
        class="block max-h-[32rem] overflow-y-auto bg-base-200 rounded-lg px-3 py-1 font-mono text-sm leading-5 whitespace-pre-wrap break-all"
      >{@value}</span>
      <button
        :if={@long? and @expanded?}
        type="button"
        data-collapse-value
        class="btn btn-xs btn-ghost mt-1"
        phx-click="collapse_value"
        phx-value-offset={@offset}
        phx-value-partition={@partition}
      >
        <.icon name="hero-chevron-up" class="size-3" /> Collapse
      </button>
      <span
        :if={@long? and not @expanded?}
        data-value-preview
        class="block bg-base-200 rounded-lg px-3 py-1 font-mono text-sm leading-5 whitespace-pre-wrap break-all"
      >{truncate(@value)}</span>
      <button
        :if={@long? and not @expanded?}
        type="button"
        data-expand-value
        class="btn btn-xs btn-ghost mt-1"
        phx-click="expand_value"
        phx-value-offset={@offset}
        phx-value-partition={@partition}
      >
        <.icon name="hero-chevron-down" class="size-3" /> Expand
      </button>
    </div>
    """
  end

  defp truncate(value), do: String.slice(value, 0, @truncate_length) <> "…"

  @doc """
  Renders a message's headers as `name=value` pills, pushed to the right
  of the meta line (DESIGN.md "Message list").
  """
  attr :headers, :list, required: true

  def message_headers(assigns) do
    ~H"""
    <div class="ml-auto flex flex-wrap gap-1.5 max-sm:basis-full max-sm:ml-0">
      <span
        :for={{name, value} <- @headers}
        class="message-header rounded-full bg-base-200 px-2 py-0.5 text-xs text-base-content/70 break-all"
      >{name}={value}</span>
    </div>
    """
  end

  @doc """
  Renders one message row's content: the meta line (partition, offset,
  timestamp, headers) and the key/value line (DESIGN.md "Message list").
  It is rendered inside `message_list/1`'s stream `<li>`; it does not
  render the `<li>` itself. `show_partition` is false on the per-partition
  browser, which leaves the partition out because its `h1` already names
  it (DESIGN.md Assumption 5).
  """
  attr :message, :map, required: true
  attr :show_partition, :boolean, default: false

  def message_row(assigns) do
    ~H"""
    <div>
      <div class="flex flex-wrap items-center gap-x-2 sm:gap-x-3 gap-y-0.5 sm:gap-y-1 text-xs font-mono">
        <span :if={@show_partition}>
          <span class="text-base-content/50">partition</span>
          <span class="text-base-content">{@message.partition}</span>
        </span>
        <span>
          <span class="text-base-content/50">offset</span>
          <span class="text-base-content">{@message.offset}</span>
        </span>
        <span class="text-base-content/60">{@message.timestamp}</span>
        <.message_headers :if={@message.headers != []} headers={@message.headers} />
      </div>
      <div class="mt-1 sm:mt-1.5 grid gap-x-6 gap-y-0.5 sm:gap-y-1 sm:grid-cols-[18rem_minmax(0,1fr)]">
        <.message_key key={@message.key} expanded?={@message.expanded?} />
        <.message_value
          value={@message.value}
          expanded?={@message.expanded?}
          offset={@message.offset}
          partition={@show_partition && @message.partition}
        />
      </div>
    </div>
    """
  end

  @doc """
  Renders the message list sheet: a `card` holding `ul.list`, the stream
  container, with a stream-safe empty state as its first child, an
  optional indeterminate scan progress bar (`running`), and an optional
  footer slot for the pager (docs/DESIGN.md "Message list").
  """
  attr :id, :string, required: true
  attr :rows, :any, required: true, doc: "the :messages stream"
  attr :row_id, :any, default: nil, doc: "dom id fn, defaults to unwrapping the stream tuple"

  attr :row_item, :any,
    default: nil,
    doc: "row unwrap fn, defaults to unwrapping the stream tuple"

  attr :row_attrs, :any,
    default: nil,
    doc: "fn from row item to a map of extra <li> attributes (the P5 test hooks)"

  attr :running, :boolean, default: false, doc: "shows the indeterminate scan progress bar"

  slot :empty, required: true
  slot :inner_block, required: true, doc: ":let={message}, rendered with message_row/1"
  slot :footer

  def message_list(assigns) do
    stream? = is_struct(assigns.rows, Phoenix.LiveView.LiveStream)

    assigns =
      assigns
      |> assign(:row_id, assigns.row_id || (stream? && fn {id, _item} -> id end))
      |> assign(
        :row_item,
        assigns.row_item ||
          if(stream?, do: fn {_id, item} -> item end, else: &Function.identity/1)
      )
      |> assign(:row_attrs, assigns.row_attrs || fn _item -> %{} end)

    ~H"""
    <div class="card bg-base-100 shadow-sm overflow-hidden">
      <progress
        :if={@running}
        class="progress progress-primary h-0.5 w-full rounded-none"
      ></progress>
      <ul id={@id} phx-update="stream" class="list">
        <li
          id={@id <> "-empty"}
          class="hidden only:block px-5 py-16 text-center text-sm text-base-content/60"
        >
          <.icon name="hero-inbox" class="size-6 mx-auto mb-2 opacity-60" />
          <div>{render_slot(@empty)}</div>
        </li>
        <li
          :for={row <- @rows}
          id={@row_id.(row)}
          class="list-row px-4 sm:px-5 py-2 sm:py-2.5"
          {@row_attrs.(@row_item.(row))}
        >
          <div class="list-col-grow">
            {render_slot(@inner_block, @row_item.(row))}
          </div>
        </li>
      </ul>
      <div :if={@footer != []} class="border-t border-base-300 px-5 py-3">
        {render_slot(@footer)}
      </div>
    </div>
    """
  end

  @doc """
  The right-aligned pager `join` under the message list sheet. Labels and
  the `data-*` hooks are attrs, because the per-partition browser reads
  "Previous page"/"Next page" (ascending offsets) while the Data view
  reads "Newer"/"Older" (DESIGN.md Assumption 8) on the same two hooks.
  A `nil` patch hides that control and leaves no gap.
  """
  attr :prev_patch, :string, default: nil
  attr :next_patch, :string, default: nil
  attr :prev_label, :string, default: "Previous page"
  attr :next_label, :string, default: "Next page"

  def pager(assigns) do
    ~H"""
    <div class="flex justify-end">
      <div class="join">
        <.link
          :if={@prev_patch}
          data-prev-page
          class="join-item btn btn-sm btn-ghost phx-click-loading:opacity-60"
          patch={@prev_patch}
        >
          <.icon name="hero-chevron-left" class="size-4" /> {@prev_label}
        </.link>
        <.link
          :if={@next_patch}
          data-next-page
          class="join-item btn btn-sm btn-ghost phx-click-loading:opacity-60"
          patch={@next_patch}
        >
          {@next_label} <.icon name="hero-chevron-right" class="size-4" />
        </.link>
      </div>
    </div>
    """
  end

  @doc """
  The tail toggle button and the `data-tail` live/off indicator
  (docs/PLAN.md 4.5, 4.10; DESIGN.md "Status and toolbar row"), shared by
  the per-partition browser and the Data view.
  """
  attr :tailing?, :boolean, required: true

  def tail_control(assigns) do
    ~H"""
    <div class="flex items-center gap-2">
      <button
        type="button"
        data-tail-toggle
        class={[
          "btn btn-sm phx-click-loading:opacity-60",
          @tailing? && "btn-soft btn-success",
          not @tailing? && "btn-ghost"
        ]}
        phx-click="toggle_tail"
      >
        <.icon name={if @tailing?, do: "hero-pause", else: "hero-play"} class="size-4" />
        {if @tailing?, do: "Stop tailing", else: "Start tailing"}
      </button>
      <span
        data-tail={if @tailing?, do: "live", else: "off"}
        class={[
          "badge badge-sm badge-soft",
          @tailing? && "badge-success",
          not @tailing? && "badge-ghost"
        ]}
      >
        <span :if={@tailing?} class="status status-success animate-pulse" />
        {if @tailing?, do: "live", else: "off"}
      </span>
    </div>
    """
  end

  @doc """
  The "Per page" page-size select (docs/DESIGN.md "Status and toolbar
  row"), a self-submitting form on `phx-change="page_size"`.
  """
  attr :page_size, :integer, required: true
  attr :id, :string, default: "page-size-form"

  def page_size_select(assigns) do
    ~H"""
    <form id={@id} data-page-size-form phx-change="page_size" class="flex items-center gap-2">
      <label for="page_size" class="text-xs text-base-content/60">Per page</label>
      <select id="page_size" name="page_size" class="select select-sm w-20">
        <option value="20" selected={@page_size == 20}>20</option>
        <option value="50" selected={@page_size == 50}>50</option>
      </select>
      <span class="loading loading-spinner loading-xs hidden phx-change-loading:inline-block" />
    </form>
    """
  end

  @doc """
  `YYYY-MM-DDTHH:MM:SS.sssZ`, always three fractional digits
  (docs/PLAN.md 4.2). Pure: it never touches the visible timestamp text,
  which stays exactly as it renders today.
  """
  def iso_timestamp(%DateTime{} = timestamp) do
    timestamp
    |> DateTime.truncate(:millisecond)
    |> DateTime.to_iso8601()
  end
end
