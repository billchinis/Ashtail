# Architecture

This document walks through Ashtail from boot to pixels: what starts,
how a request reaches the broker, how the message views read and filter data,
and how the test and fixture tooling hangs together.

- [1. Overview](#1-overview)
- [2. Features](#2-features)
- [3. Project layout](#3-project-layout)
- [4. Dependencies](#4-dependencies)
- [5. Configuration](#5-configuration)
- [6. Boot and supervision](#6-boot-and-supervision)
- [7. The Kafka layer](#7-the-kafka-layer)
- [8. The Data view read engine](#8-the-data-view-read-engine)
- [9. Filtering](#9-filtering)
- [10. The web layer](#10-the-web-layer)
- [11. UI and styling](#11-ui-and-styling)
- [12. Local broker and fixtures](#12-local-broker-and-fixtures)
- [13. Testing](#13-testing)
- [14. Screenshots](#14-screenshots)
- [15. Day-to-day workflow](#15-day-to-day-workflow)
- [16. Gotchas](#16-gotchas)
- [17. Known limitations](#17-known-limitations)

---

## 1. Overview

```
Browser ──websocket──▶ LiveView (lib/ashtail_web/live/*)
                           │  only ever calls
                           ▼
                  Ashtail.Kafka  (facade)
                           │
      ┌──────────┬─────────┼──────────┬──────────────┐
    Topics    Messages   Groups   TopicReader     Filter / JsonPath
      └──────────┴─────────┴──────────┘               (pure)
                           │
                  Ashtail.Kafka.Client   ◀── the only module that names :brod / :kpro
                           │  short-lived connections, run in supervised tasks
                           ▼
                      Kafka / Redpanda
```

There is no database and no cache. Every page load asks the broker. That keeps
the app honest (what you see is what the cluster says right now) at the cost of
a round trip per view, which is fine for a debugging tool.

Out of scope on purpose: multiple clusters, schema registry, Kafka Connect,
ACLs, offset resets, topic creation/deletion.

## 2. Features

| Screen | Route | What it does |
| --- | --- | --- |
| Topic list | `/` | Name, partitions, replication factor, message count. Case-insensitive name search (`q`), pagination with 20/50 per page. Click any column header to sort by it (click again to flip direction; default name A–Z); on mobile a "Sort by" menu does the same. Internal topics are hidden. |
| Topic → Data | `/topics/:topic` | Messages from **all** partitions merged newest-first by timestamp. Key / value / header filters (text or regex), partition filter, From/To time range, any number of JSON field conditions. Bounded scans with a progress bar, **Stop** and **Scan more**. Live tail. Cursor-based paging. |
| Topic → Partitions | `/topics/:topic/partitions` | Per-partition leader, replicas, earliest/latest offset, message count. |
| Topic → Consumer Groups | `/topics/:topic/groups` | Groups that committed on the topic or have a live member assigned to it, with lag. |
| Topic → Configs | `/topics/:topic/configs` | DescribeConfigs output. |
| Topic → Logs | `/topics/:topic/logs` | DescribeLogDirs per replica broker: log dir, size, offset lag. |
| Produce | `/topics/:topic/produce` | Send one message: key (or null key), value, headers, partition (or auto), optional timestamp. Shows partition/offset written. |
| Partition browser | `/topics/:topic/partitions/:partition` | Reads one partition from a chosen offset, pages forwards/backwards, live tail. |
| Consumer groups | `/groups` | State, members, total lag, expandable per-partition lag. Paginated (20/50 per page) and sortable by any column like the topic list; default group id A–Z. |
| Group detail | `/groups/:group` | Per-partition committed offset, latest offset, lag. |

Every screen degrades to an error card ("Could not reach the broker at …")
when the broker is down; nothing crashes.

## 3. Project layout

```
config/                     compile-time and runtime config
lib/ashtail/
  application.ex            supervision tree, resolves Kafka config at boot
  kafka.ex                  facade used by the web layer
  kafka/
    client.ex               all brod/kpro calls
    config.ex               env var → connection settings
    topics.ex messages.ex groups.ex
    topic_reader.ex         merged multi-partition reader (Data view)
    filter.ex json_path.ex  pure filter parsing and matching
    broker_error.ex topic.ex partition.ex message.ex group.ex   structs
lib/ashtail_web/
  router.ex endpoint.ex route_list.ex
  live/topic_live/*         topic list and the topic sub-pages
  live/message_live/*       per-partition browser
  live/group_live/*         consumer groups
  components/*              shared function components and layouts
lib/mix/tasks/              mix kafka.seed, mix screenshots
priv/kafka/seed.sh          fixture provisioning via rpk
scripts/screenshots.mjs     Playwright screenshot pass
test/                       unit, LiveView and smoke tests
docker-compose.yml          local Redpanda
```

## 4. Dependencies

| Dependency | Why |
| --- | --- |
| `phoenix`, `phoenix_live_view`, `phoenix_html` | Web framework and the whole UI. |
| `brod` | Kafka client. The high-level `:brod` API plus raw `:kpro` requests where brod has no wrapper (ListOffsets per leader, DescribeConfigs, DescribeLogDirs, produce with headers/timestamp). |
| `snappyer` | Snappy codec so compressed batches decode. |
| `bandit` | HTTP server. |
| `tailwind`, `esbuild` | Asset pipeline (Tailwind 4, esbuild). |
| `daisyui`, `heroicons` | Pulled from GitHub as uncompiled deps and loaded as Tailwind plugins. |
| `jason` | JSON (also used to decode message values for JSON conditions). |
| `telemetry_*`, `phoenix_live_dashboard` | Metrics, `/dev/dashboard` in dev. |
| `dns_cluster` | Standard Phoenix generator dependency. |
| `credo`, `dialyxir` | Lint and type checks (dev/test only). |
| `lazy_html` | HTML parsing for LiveView tests. |
| `playwright` (npm) | Screenshot pass only. |
| `@fontsource-variable/*` (npm) | One-time source for the self-hosted `.woff2` fonts in `priv/static/fonts`. |

## 5. Configuration

### Kafka

Kafka settings are **not** in `runtime.exs`. They're resolved by
`Ashtail.Kafka.Config` when the application starts, from environment
variables layered over per-environment defaults (`config :ashtail,
:kafka_defaults` in `dev.exs`, `test.exs`, `prod.exs`).

| Variable | Dev / test | Prod |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:19092` | required, boot raises if missing |
| `KAFKA_CLIENT_ID` | `ashtail` | `ashtail` |
| `KAFKA_CONNECT_TIMEOUT_MS` | 5000 | 10000 |
| `KAFKA_REQUEST_TIMEOUT_MS` | 10000 | 30000 |
| `KAFKA_TLS` | `false` | `true` |
| `KAFKA_SASL_MECHANISM` | none | none (`plain`, `scram-sha-256`, `scram-sha-512`) |
| `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | none | none |

`Config.resolve/2` is a pure function (env map + defaults → struct or error)
so it's trivially testable. `resolve!/0` reads `System.get_env/0` and raises
with the offending variable's name. `conn_config/1` turns the struct into the
options map brod/kpro expect.

### Phoenix

- `dev.exs` binds the endpoint to `0.0.0.0:4000` (so it's reachable from the
  Windows host when running under WSL), enables code reload and the asset
  watchers.
- `test.exs` uses port 4002 with `server: false`, and sets three test-only
  knobs read by the Data view:
  `data_scan_budget: 100`, `data_scan_chunk: 1`,
  `data_tail_interval_ms: 3_600_000` (the tail timer effectively never fires
  on its own; tests send `:tail_tick` themselves).
- `runtime.exs` is the standard Phoenix one: `PHX_SERVER`, `PORT`,
  `SECRET_KEY_BASE`, `PHX_HOST`, `DNS_CLUSTER_QUERY`.

## 6. Boot and supervision

`Ashtail.Application.start/2`:

1. `Kafka.Config.resolve!()` — a bad env var stops the boot here with a clear
   message, before anything else starts.
2. The result is stored with `Application.put_env(:ashtail, :kafka_config, …)`.
   `Ashtail.Kafka.config/0` reads it back on every call, which is what lets
   tests swap brokers at runtime.
3. Children (`:one_for_one`):
   - `AshtailWeb.Telemetry`
   - `DNSCluster`
   - `Phoenix.PubSub` (`Ashtail.PubSub`, used by LiveView internals only)
   - `Task.Supervisor` named `Ashtail.Kafka.TaskSupervisor`
   - `AshtailWeb.Endpoint`

There is no long-lived Kafka client process.

## 7. The Kafka layer

### Facade: `Ashtail.Kafka`

The only module the web layer touches. Each function injects the current
config and delegates:

| Function | Delegates to |
| --- | --- |
| `list_topics/1`, `get_topic/1`, `topic_summary/1`, `topic_log_dirs/1` | `Topics` |
| `fetch_messages/4`, `produce/3` | `Messages` |
| `list_groups/0`, `get_group/1`, `topic_groups/1` | `Groups` |
| `read_topic/2` | `TopicReader.read/3` |
| `parse_filter/1` | `Filter.parse/1` |

Every broker-facing function returns `{:ok, value}` or
`{:error, %BrokerError{address, reason, message}}`. One error shape keeps the
LiveViews simple.

### `Client`: connections and protocol

`Client` is the only module that names `:brod`, `:kpro` or the Erlang record
definitions.

**Connection model.** `run/2` executes each operation inside
`Task.Supervisor.async_nolink/2` and waits `connect_timeout + request_timeout + 1s`.
If the task crashes you get a `BrokerError`; if it hangs it's killed and you
get `reason: :timeout`. Raw kpro connections are closed in `after` blocks.
The LiveView process is never linked to a connection, so a dead broker can't
take a page down.

**Operations.**

- `metadata/1` — `:brod.get_metadata` for all topics.
- `list_offsets/3` — `:earliest`, `:latest` or `{:timestamp, ms}`. Partitions
  are grouped **by leader** and one ListOffsets v1 request (read-committed) is
  sent per leader. A timestamp lookup with no match (`-1`) comes back as `nil`;
  negative timestamps are clamped to 0 so they can't collide with the `-1`/`-2`
  sentinels.
- `fetch/5` — `:brod.fetch` with `min_bytes: 0` and a caller-supplied
  `max_bytes`, returning `%Message{}` tagged with the partition. Empty keys
  (`<<>>`, `:undefined`, `:null`) become `nil`.
- `produce/4` — raw `connect_partition_leader` + produce v7, supporting key,
  headers and timestamp.
- `describe_topic_config/2` — DescribeConfigs v1.
- `describe_log_dirs/2` — one DescribeLogDirs request per replica broker,
  version negotiated and capped at 1, future replicas dropped.
- `list_groups/1`, `describe_groups/3`, `fetch_committed_offsets/2` — brod
  wrappers; member assignments are decoded to find which topics a live member
  owns.

Missing leaders/brokers/partitions become readable errors
(`:missing_partition_leader`, `:missing_broker`) instead of a `Map.fetch!`
crash. A few pure helpers (`group_partitions_by_leader/3`,
`parse_list_offsets_response/1`, …) are `@doc false` so tests can hit them
without a broker.

### `Topics`

- `list_topics/2` fetches metadata, drops internal topics, filters by
  substring, sorts (`:sort` is `:name`, `:partitions`, `:replication` or
  `:messages`; `:dir` is `:asc`/`:desc`; ties always fall back to name A–Z),
  clamps the page, then fetches earliest/latest offsets **only for the
  visible page**. The exception is sorting by messages: counts come from
  offsets, so they're fetched for every topic matching the search before
  sorting. Message count is `Σ(latest − earliest)`; on compacted topics
  that's an upper bound, which is the usual convention.
- The topic list keeps `page`, `page_size`, `q`, `sort` and `dir` in the URL.
  A new column starts A–Z for names and largest-first for counts, and any
  sort change goes back to page 1. The table component takes `sort_link`,
  `sort_by` and `sort_dir`; columns with a `sort` key get a header link, an
  `aria-sort` attribute; the active one shows a violet label and a
  direction arrow.
- `get_topic/2` adds configs; `topic_summary/2` is offsets only (used in every
  topic page header).
- Unknown topic → `reason: :unknown_topic`.

### `Messages`

- `fetch_messages/5` backs the per-partition browser: clamp `from_offset` into
  `[earliest, latest]`, read `[start, min(latest, start + limit))`.
- `read_range/5` is the shared fetch loop for a half-open offset range. Kafka
  returns whole record batches, so records outside the range are dropped.
  `max_bytes` starts at 1 MB; an empty fetch short of the end doubles it once
  (8 MB cap), and a second empty fetch ends the range. That handles gaps left
  by compaction and transaction markers.
- `produce/4` writes to the given partition, or partition 0 if none.

### `Groups`

One pipeline serves the global list, a single group and the per-topic view:

1. list groups (with their coordinators)
2. describe them, batched per coordinator
3. fetch metadata
4. fetch committed offsets per group
5. one batched earliest + one batched latest `list_offsets`

Lag is `max(0, latest − committed)`. A partition with no commit is treated as
committed at `earliest`. The per-topic view keeps a group if it has a commit on
the topic **or** a live member assigned to it (so a freshly joined consumer
still shows up).

`list_groups/2` runs that pipeline for every group, then sorts (`:id`,
`:state`, `:members` or `:lag`) and returns one page. Lag has to be known to
sort by it, so all groups are measured, not just the visible page.

### `Listing`

Shared by `Topics` and `Groups`: `sort/4` (value in either direction, ties by
name A–Z) and `paginate/3` (slice a page, clamp the page number, report
totals). On the web side, `AshtailWeb.ListParams` parses `page`,
`page_size` and `sort`/`dir` from the URL and picks the direction for a
header click.

## 8. The Data view read engine

`Ashtail.Kafka.TopicReader.read/3` is the heart of the Data tab. It reads
many partitions and returns one page sorted by timestamp.

### Options

| Option | Meaning |
| --- | --- |
| `page_size` | rows wanted (default 50) |
| `cursor` | `nil` (newest page), `{:before, %{partition => offset}}` (older), `{:after, map}` (newer) |
| `filter` | a `%Filter{}` |
| `max_scanned` | total messages the read may look at (`:infinity` by default) |
| `on_progress` | callback receiving `%{scanned, messages}` after each refill round |
| `scan_chunk` | test-only override of the per-partition chunk size |

### Bounds

1. Partitions in scope: all of them, or just `filter.partition`.
2. `floor` = earliest offset, `ceiling` = latest offset, per partition.
3. If From/To are set, a ListOffsets-by-timestamp is sent for each end
   (To is inclusive, so the lookup uses `to + 1ms`) and the bounds are
   narrowed to `max(earliest, from)` … `min(latest, to)`.

### Merge

It's a lazy k-way merge. Each partition keeps `%{next, buffer, exhausted?}`.

- **Backward** (default and `{:before, _}`): pick the message with the largest
  `{timestamp, partition, offset}`.
- **Forward** (`{:after, _}`, used by the tail and "newer"): pick the smallest,
  then reverse the page at the end so the UI is always newest-first.

A message is only emitted when **every** partition that isn't exhausted has
something buffered. Otherwise a partition with slightly older data could be
skipped past. When that's not the case, every partition that needs data is
refilled:

- chunk size = `page_size` without a filter, 500 with one
- with a finite `max_scanned`, the remaining budget is split fairly:
  `min(chunk, max(1, remaining / partitions_needing_data))`
- the read stops when the page is full, everything is exhausted, or the budget
  can't give each partition at least one more message

Every message read counts toward `scanned`, matched or not.

### Result

```elixir
%{
  messages: [...],                 # newest first
  range: %{p => {low, high}},      # offsets actually covered per partition
  floor: ..., ceiling: ...,
  older: nil | {:before, low_map}, # cursor for the "Older" link
  newer: nil | {:after, high_map}, # cursor for the "Newer" link
  scanned: integer,
  halted: nil | {:match_limit, field}
}
```

Cursors are serialised into the URL as `before=0.120-1.98-2.143` (partition.offset
pairs), so every page is a shareable link.

## 9. Filtering

`Ashtail.Kafka.Filter` is pure: no broker, no process.

### Parsing (`Filter.parse/1`)

Input is the params map from the URL/form. Output is `{:ok, %Filter{}}` or
`{:error, %{field => message}}` so errors render next to the right input.

- **key / value / header** — text mode (default) compiles an escaped,
  case-insensitive regex; regex mode compiles the pattern as written. Patterns
  are compiled once here, not per message. A header value without a header
  name is an error.
- **partition** — integer ≥ 0.
- **from / to** — ISO 8601, truncated to milliseconds; `from > to` is an
  error on `to`.
- **json** — a list of `%{path, op, value}` rows. Blank paths are skipped.
  `op` is `equals` (default), `contains`, `regex` or `exists`. Everything but
  `exists` needs a value. `equals` pre-decodes numeric values; `contains` and
  `regex` compile a regex. Errors are keyed `json-<row id>`.

### Matching (`Filter.match/2`)

All conditions are ANDed and the cheapest run first: time → key → header →
value → JSON.

- A `nil` key never matches a key filter.
- Header: exact name, and the value pattern if one was given.
- JSON: the value is only decoded if its first non-blank byte is `{` or `[`;
  anything else simply doesn't match a JSON condition.
  - `exists` — the path resolves (even to `null`).
  - `equals` — strings exactly, numbers numerically, booleans by their text,
    `null` matches the literal `null`.
  - `contains` / `regex` — against a scalar's text form (`40`, `true`,
    `null`), so `n contains 40` behaves the way you'd expect. Objects and
    arrays never match.

**Catastrophic regexes.** Every regex runs through `:re.run/3` with
`match_limit: 100_000` and `match_limit_recursion: 10_000`. Hitting a limit
returns `{:match_limit, field}`, the reader halts, and the UI says the
pattern needs too much backtracking. A bad pattern can't pin a scheduler.

### JSON paths (`JsonPath`)

A small recursive-descent parser for `a.b[2].c` (a path may start with
`[n]`). Parse errors say what's wrong (empty segment, missing `]`, non-integer
index, …). `fetch/2` walks maps by string key and lists by index.

## 10. The web layer

### Routes

Defined in `router.ex` (see the table in [Features](#2-features)). In dev,
`/dev/dashboard` serves LiveDashboard.

### Conventions shared by every LiveView

- **Load in `handle_params`**, so every state change that matters is in the
  URL and back/forward work.
- **Streams for collections** (`stream_configure` with slug-based DOM ids).
  Large lists never sit in assigns.
- **Errors are data.** Each Kafka call's `{:error, %BrokerError{}}` is put in
  `@broker_error` and rendered by `<.broker_error>`. Pages that make two calls
  keep the first error so the message stays stable.
- **Expand/collapse** re-inserts a single stream item, looked up in a small
  index map kept alongside the stream.
- **`data-*` attributes** (`data-scan-state`, `data-partition`,
  `data-offset`, `data-tail`, `data-broker-error`, …) are the hooks tests
  use; they're part of the contract, so treat them as API.

### `TopicLive.Index`

URL params `q`, `page`, `page_size`. Search and page-size changes
`push_patch` back to page 1.

### `TopicLive.Data`

The biggest LiveView. Flow for one visit:

1. `handle_params` → `DataParams.parse/1` → new `scan_id` (a ref) → cancel any
   running scan → rebuild JSON condition rows → stop the tail if the URL has a
   cursor → `topic_summary` for the header → `load`.
2. `load` parses the filter.
   - **Invalid:** show field errors, clear the list, stop the tail.
   - **No filter:** synchronous `read_topic`, render the page.
   - **Filter:** `start_async({:scan, scan_id}, …)` calls `read_topic` with a
     `max_scanned` budget (100,000 by default) and an `on_progress` callback
     that sends `{:scan_progress, scan_id, progress}` back to the LiveView.
     Matching rows stream in as they're found and the progress bar shows the
     scanned count.
3. Results tagged with an old `scan_id` are ignored, so a stale scan can
   never overwrite a newer one.
4. When the scan finishes, `apply_page` sets the pager cursors and decides
   whether to offer **Scan more** (filter active, not halted, page not full).
   **Scan more** continues from the returned cursor for the remaining rows and
   merges the result. **Stop** cancels the async task.
5. An `{:exit, _}` from the scan is re-raised on purpose: a crash inside the
   reader is a bug, not a broker problem, and should be loud.

**Tail.** Timer-based, no PubSub.

- Toggling the tail on from page 1 remembers the page's high-water offsets
  (`tail_from`) and schedules `:tail_tick` (1 s).
- From an older page it patches back to page 1 first and arms once that read
  completes.
- Each tick reads `{:after, tail_from}` with the current filter (page size and
  budget 500), inserts new rows at the top with `limit: page_size`, trims the
  index, and advances `tail_from`. While a scan is running the tick just
  reschedules.
- A broker error during a tick shows the error card and switches the tail off.

**`TopicLive.DataParams`** is the pure URL ↔ state translator. It coerces
every scalar filter param to a string (so `?key[x]=1` can't crash the page),
normalises JSON rows from either the form's index map or a list, and builds
canonical paths that leave out defaults. It's unit-tested on its own.

### Other LiveViews

- **Partitions / Configs / Groups / Logs** — one or two facade calls each, one
  stream each, all sharing `topic_header/1` (title, stats strip, tab bar,
  Produce button, error card).
- **Produce** — plain assigns (a form, no collection). Header rows can be added
  and removed; partition `auto` means "let the context pick"; the timestamp
  must be ISO 8601.
- **MessageLive.Index** — per-partition browser with `offset`/`page_size` in
  the URL, a non-integer partition returns 404, and its own 1 s tail that
  appends at the bottom with a bounded stream.
- **GroupLive.Index / Show** — group list with inline lag and the detail page.

### `AshtailWeb.RouteList`

Single source of "every page worth visiting". It reads GET routes from the
router, substitutes seed-backed values for path params (`topic: "orders"`,
`partition: "0"`, `group: "orders-service"`) and appends extra URLs that
exercise tricky seed data (the very long topic name, empty topic, lagging
group, filter and JSON-condition URLs). A route with a path param and no
seed value raises, so a new route can't silently escape the smoke test and the
screenshot pass.

## 11. UI and styling

- Tailwind 4 with daisyUI 5 components; no hand-rolled widgets.
- Two themes, **light** and **dark**, with colours taken from the Ashtail
  logo: deep navy text, an indigo-violet primary and a magenta accent. Light
  is white sheets on a faint lavender page; dark is a navy-violet ground with
  a lighter violet primary. Every text/background pair meets WCAG AA. Lists
  use hairline separators and headings are serif. First visit is light regardless of OS
  setting; the toggle stores the choice and a small script in
  `root.html.heex` applies it before paint.
- Branding: the sticky top bar shows the "Ashtail" wordmark
  (`priv/static/images/ashtail-wordmark.png`, with a light-lettered
  `-dark` copy swapped in on the dark theme). The "A" mark alone
  (`ashtail-mark-128.png`) is used for the favicon and touch icon. Sources
  are `docs/images/logo.png` (mark) and `docs/images/logo-text.png`
  (wordmark); the web sizes are resized from them.
- Fonts (Inter, Source Serif 4, JetBrains Mono) are self-hosted from
  `priv/static/fonts`, so the app works offline.
- Shared pieces live in `components/`:
  - `core_components.ex` — flash, button, input, header, table, icon.
  - `layouts.ex` — app shell, nav, theme toggle.
  - `topic_components.ex` — topic header and tab bar (a colocated hook keeps
    the active tab scrolled into view on narrow screens).
  - `message_components.ex` — message row/list, `<null>` key marker, value
    truncation at 200 chars with a server-side expander, `(empty)` marker for
    empty values, pager, tail control, page-size select.
  - `group_components.ex` — state pill and lag display.
  - `broker_components.ex` — the error card.
- Status colours sit on dots and soft badge tints; the words stay in the
  base text colour for contrast.

## 12. Local broker and fixtures

### Redpanda

`docker-compose.yml` runs a single Redpanda node (`v24.2.4`, dev-container
mode) with Kafka on `localhost:19092` and the admin API on 9644.

### `mix kafka.seed`

Runs `docker compose up -d --wait redpanda`, then `priv/kafka/seed.sh`, which
uses `rpk` inside the container (no Kafka client needed on the host). It's
idempotent: topics are created only if missing and recreated only if their
layout or content check fails.

| Topic | Shape | Why it exists |
| --- | --- | --- |
| `orders` | 6 partitions × 100, keys `order-0001…`, JSON values, headers | the "normal" topic |
| `payments` | 3 × 40, nested JSON, every 10th value is not JSON (text, truncated JSON, or a null tombstone), `retention.ms` override | JSON conditions, invalid values, config display |
| `notifications` | 12 × 4, null keys, plain text, `channel` header, spread and timestamped so partitions interleave | merge order, null keys, header filter |
| `events.compacted` | 2 partitions, `cleanup.policy=compact`, 20 keys × 3 | compaction gaps |
| `audit-log-with-a-very-long-topic-name-…` | 1 partition, 200+ char keys, 2 KB values, long header name | layout stress |
| `empty-topic` | 2 partitions, no data | empty states |
| `scratch` | 1 partition, no seeded data | the only topic tests write to |
| `zz-filler-001…053` | filler | pagination (60 topics total) |

| Group | State |
| --- | --- |
| `orders-service` | consumed all of `orders`, lag 0 |
| `lagging-analytics` | consumed 50 of `orders`, lag 550 |
| `payments-worker` | lag 30 on `payments` |
| `live-tailer` | a detached live consumer on `notifications`, so the group is Stable |
| `zz-filler-group-01…21` | offset 0 committed on `events.compacted` via `rpk group seek` (Empty, lag 60 each), so the group list has a second page and equal lags to tie-break |

Tests only produce into `scratch`, so the counts other tests assert on never
drift.

## 13. Testing

```bash
mix kafka.seed
mix test
mix precommit   # compile --warnings-as-errors, format --check-formatted, credo --strict, test
```

### Layout

- `test/ashtail/kafka/` — unit tests for `Config`, `Client` helpers,
  `Topics`, `Messages`, `Groups`, `TopicReader`, `Filter`, `JsonPath`. Most
  are pure and `async: true`; `TopicReader` also has a few regression tests
  against the seeded broker.
- `test/ashtail_web/live/` — one file per user-visible behaviour (topic
  list, pagination, search, Data view filters, JSON conditions, scan
  limit/stop, tail, produce, group pages, broker failures, navigation, …),
  plus focused regression files. Tests that write to the broker or change app
  env are `async: false`.
- `test/smoke_test.exs` — one generated test per `RouteList` URL: GET returns
  200, the LiveView connects, and nothing is logged at warning or error.

### Helpers (`test/support/`)

- **`TcpProxy`** — a tiny GenServer that forwards TCP to the real broker.
  `cut/1` closes the listener and every open socket. The "broker lost mid-tail"
  test points the app at the proxy, starts a tail, produces a probe message,
  cuts the proxy and asserts the error card appears, the tail is off and the
  probe row is still on screen. The shared Redpanda is never stopped.
- **`BrokerHelpers`** — `override_brokers/1` swaps `KAFKA_BROKERS` and the
  resolved config for one test (restored `on_exit`); `produce_probe/3`
  writes a message. The "broker unavailable" test points at a port nothing
  listens on.
- **`AsyncAssertions.eventually/2`** — retry a block every 100 ms for up to
  5 s; used where a real timer is involved.
- **`LiveViewHelpers.assigns/1`** — reads a LiveView's assigns via
  `:sys.get_state/1`, for the one test that needs to see internal tail state.

## 14. Screenshots

`mix screenshots`:

1. seeds the broker and builds assets
2. installs Playwright + Chromium on first run
3. starts the endpoint on `127.0.0.1:4004`
4. runs `scripts/screenshots.mjs` with every `RouteList` URL

The script captures each page full-length at 1280×800 and 390×844 into
`tmp/shots/`, and fails if any page returns non-200, throws, or logs a
console error. It's the quickest way to eyeball every screen after a UI
change, including the mobile layout.

## 15. Day-to-day workflow

1. `mix setup` once.
2. `mix phx.server` and work against the local broker.
3. If a change adds something visible, add a seed case to
   `priv/kafka/seed.sh` so the smoke test and screenshots cover it. If it
   adds a route with a path param, add a value to `RouteList`'s `@params`.
4. `mix precommit` before committing; `mix screenshots` after UI work.

## 16. Gotchas

- **SASL `nil` vs `:undefined`.** kpro only treats the Erlang atom
  `undefined` as "no auth". Passing Elixir `nil` makes every plaintext
  connection try a SASL handshake. `Config.conn_config/1` maps `nil` to
  `:undefined`.
- **daisyUI control sizes.** `app.css` sets `--size-field: 0.21875rem`
  globally, so `-sm` controls are 28px rather than daisyUI's 32px. The JSON
  condition row overrides it below `sm` to keep its controls tappable.
- **WSL clock drift.** Under WSL the clock can step backwards every ~30s
  (systemd-timesyncd vs the Hyper-V time source). `plug_crypto` then rejects
  LiveView tokens "signed in the future", which shows up as intermittent
  session failures in tests. Fix the time sync rather than the tests.
- **`rpk group delete` with a live member.** It succeeds while the consumer is
  still running, and the consumer then silently recreates the group. The seed
  script kills stray consumers before deleting groups.
- **`crc32cer` needs CMake** to build (a brod dependency). Install it before
  `mix deps.get` if the build fails.

## 17. Known limitations

- Message counts are `latest − earliest`, so compacted topics and
  transactional markers overstate them.
- `produce/3` without a partition always writes to partition 0 rather than
  using a partitioner.
- Every page is a fresh broker round trip; very large clusters will feel it on
  the topic list (offsets are fetched only for the visible page to limit this,
  except when sorting by message count, which reads every matching topic's
  offsets).
- Filtered scans are capped (100,000 messages per scan by default); use
  **Scan more** or narrow the time range for bigger topics.
