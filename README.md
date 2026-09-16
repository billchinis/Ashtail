<p align="center">
  <img src="docs/images/ashtail-256.png" alt="Ashtail" width="160">
</p>

# Ashtail

A small web UI for poking at a single Kafka cluster while you debug your own
services. It lists topics, shows partitions, offsets, configs and log dirs,
browses and filters messages (including JSON field conditions), tails topics
live, shows consumer group lag, and can produce a single test message.

It has no database. Everything is read live from the broker.

Built with Phoenix 1.8 / LiveView 1.2, [brod](https://github.com/kafka4beam/brod),
Tailwind 4 and daisyUI 5.

## Quick start

Requirements: Elixir 1.17+, Docker (for the local Redpanda broker), Node.js
(only for the screenshot pass).

```bash
mix setup          # deps, assets, starts Redpanda and seeds fixture data
mix phx.server     # http://localhost:4000
```

`mix setup` runs `mix kafka.seed`, which brings up the Redpanda container from
`docker-compose.yml` (Kafka on `localhost:19092`) and creates the fixture
topics and consumer groups. The seed script is idempotent, so run it as often
as you like.

## Pointing it at a real cluster

The broker is configured from environment variables at boot:

| Variable | Default (dev/test) | Notes |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:19092` | comma-separated `host:port`; required in prod |
| `KAFKA_CLIENT_ID` | `ashtail` | |
| `KAFKA_CONNECT_TIMEOUT_MS` | `5000` | prod default 10000 |
| `KAFKA_REQUEST_TIMEOUT_MS` | `10000` | prod default 30000 |
| `KAFKA_TLS` | `false` | prod default `true` |
| `KAFKA_SASL_MECHANISM` | unset | `plain`, `scram-sha-256` or `scram-sha-512` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | unset | used when a mechanism is set |

```bash
KAFKA_BROKERS=broker1:9092,broker2:9092 mix phx.server
```

A bad value fails the boot with a message naming the variable.

## Checks

```bash
mix precommit      # compile with warnings as errors, format check, credo --strict, tests
mix screenshots    # full-page screenshots of every route into tmp/shots (Playwright)
```

Most tests talk to the seeded local broker, so run `mix kafka.seed` first.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the pieces fit
together.
