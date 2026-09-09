#!/usr/bin/env bash
# Idempotent Kafka fixtures for the local Redpanda container (docker-compose.yml).
# Safe to run repeatedly: topics are created only if missing, messages are
# produced only into empty topics, consumer groups only if they do not exist.
#
# Every page must have content, so this covers:
#   - topics with 1, 2, 3, 6 and 12 partitions, an empty topic, a compacted topic
#     with non-default config, a topic with a very long name and 2 KB values
#   - enough messages to paginate (orders: 600)
#   - keys, headers, null keys, JSON and plain-text values
#   - consumer groups with lag 0, with lag, and with a live member (Stable)
set -euo pipefail
cd "$(dirname "$0")/../.."

rpk() { docker compose exec -T redpanda rpk "$@"; }

topic_exists() { rpk topic list 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "$1"; }
topic_message_count() {
  rpk topic describe "$1" -p 2>/dev/null | awk 'NR>1 && $1 ~ /^[0-9]+$/ {sum += $NF} END {print sum + 0}'
}
group_exists() { rpk group list 2>/dev/null | awk 'NR>1 {print $2}' | grep -qx "$1"; }

ensure_topic() { # name partitions [config...]
  local name="$1" parts="$2"; shift 2
  if topic_exists "$name"; then echo "topic $name: exists"; return; fi
  local args=()
  for kv in "$@"; do args+=(-c "$kv"); done
  rpk topic create "$name" -p "$parts" -r 1 "${args[@]}" >/dev/null
  echo "topic $name: created ($parts partitions)"
}

# produce_lines TOPIC HEADER... < lines formatted as "key<TAB>value"
produce_lines() {
  local topic="$1"; shift
  local hargs=()
  for h in "$@"; do hargs+=(-H "$h"); done
  rpk topic produce "$topic" -f '%k\t%v\n' "${hargs[@]}" >/dev/null
}

order_status() {
  case $(($1 % 4)) in
    0) echo placed ;;
    1) echo paid ;;
    2) echo shipped ;;
    *) echo cancelled ;;
  esac
}

LONG_TOPIC="audit-log-with-a-very-long-topic-name-that-stresses-table-layout-and-headers-0123456789"

ensure_topic orders 6
ensure_topic payments 3
ensure_topic notifications 12
ensure_topic events.compacted 2 cleanup.policy=compact retention.ms=86400000 segment.ms=60000
ensure_topic "$LONG_TOPIC" 1 retention.bytes=104857600
ensure_topic empty-topic 2

if [ "$(topic_message_count orders)" -eq 0 ]; then
  for i in $(seq 1 600); do
    printf 'order-%04d\t{"id":%d,"customer":"customer-%03d","total":%d.%02d,"status":"%s"}\n' \
      "$i" "$i" $((i % 97)) $((i * 7 % 500)) $((i % 100)) "$(order_status "$i")"
  done | produce_lines orders "content-type:application/json" "source:seed"
  echo "orders: produced 600 messages"
fi

if [ "$(topic_message_count payments)" -eq 0 ]; then
  for i in $(seq 1 120); do
    method=sepa
    [ $((i % 3)) -eq 0 ] && method=card
    printf 'pay-%03d\t{"order":"order-%04d","amount":%d.00,"currency":"EUR","method":"%s"}\n' \
      "$i" "$i" $((i * 13 % 900 + 1)) "$method"
  done | produce_lines payments "content-type:application/json"
  echo "payments: produced 120 messages"
fi

if [ "$(topic_message_count notifications)" -eq 0 ]; then
  # Plain-text values, no key: exercises null-key rendering across 12 partitions.
  for i in $(seq 1 48); do
    printf 'Notification %d: your order has been updated. This is a plain text body, not JSON.\n' "$i"
  done | rpk topic produce notifications -H "channel:email" >/dev/null
  echo "notifications: produced 48 messages"
fi

if [ "$(topic_message_count events.compacted)" -eq 0 ]; then
  # Repeated keys so compaction has something to do; latest value wins.
  for round in 1 2 3; do
    for i in $(seq 1 20); do
      printf 'user-%02d\t{"round":%d,"name":"User %d","email":"user%d@example.com"}\n' "$i" "$round" "$i" "$i"
    done
  done | produce_lines events.compacted "content-type:application/json"
  echo "events.compacted: produced 60 messages"
fi

if [ "$(topic_message_count "$LONG_TOPIC")" -eq 0 ]; then
  long_value="$(head -c 2048 /dev/zero | tr '\0' 'x')"
  long_key="$(printf 'a-very-long-key-%0200d' 0)"
  for i in $(seq 1 40); do
    printf '%s-%d\t{"n":%d,"blob":"%s","note":"unbroken long value to stress wrapping"}\n' \
      "$long_key" "$i" "$i" "$long_value"
  done | produce_lines "$LONG_TOPIC" "trace-id:00000000-0000-0000-0000-000000000000" "x-very-long-header-name-for-layout:value"
  echo "$LONG_TOPIC: produced 40 messages"
fi

# Consumer groups. Consuming N records with a group commits offsets and exits.
ensure_group() { # group topic count
  if group_exists "$1"; then echo "group $1: exists"; return; fi
  rpk topic consume "$2" -g "$1" -n "$3" -o start >/dev/null
  echo "group $1: created (consumed $3 from $2)"
}
ensure_group orders-service orders 600     # lag 0
ensure_group lagging-analytics orders 50   # lag 550
ensure_group payments-worker payments 90   # lag 30

# A group with a live member, so at least one group reports state Stable.
if ! rpk group describe live-tailer 2>/dev/null | grep -q 'STATE *Stable'; then
  docker compose exec -d redpanda sh -c 'rpk topic consume notifications -g live-tailer -o start >/dev/null 2>&1'
  echo "group live-tailer: started live consumer"
fi

echo "kafka seed: done"
