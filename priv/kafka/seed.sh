#!/usr/bin/env bash
# Idempotent Kafka fixtures for the local Redpanda container (docker-compose.yml).
# Safe to run repeatedly: topics are created only if missing, messages are
# produced only into empty topics, consumer groups only if they do not exist.
# `orders` and `notifications` are recreated (and only then) if their
# per-partition layout does not already match the deterministic shape below.
#
# Every page must have content, so this covers:
#   - topics with 1, 2, 3, 6 and 12 partitions, an empty topic, a compacted topic
#     with non-default config, a topic with a very long name and 2 KB values
#   - enough messages to paginate (orders: 600, plus 53 filler topics)
#   - keys, headers, null keys, JSON and plain-text values
#   - deterministic per-partition message counts for orders and notifications
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

# ensure_topic_config TOPIC key=value: sets the config explicitly (as a
# per-topic override, not a cluster default) unless it is already explicit.
ensure_topic_config() {
  local name="$1" kv="$2" key="${2%%=*}"
  local source
  source=$(rpk topic describe "$name" -c 2>/dev/null | awk -v k="$key" '$1==k {print $NF}')
  if [ "$source" = "DYNAMIC_TOPIC_CONFIG" ]; then
    echo "topic $name: $key already explicit"
    return
  fi
  rpk topic alter-config "$name" --set "$kv" >/dev/null
  echo "topic $name: set $kv"
}

# topic_layout_ok TOPIC PARTITIONS PER_PARTITION: true if the topic has
# exactly PARTITIONS partitions, each holding exactly PER_PARTITION messages.
topic_layout_ok() {
  local name="$1" partitions="$2" per_partition="$3"
  local n=0 count
  while read -r count; do
    n=$((n + 1))
    [ "$count" = "$per_partition" ] || return 1
  done < <(rpk topic describe "$name" -p 2>/dev/null | awk 'NR>1 {print $NF - $(NF-1)}')
  [ "$n" -eq "$partitions" ]
}

# ensure_deterministic_topic NAME PARTITIONS PER_PARTITION: recreates the
# topic only when it exists but its per-partition message layout is wrong
# (e.g. the old key-hash-partitioned data). Never recreates a topic whose
# layout already matches, so re-running the script is a no-op. Exit status
# 0 means the topic was (re)created fresh, 1 means it already had the right
# layout, so callers can decide whether groups pointed at it must be reset.
ensure_deterministic_topic() {
  local name="$1" partitions="$2" per_partition="$3"
  if topic_exists "$name"; then
    if topic_layout_ok "$name" "$partitions" "$per_partition"; then
      echo "topic $name: exists (deterministic layout ok)"
      return 1
    fi
    echo "topic $name: wrong per-partition layout, recreating"
    rpk topic delete "$name" >/dev/null
  fi
  rpk topic create "$name" -p "$partitions" -r 1 >/dev/null
  echo "topic $name: created ($partitions partitions)"
  return 0
}

# kill_stray_consumer PATTERN: best-effort kill of any process in the
# container whose command line contains PATTERN (no ps/pkill in the image,
# so this walks /proc directly). Used to stop a leftover live consumer
# before its group can be deleted.
kill_stray_consumer() {
  docker compose exec -T redpanda sh -c '
    pattern="$1"
    for round in 1 2 3; do
      found=0
      for p in /proc/[0-9]*; do
        pid=${p#/proc/}
        cmd=$(tr "\0" " " < "$p/cmdline" 2>/dev/null) || continue
        case "$cmd" in
          *"$pattern"*)
            found=1
            kill -9 "$pid" 2>/dev/null
            ;;
        esac
      done
      [ "$found" -eq 0 ] && break
      sleep 1
    done
  ' sh "$1" >/dev/null 2>&1 || true
}

# delete_group_if_exists GROUP: drops a consumer group's committed offsets so
# it gets recreated fresh against a topic that was just recreated. Retries
# briefly: a just-killed live member can take a few seconds to be dropped by
# the broker before the group can be deleted.
delete_group_if_exists() {
  local group="$1" attempts=0
  while group_exists "$group"; do
    if rpk group delete "$group" >/dev/null 2>&1; then
      echo "group $group: deleted (topic recreated)"
      return
    fi
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 15 ]; then
      echo "group $group: could not delete after ${attempts}s" >&2
      return 1
    fi
    sleep 1
  done
}

# produce_lines TOPIC HEADER... < lines formatted as "key<TAB>value"
produce_lines() {
  local topic="$1"; shift
  local hargs=()
  for h in "$@"; do hargs+=(-H "$h"); done
  rpk topic produce "$topic" -f '%k\t%v\n' "${hargs[@]}" >/dev/null
}

# produce_lines_to_partition TOPIC PARTITION HEADER... < lines formatted as
# "key<TAB>value". Explicit partition targeting, for deterministic layouts.
produce_lines_to_partition() {
  local topic="$1" partition="$2"; shift 2
  local hargs=()
  for h in "$@"; do hargs+=(-H "$h"); done
  rpk topic produce "$topic" -p "$partition" -f '%k\t%v\n' "${hargs[@]}" >/dev/null
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

if ensure_deterministic_topic orders 6 100; then
  delete_group_if_exists orders-service
  delete_group_if_exists lagging-analytics
fi
ensure_topic payments 3
ensure_topic_config payments retention.ms=604800000
if ensure_deterministic_topic notifications 12 4; then
  kill_stray_consumer "consume notifications -g live-tailer"
  delete_group_if_exists live-tailer
fi
ensure_topic events.compacted 2 cleanup.policy=compact retention.ms=86400000 segment.ms=60000
ensure_topic "$LONG_TOPIC" 1 retention.bytes=104857600
ensure_topic empty-topic 2
ensure_topic scratch 1

# 53 filler topics so the list holds 60 topics and both page sizes (20, 50)
# have a next page. Created in a single `rpk topic create` call.
filler_missing=()
for i in $(seq 1 53); do
  name=$(printf 'zz-filler-%03d' "$i")
  topic_exists "$name" || filler_missing+=("$name")
done
if [ ${#filler_missing[@]} -gt 0 ]; then
  rpk topic create "${filler_missing[@]}" -p 1 -r 1 >/dev/null
  echo "filler topics: created ${#filler_missing[@]}"
else
  echo "filler topics: exist"
fi

if [ "$(topic_message_count orders)" -eq 0 ]; then
  for p in $(seq 0 5); do
    start=$((p * 100 + 1))
    end=$((p * 100 + 100))
    for i in $(seq "$start" "$end"); do
      printf 'order-%04d\t{"id":%d,"customer":"customer-%03d","total":%d.%02d,"status":"%s"}\n' \
        "$i" "$i" $((i % 97)) $((i * 7 % 500)) $((i % 100)) "$(order_status "$i")"
    done | produce_lines_to_partition orders "$p" "content-type:application/json" "source:seed"
  done
  echo "orders: produced 600 messages (100 per partition)"
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
  counter=0
  for p in $(seq 0 11); do
    for _ in 1 2 3 4; do
      counter=$((counter + 1))
      printf 'Notification %d: your order has been updated. This is a plain text body, not JSON.\n' "$counter"
    done | rpk topic produce notifications -p "$p" -H "channel:email" >/dev/null
  done
  echo "notifications: produced 48 messages (4 per partition)"
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
