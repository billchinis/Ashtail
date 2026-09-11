#!/usr/bin/env bash
# Idempotent Kafka fixtures for the local Redpanda container (docker-compose.yml).
# Safe to run repeatedly: topics are created only if missing, messages are
# produced only into empty topics, consumer groups only if they do not exist.
# `orders`, `notifications` and `payments` are recreated (and only then) if
# their per-partition layout does not already match the deterministic shape
# below.
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

# topic_layout_ok TOPIC PARTITIONS PER_PARTITION [CONTENT_CHECK_FN]: true if
# the topic has exactly PARTITIONS partitions, each holding exactly
# PER_PARTITION messages, and, when CONTENT_CHECK_FN is given, that function
# (called as `CONTENT_CHECK_FN TOPIC`) also returns true. The content check
# exists because per-partition counts alone cannot tell the old
# same-batch-timestamp `notifications` data from the fixed interleaved order
# AC-18 needs: both lay out as 12 partitions x 4 messages.
topic_layout_ok() {
  local name="$1" partitions="$2" per_partition="$3" content_check="${4:-}"
  local n=0 count
  while read -r count; do
    n=$((n + 1))
    [ "$count" = "$per_partition" ] || return 1
  done < <(rpk topic describe "$name" -p 2>/dev/null | awk 'NR>1 {print $NF - $(NF-1)}')
  [ "$n" -eq "$partitions" ] || return 1
  if [ -n "$content_check" ]; then
    "$content_check" "$name" || return 1
  fi
  return 0
}

# ensure_deterministic_topic NAME PARTITIONS PER_PARTITION [CONTENT_CHECK_FN]:
# recreates the topic only when it exists but its per-partition message
# layout (and, if given, its content) is wrong (e.g. the old
# key-hash-partitioned or same-batch-timestamp data). Never recreates a topic
# whose layout already matches, so re-running the script is a no-op. Exit
# status 0 means the topic was (re)created fresh, 1 means it already had the
# right layout, so callers can decide whether groups pointed at it must be
# reset.
ensure_deterministic_topic() {
  local name="$1" partitions="$2" per_partition="$3" content_check="${4:-}"
  if topic_exists "$name"; then
    if topic_layout_ok "$name" "$partitions" "$per_partition" "$content_check"; then
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

# payments_content_ok NAME: true only if partition 0's first 10 offsets hold
# the reshaped content (AC-22): offset 0 is key pay-001 with a JSON value
# carrying "customer":{"id":"cust-001", and offset 9 is key pay-010 with the
# plain-text legacy-export value. Distinguishes the reshape from the old
# key-hash, flat-JSON layout, which both lay out as 3 partitions x 40
# messages.
payments_content_ok() {
  local name="$1" lines line1 line10
  lines=$(rpk topic consume "$name" -p 0 -n 10 -o start -f '%k\t%v\n' 2>/dev/null)
  line1=$(printf '%s\n' "$lines" | sed -n '1p')
  line10=$(printf '%s\n' "$lines" | sed -n '10p')
  case "$line1" in
    "pay-001"$'\t'*'"customer":{"id":"cust-001"'*) ;;
    *) return 1 ;;
  esac
  case "$line10" in
    "pay-010"$'\t'"pay-010 legacy export:"*) ;;
    *) return 1 ;;
  esac
  return 0
}

# produce_payments_partition PARTITION < lines formatted as "key<TAB>value".
# Like produce_lines_to_partition, but with -Z (empty values become
# tombstones, AC-22's 4 null-value rows). Not folded into
# produce_lines_to_partition itself: orders never produces a null value.
produce_payments_partition() {
  local partition="$1"
  rpk topic produce payments -p "$partition" -Z -f '%k\t%v\n' -H content-type:application/json >/dev/null
}

# payment_value N: prints one "pay-NNN<TAB>value" line for the reshaped
# `payments` topic (AC-22, "Seed changes required ... JSON field filter").
# N divisible by 10 is not JSON: N mod 30 = 10 is plain text, N mod 30 = 20
# is JSON truncated before its closing brace, N mod 30 = 0 is null (an empty
# value, made a tombstone by -Z). Every other N is one line of valid nested
# JSON, with a "note" field inserted before the final brace when N mod 8 = 5.
payment_value() {
  local n="$1" key amount method
  key=$(printf 'pay-%03d' "$n")
  amount=$(( (n * 13) % 900 + 1 ))
  method="sepa"
  [ $((n % 3)) -eq 0 ] && method="card"

  if [ $((n % 10)) -eq 0 ]; then
    case $(( (n / 10) % 3 )) in
      1)
        printf '%s\tpay-%03d legacy export: refunded=true shipping.country=GR note=Gift wrap\n' \
          "$key" "$n"
        ;;
      2)
        printf '%s\t{"amount":%d.00,"method":"%s","refunded":true,"shipping":{"country":"GR"},"note":"Gift wrap"\n' \
          "$key" "$amount" "$method"
        ;;
      0)
        printf '%s\t\n' "$key"
        ;;
    esac
    return
  fi

  local cc="DE" sc="DE" q0 q1 items refunded note_part=""
  [ $((n % 12)) -eq 0 ] && cc="GR"
  [ $((n % 11)) -eq 0 ] && sc="GR"
  q0=$(( (n % 4) + 1 ))
  q1=$(( (n % 7) + 1 ))
  items=$(printf '{"sku":"A-%03d","qty":%d}' "$n" "$q0")
  if [ $((n % 2)) -eq 1 ]; then
    items="$items,$(printf '{"sku":"B-%03d","qty":%d}' "$n" "$q1")"
  fi
  refunded="false"
  [ $((n % 3)) -eq 1 ] && refunded="true"
  if [ $((n % 8)) -eq 5 ]; then
    local note="Ring bell"
    [ $((n % 16)) -eq 5 ] && note="Gift wrap"
    note_part=$(printf ',"note":"%s"' "$note")
  fi
  printf '%s\t{"amount":%d.00,"method":"%s","customer":{"id":"cust-%03d","country":"%s"},"shipping":{"country":"%s"},"items":[%s],"refunded":%s%s}\n' \
    "$key" "$amount" "$method" "$n" "$cc" "$sc" "$items" "$refunded" "$note_part"
}

# verify_payments: fails loudly unless, for each partition p (0, 1, 2),
# consuming its 40 messages from the start yields keys pay-(40p+1)..
# pay-(40p+40) in offset order, no value longer than 199 characters, and
# every timestamp in partition p is strictly lower than every timestamp in
# partition p + 1 (AC-22..AC-24 need a real cross-partition newest-first
# order, not an accident of production speed).
verify_payments() {
  local p n key ts value expected_key len
  local prev_max=-1 this_min this_max

  for p in 0 1 2; do
    this_min="" this_max="" n=0
    while IFS=$'\t' read -r key ts value; do
      n=$((n + 1))
      expected_key=$(printf 'pay-%03d' $((p * 40 + n)))
      if [ "$key" != "$expected_key" ]; then
        echo "seed: payments partition $p offset $((n - 1)) has key '$key', expected '$expected_key'" >&2
        exit 1
      fi
      len=${#value}
      if [ "$len" -gt 199 ]; then
        echo "seed: payments key $key has a value $len characters long (max 199)" >&2
        exit 1
      fi
      [ -z "$this_min" ] && this_min="$ts"
      this_max="$ts"
    done < <(rpk topic consume payments -p "$p" -o start -n 40 -f '%k\t%d\t%v\n' 2>/dev/null)

    if [ "$n" -ne 40 ]; then
      echo "seed: expected 40 messages in payments partition $p, got $n" >&2
      exit 1
    fi
    if [ "$prev_max" -ge 0 ] && [ "$this_min" -le "$prev_max" ]; then
      echo "seed: payments partition $p's earliest timestamp is not after partition $((p - 1))'s latest" >&2
      exit 1
    fi
    prev_max="$this_max"
  done

  echo "payments: verified per-partition keys, max value length, and ascending cross-partition timestamps"
}

# notifications_partition0_ok NAME: true only if partition 0 holds, at
# offsets 0..3, values starting "Notification 1:", "Notification 13:",
# "Notification 25:", "Notification 37:" with channel headers email, email,
# sms, email (AC-18's fixed interleaved order: message N sits at partition
# ((N - 1) * 5) mod 12, so partition 0 is exactly N = 1, 13, 25, 37).
notifications_partition0_ok() {
  local name="$1" actual expected
  actual=$(rpk topic consume "$name" -p 0 -n 4 -o start -f '%v\t%h{%k=%v}\n' 2>/dev/null)
  expected=$(printf 'Notification %d: your order has been updated. This is a plain text body, not JSON.\tchannel=%s\n' \
    1 email 13 email 25 sms 37 email)
  [ "$actual" = "$expected" ]
}

# produce_notifications: produces N = 1..48 to `notifications` strictly in
# order, one `rpk topic produce` invocation per message (so each gets its own
# CreateTime), message N to partition ((N - 1) * 5) mod 12 with header
# `channel: sms` on N divisible by 5 and `channel: email` otherwise. Runs
# inside one `docker compose exec` shell so 48 host-side execs are not
# needed; each invocation already takes far longer than 2 ms, which is what
# keeps the 48 timestamps apart (verify_notifications_timestamps checks it).
produce_notifications() {
  docker compose exec -T redpanda sh -c '
    set -e
    for i in $(seq 1 48); do
      channel="email"
      [ $((i % 5)) -eq 0 ] && channel="sms"
      partition=$(( ((i - 1) * 5) % 12 ))
      printf "Notification %d: your order has been updated. This is a plain text body, not JSON.\n" "$i" |
        rpk topic produce notifications -p "$partition" -H "channel:$channel" >/dev/null
    done
  '
}

# verify_notifications_timestamps: fails loudly unless consuming all 48
# `notifications` messages yields 48 distinct millisecond timestamps whose
# ascending order is N = 1..48 (AC-18, AC-19, AC-20 need a real per-message
# timestamp merge, not same-batch ties).
verify_notifications_timestamps() {
  local value ts n prev=-1 count=0
  declare -A ts_by_n

  while IFS=$'\t' read -r value ts; do
    n=$(printf '%s' "$value" | sed -n 's/^Notification \([0-9]*\):.*/\1/p')
    if [ -z "$n" ]; then
      echo "seed: could not parse a notification number from '$value'" >&2
      exit 1
    fi
    ts_by_n["$n"]="$ts"
    count=$((count + 1))
  done < <(rpk topic consume notifications -o start -n 48 -f '%v\t%d\n' 2>/dev/null)

  if [ "$count" -ne 48 ]; then
    echo "seed: expected to consume 48 notifications, got $count" >&2
    exit 1
  fi

  for n in $(seq 1 48); do
    ts="${ts_by_n[$n]:-}"
    if [ -z "$ts" ]; then
      echo "seed: notification $n is missing from notifications" >&2
      exit 1
    fi
    if [ "$ts" -le "$prev" ]; then
      echo "seed: notification timestamps are not strictly ascending (N=$n, ts=$ts, prev=$prev)" >&2
      exit 1
    fi
    prev="$ts"
  done

  echo "notifications: verified 48 distinct, ascending timestamps (N = 1..48)"
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
if ensure_deterministic_topic payments 3 40 payments_content_ok; then
  delete_group_if_exists payments-worker
fi
ensure_topic_config payments retention.ms=604800000
if ensure_deterministic_topic notifications 12 4 notifications_partition0_ok; then
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
  for p in 0 1 2; do
    start=$((p * 40 + 1))
    end=$((p * 40 + 40))
    for i in $(seq "$start" "$end"); do
      payment_value "$i"
    done | produce_payments_partition "$p"
  done
  echo "payments: produced 120 messages (3 partitions x 40, mixed JSON/plain/truncated/null)"
fi
verify_payments

if [ "$(topic_message_count notifications)" -eq 0 ]; then
  # Plain-text, null-key values, produced one at a time (own CreateTime each)
  # in the fixed interleaved order AC-18/AC-19/AC-20 need.
  produce_notifications
  echo "notifications: produced 48 messages (4 per partition, interleaved order)"
fi
verify_notifications_timestamps

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
