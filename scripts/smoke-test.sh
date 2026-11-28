#!/usr/bin/env bash
# End-to-end smoke test of the real system (owner: tools). Starts a private venue (real cluster nodes with
# the real matcher, one control plane, one gateway; ports 39000-39999 from tools/e2e/topology.properties,
# data under tools/target/e2e), sends crossing order pairs through the gateway with
# io.lmb.tools.SmokeTestMain, checks ack + fills, and stops everything it started.
#
#   scripts/smoke-test.sh                       gRPC, 2 shards x 2 members
#   scripts/smoke-test.sh --protocol fix        FIX 4.4 (Artio acceptor on the gateway)
#   scripts/smoke-test.sh --protocol both       gRPC then FIX against the same venue
#   scripts/smoke-test.sh --failover            3 members per shard; kills each shard's leader
#                                               (scripts/kill-leader.sh) while orders keep flowing
#   scripts/smoke-test.sh --keep                leave the venue running afterwards
#                                               (stop: LMB_LOCAL_DATA=tools/target/e2e scripts/local-down.sh)
#   scripts/smoke-test.sh --no-start            use an already running venue started with --keep
#   scripts/smoke-test.sh -- --pairs 20 --verbose   extra SmokeTestMain arguments
#   --members N   members per shard (default 2; 3 with --failover)
# Exit code 0 when every check passed. Needs the distribution: ./mvnw -q -pl dist install.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LMB_LOCAL_DATA="${LMB_LOCAL_DATA:-$ROOT/tools/target/e2e}"
TOPO_BASE="$ROOT/tools/e2e/topology.properties"
PROTOCOL=grpc; MEMBERS=""; KEEP=0; START=1; FAILOVER=0
EXTRA=()
while [ $# -gt 0 ]; do
  case "$1" in
    --protocol) PROTOCOL="$2"; shift ;;
    --members)  MEMBERS="$2"; shift ;;
    --keep)     KEEP=1 ;;
    --no-start) START=0; KEEP=1 ;;
    --failover) FAILOVER=1 ;;
    --) shift; EXTRA=("$@"); break ;;
    -h|--help)  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option $1 (see --help)" >&2; exit 2 ;;
  esac
  shift
done
[ -z "$MEMBERS" ] && { [ "$FAILOVER" -eq 1 ] && MEMBERS=3 || MEMBERS=2; }
case "$PROTOCOL" in grpc) PROTOS=(grpc) ;; fix) PROTOS=(fix) ;; both) PROTOS=(grpc fix) ;;
  *) echo "--protocol must be grpc, fix or both" >&2; exit 2 ;; esac

LOGDIR="$LMB_LOCAL_DATA/logs"
stop_venue() {
  if [ "$KEEP" -eq 0 ]; then
    echo "==> stopping the venue"
    "$ROOT/scripts/local-down.sh" --grace 10 > "$ROOT/tools/target/smoke-down.log" 2>&1 \
      || { echo "local-down failed, see tools/target/smoke-down.log" >&2; }
  else
    echo "==> venue left running (stop: LMB_LOCAL_DATA=$LMB_LOCAL_DATA scripts/local-down.sh)"
  fi
}

mkdir -p "$ROOT/tools/target"
if [ "$START" -eq 1 ]; then
  "$ROOT/scripts/local-down.sh" --wipe --grace 10 > "$ROOT/tools/target/smoke-down.log" 2>&1 || true
  trap stop_venue EXIT
  echo "==> starting the venue: 2 shards x $MEMBERS members, 1 control plane, 1 gateway (log: tools/target/smoke-up.log)"
  if ! "$ROOT/scripts/local-up.sh" --topology "$TOPO_BASE" --shards 2 --members "$MEMBERS" --gateways 1 \
       --controlplanes 1 --no-marketdata --no-consolidators --no-feedsim --no-posttrade --no-replayer \
       --timeout 90 --strict-leaders > "$ROOT/tools/target/smoke-up.log" 2>&1; then
    tail -n 60 "$ROOT/tools/target/smoke-up.log" >&2
    exit 1
  fi
  grep -E 'ready after|leader is' "$ROOT/tools/target/smoke-up.log" | sed 's/^ */    /'
fi
TOPO="$LMB_LOCAL_DATA/run/topology.properties"
[ -f "$TOPO" ] || { echo "no running venue topology at $TOPO" >&2; exit 1; }

# The control plane opens the market (autoOpen) shortly after it defines the seeded symbols; wait for all of them.
CP_PORT="$(grep -E '^lmb.controlplane.0.restPort=' "$TOPO" | tail -1 | cut -d= -f2)"
n=0; total=0
for _ in $(seq 1 60); do
  body="$(curl -s -H 'X-LMB-Admin-Token: lmb-local-admin' "http://127.0.0.1:${CP_PORT:-9700}/api/symbols" || true)"
  total="$(grep -o '"status":"ACTIVE"' <<< "$body" | wc -l || true)"
  n="$(grep -o '"shardState":"CONTINUOUS"' <<< "$body" | wc -l || true)"
  [ "${n:-0}" -gt 0 ] && [ "$n" -eq "$total" ] && break
  sleep 0.5
done
echo "==> $n of $total symbols CONTINUOUS according to the control plane"

run_tool() {  # run_tool <log> <main> args...
  local log="$1"; shift
  LMB_HEAP=96m "$ROOT/scripts/lmb-run" "$@" > "$log" 2>&1
}

RC=0
if [ "$FAILOVER" -eq 1 ]; then
  for proto in "${PROTOS[@]}"; do
    log="$ROOT/tools/target/smoke-failover-$proto.log"
    echo "==> [$proto] orders every 200 ms while each shard's leader is killed (log: ${log#"$ROOT/"})"
    run_tool "$log" io.lmb.tools.SmokeTestMain --topology "$TOPO" --protocol "$proto" --pairs 60 \
      --interval-ms 200 --timeout-ms 10000 --max-failures 0 ${EXTRA[@]+"${EXTRA[@]}"} &
    tool=$!
    sleep 3
    for shard in 0 1; do
      "$ROOT/scripts/kill-leader.sh" "$shard" --timeout 30 2>&1 | grep -E 'leader|killed|elected|error' | sed 's/^ */    /' || true
      sleep 3
    done
    wait "$tool" || RC=1
    grep -E '^SMOKE (FAIL|RESULT)' "$log" | sed 's/^/    /'
    grep -c '^SMOKE PASS' "$log" | sed 's/^/    passes: /'
  done
else
  for proto in "${PROTOS[@]}"; do
    log="$ROOT/tools/target/smoke-$proto.log"
    echo "==> [$proto] crossing order pairs through gateway-0 (log: ${log#"$ROOT/"})"
    if run_tool "$log" io.lmb.tools.SmokeTestMain --topology "$TOPO" --protocol "$proto" --verbose \
         ${EXTRA[@]+"${EXTRA[@]}"}; then :; else RC=1; fi
    grep -E '^SMOKE|<-' "$log" | sed 's/^/    /'
  done
fi

errs="$(grep -lE ' ERROR ' "$LOGDIR"/*.log 2>/dev/null || true)"
[ -n "$errs" ] && echo "==> ERROR lines in: $(echo "$errs" | xargs -n1 basename | tr '\n' ' ')"
[ "$RC" -eq 0 ] && echo "==> SMOKE TEST PASSED" || echo "==> SMOKE TEST FAILED"
exit "$RC"
