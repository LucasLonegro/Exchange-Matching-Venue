#!/usr/bin/env bash
# Load test of the real system (owner: tools): starts the private venue of scripts/smoke-test.sh (2 shards x
# 2 members, 1 control plane, 1 gateway; ports 39000-39999, data tools/target/e2e), runs
# io.lmb.tools.LoadGeneratorMain and prints its p50/p99/p999/max latencies, then stops the venue.
#
#   scripts/load-test.sh                                  gRPC, 1000 orders/s for 30 s
#   scripts/load-test.sh --protocol fix --rate 1500 --duration-s 60
#   scripts/load-test.sh --no-start -- --sessions 1001,2001   against a venue left running by smoke-test.sh --keep
#   --keep        leave the venue running afterwards
#   -- ARGS       extra LoadGeneratorMain arguments (--sessions, --symbols, --qty, --warmup-s, --report-s)
# The gateway throttles each session to 500 orders/s by default: keep --rate <= 500 x sessions (4 by default).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export LMB_LOCAL_DATA="${LMB_LOCAL_DATA:-$ROOT/tools/target/e2e}"
PROTOCOL=grpc; RATE=1000; DURATION=30; START=1; KEEP=0; EXTRA=()
while [ $# -gt 0 ]; do
  case "$1" in
    --protocol)   PROTOCOL="$2"; shift ;;
    --rate)       RATE="$2"; shift ;;
    --duration-s) DURATION="$2"; shift ;;
    --no-start)   START=0; KEEP=1 ;;
    --keep)       KEEP=1 ;;
    --) shift; EXTRA=("$@"); break ;;
    -h|--help)    sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option $1 (see --help)" >&2; exit 2 ;;
  esac
  shift
done
if [ "$START" -eq 1 ]; then
  "$ROOT/scripts/smoke-test.sh" --keep --protocol "$PROTOCOL" | grep -E '^==>|SMOKE (PASS|FAIL|RESULT)' || exit 1
fi
if [ "$KEEP" -eq 0 ]; then
  trap '"$ROOT/scripts/local-down.sh" --grace 10 > "$ROOT/tools/target/load-down.log" 2>&1; echo "==> venue stopped"' EXIT
fi
LOG="$ROOT/tools/target/load-$PROTOCOL.log"
echo "==> LoadGeneratorMain $PROTOCOL rate=$RATE/s duration=${DURATION}s (log: ${LOG#"$ROOT/"})"
RC=0
LMB_HEAP=128m "$ROOT/scripts/lmb-run" io.lmb.tools.LoadGeneratorMain --topology "$LMB_LOCAL_DATA/run/topology.properties" \
  --protocol "$PROTOCOL" --rate "$RATE" --duration-s "$DURATION" ${EXTRA[@]+"${EXTRA[@]}"} > "$LOG" 2>&1 || RC=$?
grep '^LOADGEN' "$LOG" | sed 's/^/    /'
[ "$RC" -eq 0 ] || { echo "==> load test FAILED (exit $RC)"; tail -n 20 "$LOG" >&2; }
exit "$RC"
