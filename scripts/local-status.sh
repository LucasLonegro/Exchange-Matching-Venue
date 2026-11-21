#!/usr/bin/env bash
# Shows the processes of the local system started by scripts/local-up.sh.
#
#   scripts/local-status.sh            name, pid, state, LMB_READY seen, RSS, uptime; total RSS
#   scripts/local-status.sh --leaders  also the leader of each shard (metrics, then Aeron ClusterTool)
#   scripts/local-status.sh --probe    also check each process's /metrics endpoint answers
#   scripts/local-status.sh --watch 2  refresh every 2 seconds
# Exit status: 0 when every started process is running, 1 otherwise (usable from scripts).
set -euo pipefail
# shellcheck source=lib/local-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/local-common.sh"

LEADERS=0; PROBE=0; WATCH=0
while [ $# -gt 0 ]; do
  case "$1" in
    --leaders) LEADERS=1 ;;
    --probe)   PROBE=1 ;;
    --watch)   [[ "${2:-}" =~ ^[1-9][0-9]*$ ]] || die "--watch needs seconds"; WATCH="$2"; shift ;;
    -h|--help) sed -n '2,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)         die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

status_once() {
  local name pid state ready rss up mport probe total=0 bad=0 n=0
  [ -f "$LMB_RUN_DIR/start-order" ] || { log "no local run found under $LMB_DATA (scripts/local-up.sh starts one)"; return 1; }
  topo_load
  printf '%-16s %8s %-8s %-6s %8s %9s %s\n' NAME PID STATE READY RSS_MB UPTIME "$([ "$PROBE" -eq 1 ] && echo METRICS)"
  for name in $(started_names); do
    n=$((n + 1))
    pid="$(pid_of "$name" || true)"
    rss="-"; up="-"; probe=""
    if is_ours "$pid"; then
      state="${C_GRN}running${C_OFF}"
      rss="$(awk '/^VmRSS:/ { print int($2 / 1024) }' "/proc/$pid/status" 2>/dev/null || echo 0)"
      total=$((total + ${rss:-0}))
      up="$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')"; up="${up:-0}s"
    else
      state="${C_RED}down${C_OFF}   "; pid="${pid:--}"; bad=$((bad + 1))
    fi
    if is_ready "$name"; then ready=yes; else ready="${C_YEL}no${C_OFF}  "; fi
    if [ "$PROBE" -eq 1 ]; then
      mport="$(metrics_port_of "$name" || true)"
      if curl -fsS --max-time 1 -o /dev/null "http://$(host_of "$name"):$mport/metrics" 2>/dev/null; then
        probe=":$mport ok"
      else
        probe=":$mport ${C_RED}no answer${C_OFF}"
      fi
    fi
    printf '%-16s %8s %-8s %-6s %8s %9s %s\n' "$name" "$pid" "$state" "$ready" "$rss" "$up" "$probe"
  done
  printf '%d processes, %d not running, total RSS %d MB\n' "$n" "$bad" "$total"
  if [ "$LEADERS" -eq 1 ]; then
    local s leader
    for ((s = 0; s < $(shard_count); s++)); do
      if shard_leader "$s"; then
        leader="$LMB_LEADER"
        printf 'shard %d leader: member %s (node-%d-%s)\n' "$s" "$leader" "$s" "$leader"
      else
        printf 'shard %d leader: %sunknown%s (%s)\n' "$s" "$C_YEL" "$C_OFF" "${LMB_LEADER_DIAG:-}"
      fi
    done
  fi
  [ "$bad" -eq 0 ]
}

if [ "$WATCH" -gt 0 ]; then
  while :; do
    clear 2>/dev/null || true
    date '+%H:%M:%S'
    status_once || true
    sleep "$WATCH"
  done
fi
status_once
