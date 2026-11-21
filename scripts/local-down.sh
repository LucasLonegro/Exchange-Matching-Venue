#!/usr/bin/env bash
# Stops everything scripts/local-up.sh started, in reverse start order (replayers first, shard nodes
# last), with SIGTERM and a grace period, then SIGKILL. Also stops stray processes carrying this
# data directory's tag (pid file lost) and removes the run's Aeron directories under /dev/shm.
#
#   scripts/local-down.sh              stop, keep data and logs (a restart recovers from the Raft logs)
#   scripts/local-down.sh --wipe       stop and delete data/local entirely (fresh start next time)
#   scripts/local-down.sh --snapshot   first ask each shard's leader for a snapshot + orderly shutdown
#                                      (Aeron ClusterTool <cluster-dir> shutdown), then stop
#   scripts/local-down.sh --force      SIGKILL immediately (simulates a crash of everything)
#   --grace SECS   time between SIGTERM and SIGKILL per tier (default 20)
set -euo pipefail
# shellcheck source=lib/local-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/local-common.sh"

WIPE=0; FORCE=0; SNAPSHOT=0; GRACE=20
while [ $# -gt 0 ]; do
  case "$1" in
    --wipe)     WIPE=1 ;;
    --force)    FORCE=1 ;;
    --snapshot) SNAPSHOT=1 ;;
    --grace)    [[ "${2:-}" =~ ^[0-9]+$ ]] || die "--grace needs seconds"; GRACE="$2"; shift ;;
    -h|--help)  sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)          die "unknown option '$1' (see --help)" ;;
  esac
  shift
done
[ "$FORCE" -eq 1 ] && GRACE=0

if [ "$SNAPSHOT" -eq 1 ] && [ "$FORCE" -eq 0 ] && [ -f "$LMB_TOPO_FILE" ]; then
  info "orderly cluster shutdown with snapshot (best effort)"
  topo_load
  for ((s = 0; s < $(shard_count); s++)); do
    if LMB_LEADER_METHODS="${LMB_LEADER_METHODS:-metrics clustertool}" shard_leader "$s"; then
      leader="$LMB_LEADER"
      dir="$(cluster_dirs "$s" "$leader" | head -n1)"
      if [ -n "$dir" ] && clustertool "$dir" shutdown >/dev/null; then
        ok "shard $s: shutdown requested on leader member $leader"
      else
        warn "shard $s: ClusterTool shutdown failed; nodes will get SIGTERM"
      fi
    else
      warn "shard $s: leader unknown, skipping snapshot (${LMB_LEADER_DIAG:-})"
    fi
  done
  sleep 2
fi

stopped_any=0
if [ -f "$LMB_RUN_DIR/start-order" ]; then
  mapfile -t tiers < <(started_tiers)
  for ((t = ${#tiers[@]} - 1; t >= 0; t--)); do
    mapfile -t names < <(names_in_tier "${tiers[$t]}")
    live=()
    for n in "${names[@]}"; do is_alive "$n" && live+=("$n"); done
    [ "${#live[@]}" -gt 0 ] || continue
    info "stopping ${tiers[$t]}: ${live[*]}"
    stop_names "$GRACE" "${live[@]}"
    stopped_any=1
  done
fi

# Strays: processes with our tag that have no (valid) pid file.
strays="$(tagged_pids | tr '\n' ' ')"
if [ -n "${strays// /}" ]; then
  info "stopping stray processes with tag $LMB_TAG: $strays"
  # shellcheck disable=SC2086
  kill -TERM $strays 2>/dev/null || true
  for _ in $(seq 1 $((GRACE * 5))); do
    [ -z "$(tagged_pids)" ] && break
    sleep 0.2
  done
  left="$(tagged_pids | tr '\n' ' ')"
  # shellcheck disable=SC2086
  [ -n "${left// /}" ] && { warn "SIGKILL $left"; kill -KILL $left 2>/dev/null || true; }
  stopped_any=1
fi
[ "$stopped_any" -eq 1 ] || info "nothing running for $LMB_DATA"
rm -f "$LMB_PID_DIR"/*.pid 2>/dev/null || true

# Aeron directories of this run (each process got -Daeron.dir=$LMB_SHM_DIR/<name>).
case "$LMB_SHM_DIR" in
  /dev/shm/lmb-local-*)
    if [ -d "$LMB_SHM_DIR" ]; then rm -rf -- "$LMB_SHM_DIR"; ok "removed $LMB_SHM_DIR"; fi ;;
  *) warn "LMB_SHM_DIR=$LMB_SHM_DIR is not under /dev/shm/lmb-local-*: not removing it" ;;
esac
others="$(find /dev/shm -maxdepth 1 -user "$(id -u)" \( -name 'aeron-*' -o -name 'lmb-local-*' \) 2>/dev/null | tr '\n' ' ')"
[ -n "${others// /}" ] && warn "other Aeron directories of yours remain in /dev/shm (not from this run, left alone): $others"

if [ "$WIPE" -eq 1 ]; then
  [ -z "$(tagged_pids)" ] || die "processes still running; not wiping"
  real="$(cd "$LMB_DATA" 2>/dev/null && pwd -P || true)"
  if [ -z "$real" ]; then
    info "nothing to wipe ($LMB_DATA does not exist)"
  elif [ "$real" = / ] || [ "$real" = "$HOME" ] || [ "$real" = "$(cd "$LMB_ROOT" && pwd -P)" ]; then
    die "refusing to wipe $real"
  else
    rm -rf -- "$real"
    ok "wiped $real"
  fi
fi
