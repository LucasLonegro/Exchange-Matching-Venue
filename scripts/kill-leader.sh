#!/usr/bin/env bash
# Failover drill: kills the current leader of a shard in the local system and measures how long the
# shard is without a leader.
#
#   scripts/kill-leader.sh 0                     SIGKILL the leader of shard 0 (a crash, no clean exit)
#   scripts/kill-leader.sh 1 --signal TERM       graceful stop instead
#   scripts/kill-leader.sh 0 --restart-after 5   relaunch the killed member after 5 s (rejoins as follower)
#   scripts/kill-leader.sh 0 --dry-run           only report who the leader is and what would happen
#   --timeout SECS   how long to wait for a new leader (default 30)
#
# Leader discovery (scripts/lib/local-common.sh shard_leader): the node's Prometheus endpoint on
# memberBasePort+9 (lmb_cluster_role == 2, name in deploy/monitoring/metric-names.env), then Aeron's
# ClusterTool list-members on data/local/cluster-node/shard-S-member-M/cluster.
# Failover needs a quorum of the survivors: with 2 members per shard (the default local profile) killing
# the leader leaves 1 of 2, no majority, so NO new leader is elected until the member comes back. Start
# with --members 3 for a real failover drill. The measured time is bounded below by the polling interval
# (~0.1 s with metrics, ~1 s with ClusterTool) and is dominated by lmb.cluster.leaderHeartbeatTimeoutMs.
set -euo pipefail
# shellcheck source=lib/local-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/local-common.sh"

SHARD=""; SIGNAL=KILL; RESTART_AFTER=""; TIMEOUT=30; DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --signal)        SIGNAL="${2:-}"; shift ;;
    --restart-after) [[ "${2:-}" =~ ^[0-9]+$ ]] || die "--restart-after needs seconds"; RESTART_AFTER="$2"; shift ;;
    --timeout)       [[ "${2:-}" =~ ^[1-9][0-9]*$ ]] || die "--timeout needs seconds"; TIMEOUT="$2"; shift ;;
    --dry-run|-n)    DRY_RUN=1 ;;
    -h|--help)       sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*)              die "unknown option '$1' (see --help)" ;;
    *)               SHARD="$1" ;;
  esac
  shift
done
[[ "$SHARD" =~ ^[0-9]+$ ]] || die "usage: kill-leader.sh <shard> [--signal KILL|TERM] [--restart-after S] [--dry-run]"
case "$SIGNAL" in KILL|TERM|INT|STOP) ;; *) die "--signal must be KILL, TERM, INT or STOP" ;; esac
[ -f "$LMB_TOPO_FILE" ] || die "no local run found ($LMB_TOPO_FILE); start one with scripts/local-up.sh --members 3"
topo_load
[ "$SHARD" -lt "$(shard_count)" ] || die "shard $SHARD does not exist (lmb.shards=$(shard_count))"
MEMBERS="$(member_count)"
now_ms() { date +%s%3N; }

shard_leader "$SHARD" || die "cannot find the leader of shard $SHARD: ${LMB_LEADER_DIAG:-}"
leader="$LMB_LEADER"
name="node-$SHARD-$leader"
pid="$(pid_of "$name" || true)"
is_ours "$pid" || die "leader is member $leader but $name is not running from this run (pid '${pid:-none}')"
info "shard $SHARD: leader is member $leader ($name, pid $pid); $MEMBERS member(s)"
quorum=$((MEMBERS / 2 + 1))
if [ $((MEMBERS - 1)) -lt "$quorum" ]; then
  warn "with $MEMBERS member(s) the $((MEMBERS - 1)) survivor(s) are below the quorum of $quorum: the shard stays unavailable until $name returns${RESTART_AFTER:+ (in ${RESTART_AFTER}s)}. Use local-up.sh --members 3 for a failover drill."
fi
if [ "$DRY_RUN" -eq 1 ]; then
  info "dry run: would send SIG$SIGNAL to pid $pid, then poll for a new leader (timeout ${TIMEOUT}s)${RESTART_AFTER:+, relaunch $name after ${RESTART_AFTER}s}"
  exit 0
fi

t0="$(now_ms)"
kill -"$SIGNAL" "$pid"
info "sent SIG$SIGNAL to $name at $(date -u +%H:%M:%S.%3N)"

restarted=0
restart_member() {
  "$LMB_ROOT/scripts/local-up.sh" --restart "$name" || warn "relaunch of $name failed"
  restarted=1
}

new=""
while :; do
  elapsed=$(( $(now_ms) - t0 ))
  if [ -n "$RESTART_AFTER" ] && [ "$restarted" -eq 0 ] && [ "$elapsed" -ge $((RESTART_AFTER * 1000)) ]; then
    restart_member
    elapsed=$(( $(now_ms) - t0 ))
  fi
  if shard_leader "$SHARD"; then
    cand="$LMB_LEADER"
    if [ "$cand" != "$leader" ] || [ "$restarted" -eq 1 ] || [ "$SIGNAL" = STOP ]; then
      # A paused (STOP) old leader can still report LEADER from its last scrape; require a change.
      if [ "$cand" != "$leader" ] || ! is_ours "$pid"; then new="$cand"; break; fi
    fi
  fi
  [ "$elapsed" -ge $((TIMEOUT * 1000)) ] && break
  sleep 0.1
done
elapsed=$(( $(now_ms) - t0 ))
if [ -n "$new" ]; then
  ok "shard $SHARD: new leader member $new after ${elapsed} ms (detection resolution: polling, see header)"
else
  err "shard $SHARD: no new leader within ${TIMEOUT}s (${LMB_LEADER_DIAG:-})"
fi
if [ -n "$RESTART_AFTER" ] && [ "$restarted" -eq 0 ]; then
  sleep_left=$(( RESTART_AFTER - elapsed / 1000 ))
  [ "$sleep_left" -gt 0 ] && sleep "$sleep_left"
  restart_member
fi
[ "$SIGNAL" = STOP ] && warn "$name is stopped (SIGSTOP), not dead: kill -CONT $pid to resume it"
[ -n "$new" ]
