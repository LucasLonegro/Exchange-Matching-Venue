#!/usr/bin/env bash
# Brings up the local copy of the venue from the assembled distribution, no Docker needed.
#
#   scripts/local-up.sh                  2 shards x 2 members, 2 gateways, 2 MD publishers (A/B),
#                                        2 control planes (no NBBO consolidators or feed simulator:
#                                        the venue sets its own reference prices), post-trade, the ledger,
#                                        1 shadow replayer per shard
#   scripts/local-up.sh --minimal        1 shard x 1 member and one of everything, no post-trade or ledger
#   scripts/local-up.sh --members 3      3 members per shard (survives a leader kill: kill-leader.sh)
#   scripts/local-up.sh --dry-run        print the exact commands, start nothing
#   scripts/local-up.sh --restart gateway-1    relaunch one process with its saved command
#
# Start order: shard nodes -> wait for a leader per shard -> control plane -> access service -> seed
# (scripts/local-seed: symbols, risk accounts, teams and their keys, through the APIs) -> consolidators and
# feed simulator -> market data -> gateways -> post-trade -> ledger -> replayers. Each process must print
# "LMB_READY <service> <instance>" within the tier timeout; otherwise the run is torn down (unless
# --keep-on-failure) and the tail of the failing log is printed.
#
# Per-service configuration without editing this script (later wins):
#   deploy/local/common.env           every process (JVM caps, Aeron settings for a laptop)
#   deploy/local/full.env             with --full: points the services at deploy/local/docker-compose.yml
#   deploy/local/<service>.env        e.g. gateway.env, node.env (service names below)
#   deploy/local/<name>.env           one instance, e.g. node-0-1.env, gateway-1.env
#   --env  <target>:KEY=VALUE         target = all | <service> | <name>; LMB_* keys become config
#                                     (io.lmb.common.Config maps LMB_FOO_BAR to lmb.foo.bar)
#   --jvm  <target>:-Dkey=value       extra JVM argument
#   --heap <target>=256m              max heap
# The env files are sourced by bash with `set -a`. Besides any LMB_<KEY> config they may set
# LMB_HEAP, LMB_JAVA_OPTS (JVM flags; append to it), LMB_COMMON_ARGS / LMB_EXTRA_ARGS (-D arguments).
# Service names: node gateway marketdata consolidator feedsim controlplane access posttrade ledger replayer.
set -euo pipefail
# shellcheck source=lib/local-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/local-common.sh"

usage() { sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed -e '$d' -e 's/^# \{0,1\}//'; cat <<'EOF'
Options:
  --shards N --members N --gateways N --md N --consolidators N --controlplanes N
  --minimal               1 shard x 1 member, one instance of each service, no post-trade or ledger
  --no-posttrade --no-ledger --no-replayer --no-marketdata --no-gateways
  --no-seed               do not run scripts/local-seed (symbols, firms, teams and keys come only from you)
  --consolidators N --with-feedsim   also run NBBO consolidators and the feed simulator (legacy, external-market mode)
  --skip <name|service>   do not start it (repeatable)
  --full                  use deploy/local/full.env (start the infrastructure first:
                          docker compose -f deploy/local/docker-compose.yml up -d)
  --topology FILE         base topology instead of deploy/local/topology.properties
  --env T:K=V  --jvm T:-Dk=v  --heap T=SIZE      per-target overrides (see above)
  --timeout SECS          readiness timeout per tier (default 120)
  --leader-timeout SECS   time to wait for a leader per shard (default 60)
  --strict-leaders        fail (instead of warn) when the leader of a shard cannot be determined
  --keep-on-failure       leave already started processes running when a start fails
  --force                 start despite pre-flight failures (memory, ports, missing main classes)
  --dry-run               print the exact launch commands and generated topology; start nothing
  --restart NAME          stop (if running) and relaunch one process from data/local/run/cmd/NAME.sh
  --data DIR              data directory (default data/local; or LMB_LOCAL_DATA)
Environment: LMB_RUNNER (default scripts/lmb-run; scripts/testing/stub-lmb-run for a JVM-free
rehearsal), LMB_HEAP_<SERVICE> (default heap per service), LMB_LEADER_METHODS, LMB_LEADER_CMD,
LMB_CLUSTER_DIR_TEMPLATE (see scripts/lib/local-common.sh).
EOF
}

# ---- arguments ---------------------------------------------------------------------------------------
BASE_TOPO="$LMB_ROOT/deploy/local/topology.properties"
SHARDS=""; MEMBERS=""; GATEWAYS=""; MDS=""; CONS=""; CPS=""
WITH_POSTTRADE=1; WITH_LEDGER=1; WITH_REPLAYER=1; WITH_FEEDSIM=0; SEED=1
FULL=0; DRY_RUN=0; FORCE=0; KEEP_ON_FAILURE=0; STRICT_LEADERS=0
TIER_TIMEOUT=120; LEADER_TIMEOUT=60
RESTART=""
declare -a SKIP=() CLI_ENV=() CLI_JVM=() CLI_HEAP=() SKIPPED_MISSING=()

need() { [ $# -ge 2 ] && [ -n "$2" ] || die "option $1 needs a value (see --help)"; }
posint() { [[ "$2" =~ ^[1-9][0-9]*$ ]] || die "option $1 needs a positive integer, got '$2'"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --shards)         need "$@"; posint "$1" "$2"; SHARDS="$2"; shift ;;
    --members)        need "$@"; posint "$1" "$2"; MEMBERS="$2"; shift ;;
    --gateways)       need "$@"; posint "$1" "$2"; GATEWAYS="$2"; shift ;;
    --md)             need "$@"; posint "$1" "$2"; MDS="$2"; shift ;;
    --consolidators)  need "$@"; posint "$1" "$2"; CONS="$2"; shift ;;
    --controlplanes)  need "$@"; posint "$1" "$2"; CPS="$2"; shift ;;
    --minimal)        SHARDS=1; MEMBERS=1; GATEWAYS=1; MDS=1; CONS=0; CPS=1; WITH_POSTTRADE=0; WITH_LEDGER=0 ;;
    --no-posttrade)   WITH_POSTTRADE=0 ;;
    --no-ledger)      WITH_LEDGER=0 ;;
    --no-seed)        SEED=0 ;;
    --no-replayer|--no-replayers) WITH_REPLAYER=0 ;;
    --no-feedsim)     WITH_FEEDSIM=0 ;;
    --with-feedsim)   WITH_FEEDSIM=1 ;;
    --no-marketdata|--no-md) SKIP+=(marketdata) ;;
    --no-gateways|--no-gateway) SKIP+=(gateway) ;;
    --no-consolidators|--no-consolidator) SKIP+=(consolidator) ;;
    --skip)           need "$@"; SKIP+=("$2"); shift ;;
    --full)           FULL=1 ;;
    --topology)       need "$@"; BASE_TOPO="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift ;;
    --env)            need "$@"; [[ "$2" == *:*=* ]] || die "--env wants target:KEY=VALUE"; CLI_ENV+=("$2"); shift ;;
    --jvm)            need "$@"; [[ "$2" == *:-* ]] || die "--jvm wants target:-Dkey=value"; CLI_JVM+=("$2"); shift ;;
    --heap)           need "$@"; [[ "$2" == *=* ]] || die "--heap wants target=SIZE"; CLI_HEAP+=("$2"); shift ;;
    --timeout)        need "$@"; posint "$1" "$2"; TIER_TIMEOUT="$2"; shift ;;
    --leader-timeout) need "$@"; posint "$1" "$2"; LEADER_TIMEOUT="$2"; shift ;;
    --strict-leaders) STRICT_LEADERS=1 ;;
    --keep-on-failure) KEEP_ON_FAILURE=1 ;;
    --force)          FORCE=1 ;;
    --dry-run|-n)     DRY_RUN=1 ;;
    --restart)        need "$@"; RESTART="$2"; shift ;;
    --data)           need "$@"; die "--data must come first; use LMB_LOCAL_DATA=$2 $0 ... instead" ;;
    -h|--help)        usage; exit 0 ;;
    *)                die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

# ---- --restart: relaunch one saved process --------------------------------------------------------------
launch() {  # launch <name>: start the saved command detached, record the pid
  local name="$1" cmd log
  cmd="$LMB_CMD_DIR/$name.sh"; log="$(log_file "$name")"
  [ -f "$cmd" ] || die "no saved command for $name ($cmd); run local-up.sh first"
  [ -f "$log" ] && mv -f "$log" "$log.prev"
  if command -v setsid >/dev/null 2>&1; then
    # Not a process-group leader (no job control in scripts), so setsid does not fork: $! is the
    # pid that lmb-run finally execs into java.
    setsid bash "$cmd" > "$log" 2>&1 < /dev/null &
  else
    nohup bash "$cmd" > "$log" 2>&1 < /dev/null &
  fi
  echo "$!" > "$(pid_file "$name")"
}

FAILED_NAMES=()
fail_start() {  # fail_start <name> <reason>
  local name="$1" reason="$2" log
  log="$(log_file "$name")"
  err "$name: $reason"
  if [ -f "$log" ]; then
    printf '%s---- last 40 lines of %s ----%s\n' "$C_DIM" "$log" "$C_OFF" >&2
    tail -n 40 "$log" >&2 || true
    printf '%s---- end of %s ----%s\n' "$C_DIM" "$log" "$C_OFF" >&2
  fi
  FAILED_NAMES+=("$name")
}

# wait_ready <timeout> <name>...: every process prints LMB_READY, or the start fails.
wait_ready() {
  local timeout="$1"; shift
  local -a pending=("$@") still=()
  local start=$SECONDS name
  while [ "${#pending[@]}" -gt 0 ]; do
    still=()
    for name in "${pending[@]}"; do
      if is_ready "$name"; then
        ok "$name ready after $((SECONDS - start))s (pid $(pid_of "$name"))"
      # Right after launch the pid is still the bash launcher (no -Dlmb.local.tag on its cmdline until
      # lmb-run execs java), so is_alive alone reports a slow start as an exit.
      elif ! is_alive "$name" && [[ "$(ps -o stat= -p "$(pid_of "$name")" 2>/dev/null || true)" =~ ^(Z.*)?$ ]]; then
        fail_start "$name" "exited before printing LMB_READY (see the log above; $(log_file "$name"))"
        return 1
      else
        still+=("$name")
      fi
    done
    pending=("${still[@]+"${still[@]}"}")
    [ "${#pending[@]}" -eq 0 ] && break
    if [ $((SECONDS - start)) -ge "$timeout" ]; then
      for name in "${pending[@]}"; do
        fail_start "$name" "no LMB_READY line within ${timeout}s (still running as pid $(pid_of "$name"); raise --timeout if the machine is just slow)"
      done
      return 1
    fi
    sleep 0.5
  done
}

if [ -n "$RESTART" ]; then
  [ -f "$LMB_CMD_DIR/$RESTART.sh" ] || die "unknown process '$RESTART' (have: $(started_names | tr '\n' ' '))"
  mkdir -p "$LMB_PID_DIR" "$LMB_LOG_DIR"
  if is_alive "$RESTART"; then info "stopping $RESTART"; stop_names 20 "$RESTART"; fi
  info "relaunching $RESTART"
  launch "$RESTART"
  wait_ready "$TIER_TIMEOUT" "$RESTART" || exit 1
  exit 0
fi

# ---- profile -------------------------------------------------------------------------------------------------
[ -f "$BASE_TOPO" ] || die "topology not found: $BASE_TOPO"
topo_load "$BASE_TOPO"
SHARDS="${SHARDS:-$(shard_count)}"
MEMBERS="${MEMBERS:-$(member_count)}"
GATEWAYS="${GATEWAYS:-$(gateway_count)}"
MDS="${MDS:-$(md_count)}"
CONS="${CONS:-$(cons_count)}"
CPS="${CPS:-$(cp_count)}"

skipped() {  # skipped <name>: by name or by service
  local x
  for x in "${SKIP[@]+"${SKIP[@]}"}"; do
    [ "$x" = "$1" ] || [ "$x" = "${1%%-*}" ] && return 0
  done
  return 1
}

# Tiers in start order. Each entry: "<tier> <name> <service> <identity args...>"
declare -a PLAN=()
plan() { skipped "$2" || PLAN+=("$*"); }
for ((s = 0; s < SHARDS; s++)); do
  for ((m = 0; m < MEMBERS; m++)); do
    plan nodes "node-$s-$m" node "-Dlmb.shard.id=$s" "-Dlmb.member.id=$m"
  done
done
for ((i = 0; i < CPS; i++)); do plan controlplane "controlplane-$i" controlplane "-Dlmb.instance.id=$i"; done
# credentials: before the gateways and the drop copy, which poll it (they fall back to their cached copy)
plan access access-0 access "-Dlmb.instance.id=0"
for ((i = 0; i < CONS; i++)); do plan feeds "consolidator-$i" consolidator "-Dlmb.instance.id=$i"; done
[ "$WITH_FEEDSIM" -eq 1 ] && plan feeds feedsim-0 feedsim "-Dlmb.instance.id=0"
for ((i = 0; i < MDS; i++)); do plan marketdata "marketdata-$i" marketdata "-Dlmb.instance.id=$i"; done
for ((i = 0; i < GATEWAYS; i++)); do plan gateways "gateway-$i" gateway "-Dlmb.instance.id=$i"; done
[ "$WITH_POSTTRADE" -eq 1 ] && plan posttrade posttrade-0 posttrade "-Dlmb.instance.id=0"
[ "$WITH_LEDGER" -eq 1 ] && plan ledger ledger-0 ledger "-Dlmb.instance.id=0"
if [ "$WITH_REPLAYER" -eq 1 ]; then
  RBASE="$(topo_get lmb.replayer.metricsPort 9900)"
  for ((s = 0; s < SHARDS; s++)); do
    plan replayers "replayer-$s" replayer "-Dlmb.shard.id=$s" "-Dlmb.instance.id=$s" \
      "-Dlmb.replayer.metricsPort=$((RBASE + s))"
  done
fi
[ "${#PLAN[@]}" -gt 0 ] || die "nothing to start"
plan_names() { local e; for e in "${PLAN[@]}"; do set -- $e; echo "$2"; done; }

if [ "$MEMBERS" -eq 2 ]; then
  NOTE_MEMBERS="2 members per shard replicate but tolerate no failure (both must be up); use --members 3 for failover drills"
else
  NOTE_MEMBERS="$MEMBERS member(s) per shard"
fi

# ---- generated run configuration -------------------------------------------------------------------------
# Writes $1/topology.properties: base + profile overrides. No reference data files: symbols, firms and teams live
# in the control plane's and the access service's databases (scripts/local-seed fills them through the APIs).
generate_run_config() {
  local out="$1" url
  mkdir -p "$out"
  url="http://127.0.0.1:$(topo_get lmb.access.internalPort 9760)"
  {
    echo "# GENERATED by scripts/local-up.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ); rewritten on every start."
    echo "# Base: ${BASE_TOPO#"$LMB_ROOT/"}. Every service of this run reads this file (-Dlmb.topology)."
    echo
    cat "$BASE_TOPO"
    echo
    echo "# ---- profile of this run (later keys win) ----"
    echo "lmb.shards=$SHARDS"
    echo "lmb.members=$MEMBERS"
    echo "lmb.gateways=$GATEWAYS"
    echo "lmb.md.instances=$MDS"
    echo "lmb.consolidators=$CONS"
    echo "lmb.controlplane.instances=$CPS"
    echo "# credentials from the access service, symbols from the control plane (local tokens)"
    echo "lmb.gateway.access.url=$url"
    echo "lmb.posttrade.access.url=$url"
    echo "# the keys scripts/local-seed issued, for the tools (SmokeTestMain, LoadGeneratorMain)"
    echo "lmb.tools.credentials=$LMB_DATA/credentials.json"
  } > "$out/topology.properties"
}

# seed: symbols spread over the shards and risk accounts (control plane), teams and keys (access service)
run_seed() {
  local cp_url acc_args=()
  cp_url="http://127.0.0.1:$(topo_get lmb.controlplane.0.restPort 9700)"
  skipped access-0 || acc_args=(--access "http://127.0.0.1:$(topo_get lmb.access.adminPort 9750)")
  info "seeding symbols, firms, teams and keys through the APIs (scripts/local-seed)"
  if ! python3 "$LMB_ROOT/scripts/local-seed" --controlplane "$cp_url" ${acc_args[@]+"${acc_args[@]}"} --shards "$SHARDS" \
       --credentials "$LMB_DATA/credentials.json" 2>&1 | sed 's/^/    /'; then
    FAILED_NAMES=(seed); err "scripts/local-seed failed"; on_failure
  fi
}

q() { printf '%q' "$1"; }

# write_cmd <dir> <name> <service> <identity args...>: the per-process launch script.
write_cmd() {
  local dir="$1" name="$2" svc="$3"; shift 3
  local main="${LMB_MAIN[$svc]}" heap_var heap t spec f
  heap_var="LMB_HEAP_${svc^^}"
  heap="${!heap_var:-${LMB_DEFAULT_HEAP[$svc]}}"
  for spec in "${CLI_HEAP[@]+"${CLI_HEAP[@]}"}"; do
    t="${spec%%=*}"; [ "$t" = all ] || [ "$t" = "$svc" ] || [ "$t" = "$name" ] && heap="${spec#*=}"
  done
  {
    echo '#!/usr/bin/env bash'
    echo "# GENERATED by scripts/local-up.sh: launch script of $name. Relaunch: scripts/local-up.sh --restart $name"
    echo 'set -a'
    echo "LMB_ROOT=$(q "$LMB_ROOT")"
    echo "LMB_DATA=$(q "$LMB_DATA")"
    echo "LMB_RUNNER=$(q "$LMB_RUNNER")"
    echo "LMB_SERVICE=$svc"
    echo "LMB_PROCESS=$name"
    echo "LMB_HEAP=$(q "$heap")"
    echo 'LMB_COMMON_ARGS=""'
    echo 'LMB_EXTRA_ARGS=""'
    for f in common.env $([ "$FULL" -eq 1 ] && echo full.env) "$svc.env" "$name.env"; do
      echo "[ -f \"\$LMB_ROOT/deploy/local/$f\" ] && . \"\$LMB_ROOT/deploy/local/$f\""
    done
    for spec in "${CLI_ENV[@]+"${CLI_ENV[@]}"}"; do
      t="${spec%%:*}"
      [ "$t" = all ] || [ "$t" = "$svc" ] || [ "$t" = "$name" ] || continue
      spec="${spec#*:}"
      [[ "${spec%%=*}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "--env: bad variable name in '${spec%%=*}'"
      echo "${spec%%=*}=$(q "${spec#*=}")"
    done
    for spec in "${CLI_HEAP[@]+"${CLI_HEAP[@]}"}"; do
      t="${spec%%=*}"; [ "$t" = all ] || [ "$t" = "$svc" ] || [ "$t" = "$name" ] && echo "LMB_HEAP=$(q "${spec#*=}")"
    done
    echo 'set +a'
    echo 'set -f  # LMB_COMMON_ARGS / LMB_EXTRA_ARGS are word-split, never globbed'
    echo 'cd "$LMB_ROOT"'
    printf 'args=(%s' "$(q "$main")"
    # Only nodes run a media driver in lmb.aeron.dir. For any other service lmb.aeron.dir would make
    # cluster-client connect to an external driver that does not exist:
    # they get an embedded client driver under the same per-process directory instead.
    printf ' %s' \
      "$(q "-Dlmb.topology=$LMB_TOPO_FILE")" \
      "$(q "-Dlmb.data.dir=$LMB_DATA")" \
      "$(q "-Daeron.dir=$LMB_SHM_DIR/$name")" \
      "$(q "$( [ "$svc" = node ] && echo "-Dlmb.aeron.dir=$LMB_SHM_DIR/$name" \
               || echo "-Dlmb.client.aeron.dir.base=$LMB_SHM_DIR/$name")")" \
      "$(q "-Dlmb.local.process=$name")" \
      "$(q "-Dlmb.local.tag=$LMB_TAG")"
    for f in "$@"; do printf ' %s' "$(q "$f")"; done
    echo ')'
    # shellcheck disable=SC2016
    echo 'args+=($LMB_COMMON_ARGS $LMB_EXTRA_ARGS)'
    for spec in "${CLI_JVM[@]+"${CLI_JVM[@]}"}"; do
      t="${spec%%:*}"
      [ "$t" = all ] || [ "$t" = "$svc" ] || [ "$t" = "$name" ] && echo "args+=($(q "${spec#*:}"))"
    done
    cat <<'EOF'
if [ -n "${LMB_DRY_RUN:-}" ]; then
  env | grep -E '^LMB_[A-Z0-9_]+=' | grep -vE '^LMB_(ROOT|DATA|RUNNER|SERVICE|PROCESS|HEAP|JAVA_OPTS|COMMON_ARGS|EXTRA_ARGS|DRY_RUN|LOCAL_DATA)=' \
    | sort | sed 's/^/    config env: /'
  printf '    LMB_HEAP=%q LMB_JAVA_OPTS=%q \\\n      ' "$LMB_HEAP" "${LMB_JAVA_OPTS:-}"
  printf '%q ' "$LMB_RUNNER" "${args[@]}"; echo
  exit 0
fi
export LMB_HEAP LMB_JAVA_OPTS
exec "$LMB_RUNNER" "${args[@]}"
EOF
  } > "$dir/$name.sh"
}

# ---- pre-flight ----------------------------------------------------------------------------------------------
PREFLIGHT_PROBLEMS=0
problem() { if [ "$DRY_RUN" -eq 1 ]; then warn "$*"; else err "$*"; fi; PREFLIGHT_PROBLEMS=$((PREFLIGHT_PROBLEMS + 1)); }

preflight() {
  local e svc name pids port mb total=0 avail jars need_mains=() missing_mains=() main found ports_tcp ports_udp s m base off
  info "pre-flight checks"
  # already running?
  pids="$(tagged_pids | tr '\n' ' ')"
  if [ -n "${pids// /}" ]; then
    die "a local system from $LMB_DATA is already running (pids: $pids). scripts/local-status.sh shows it; scripts/local-down.sh stops it."
  fi
  # runner and distribution
  [ -x "$LMB_RUNNER" ] || problem "runner not executable: $LMB_RUNNER"
  if [ "$(basename "$LMB_RUNNER")" = lmb-run ] || [ -n "${LMB_CHECK_MAINS:-}" ]; then
    if [ ! -d "$LMB_LIB" ]; then
      problem "distribution not found at $LMB_LIB: run ./mvnw -DskipTests install"
    else
      for e in "${PLAN[@]}"; do set -- $e; need_mains+=("${LMB_MAIN[$3]}"); done
      jars=("$LMB_LIB"/lmb-*.jar)
      if ! command -v unzip >/dev/null 2>&1; then
        warn "unzip not installed: cannot check that the main classes are in the distribution"
      else
        for main in $(printf '%s\n' "${need_mains[@]}" | sort -u); do
          found=0
          for e in "${jars[@]}"; do
            [ -f "$e" ] && unzip -Z1 "$e" 2>/dev/null | grep -qxF "${main//.//}.class" && { found=1; break; }
          done
          [ "$found" -eq 1 ] || missing_mains+=("$main")
        done
      fi
    fi
    "$LMB_JAVA" -version >/dev/null 2>&1 || problem "no java found ($LMB_JAVA)"
    # A service whose main class is not built yet is skipped with a message, not a failure
    # (modules land at different times). Without shard nodes nothing else can work, so that is fatal.
    if [ "${#missing_mains[@]}" -gt 0 ]; then
      local -a kept=()
      for e in "${PLAN[@]}"; do
        set -- $e
        if printf '%s\n' "${missing_mains[@]}" | grep -qxF "${LMB_MAIN[$3]}"; then
          SKIPPED_MISSING+=("$2")
          [ "$DRY_RUN" -eq 1 ] && kept+=("$e")   # a dry run still shows the command, marked SKIPPED
        else
          kept+=("$e")
        fi
      done
      PLAN=("${kept[@]+"${kept[@]}"}")
      printf '%s\n' "${missing_mains[@]}" | grep -qxF "${LMB_MAIN[node]}" \
        && problem "${LMB_MAIN[node]} is not in the distribution: nothing can run without shard nodes (./mvnw -DskipTests install)"
      for main in "${missing_mains[@]}"; do
        warn "SKIPPING $main: not in the distribution ($LMB_LIB/lmb-*.jar). Its module is not implemented yet or dist was not rebuilt (./mvnw -DskipTests install)."
      done
    fi
  fi
  # memory
  for e in "${PLAN[@]}"; do
    set -- $e; svc="$3"; name="$2"
    local hv="LMB_HEAP_${svc^^}"; local heap="${!hv:-${LMB_DEFAULT_HEAP[$svc]}}" spec t
    for spec in "${CLI_HEAP[@]+"${CLI_HEAP[@]}"}"; do
      t="${spec%%=*}"; [ "$t" = all ] || [ "$t" = "$svc" ] || [ "$t" = "$name" ] && heap="${spec#*=}"
    done
    mb="$(size_to_mb "$heap")"; total=$((total + mb + LMB_NONHEAP_ESTIMATE_MB))
  done
  avail="$(awk '/^MemAvailable:/ { print int($2 / 1024) }' /proc/meminfo 2>/dev/null || echo 0)"
  if [ "${avail:-0}" -gt 0 ] && [ "$total" -gt "$avail" ]; then
    problem "estimated footprint ${total} MB (max heaps + ${LMB_NONHEAP_ESTIMATE_MB} MB non-heap each) exceeds MemAvailable ${avail} MB; use --minimal, --shards 1 or --heap"
  else
    ok "${#PLAN[@]} processes, estimated worst-case footprint ${total} MB (MemAvailable ${avail:-?} MB)"
  fi
  # /dev/shm
  if [ -d /dev/shm ]; then
    local shm_free; shm_free="$(df -Pm /dev/shm | awk 'NR == 2 { print $4 }')"
    [ "${shm_free:-0}" -lt 1024 ] && problem "/dev/shm has only ${shm_free} MB free; Aeron needs room for log buffers"
  fi
  # ports
  if command -v ss >/dev/null 2>&1; then
    ports_tcp="$(ss -Hltn 2>/dev/null | awk '{ n = split($4, a, ":"); print a[n] }' | sort -u)"
    ports_udp="$(ss -Hlun 2>/dev/null | awk '{ n = split($4, a, ":"); print a[n] }' | sort -u)"
    for e in "${PLAN[@]}"; do
      set -- $e; name="$2"
      while IFS= read -r port; do
        [ -n "$port" ] || continue
        grep -qx "$port" <<< "$ports_tcp" && problem "TCP port $port (for $name) is already in use: $(ss -Hltnp "sport = :$port" 2>/dev/null | awk '{print $NF}' | head -n1)"
      done < <(tcp_ports_of "$name")
    done
    for ((s = 0; s < SHARDS; s++)); do
      for ((m = 0; m < MEMBERS; m++)); do
        base="$(node_base_port "$s" "$m")"
        for off in 0 1 2 3 4 5 6 7 8; do
          grep -qx "$((base + off))" <<< "$ports_udp" && problem "UDP port $((base + off)) (node-$s-$m) is already in use"
        done
      done
    done
  else
    warn "ss not installed: port collisions not checked"
  fi
  if [ "$FULL" -eq 1 ]; then
    for e in 127.0.0.1:5432:postgres 127.0.0.1:19092:redpanda 127.0.0.1:8123:clickhouse 127.0.0.1:9002:minio; do
      IFS=: read -r h port svc <<< "$e"
      if (exec 3<>"/dev/tcp/$h/$port") 2>/dev/null; then ok "$svc reachable on $h:$port"
      else problem "--full: $svc not reachable on $h:$port (docker compose -f deploy/local/docker-compose.yml up -d)"; fi
    done
  fi
  if [ "$PREFLIGHT_PROBLEMS" -gt 0 ] && [ "$DRY_RUN" -eq 0 ] && [ "$FORCE" -eq 0 ]; then
    die "$PREFLIGHT_PROBLEMS pre-flight problem(s); fix them or pass --force"
  fi
}

# ---- dry run -------------------------------------------------------------------------------------------------
if [ "$DRY_RUN" -eq 1 ]; then
  TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
  preflight
  generate_run_config "$TMP"
  info "dry run: profile ${SHARDS} shard(s) x ${MEMBERS} member(s); $NOTE_MEMBERS"
  info "would write $LMB_TOPO_FILE:"
  sed 's/^/    /' "$TMP/topology.properties"
  [ "$SEED" -eq 1 ] && info "after the control plane and access tiers: scripts/local-seed --shards $SHARDS --credentials $LMB_DATA/credentials.json"
  tier=""
  for e in "${PLAN[@]}"; do
    set -- $e; t="$1"; name="$2"; svc="$3"; shift 3
    if [ "$t" != "$tier" ]; then
      tier="$t"
      info "tier $tier (wait for LMB_READY, timeout ${TIER_TIMEOUT}s)"
    fi
    write_cmd "$TMP" "$name" "$svc" "$@"
    if printf '%s\n' "${SKIPPED_MISSING[@]+"${SKIPPED_MISSING[@]}"}" | grep -qxF "$name"; then
      echo "  $name   ${C_YEL}SKIPPED on a real run: ${LMB_MAIN[$svc]} not built yet${C_OFF}"
    else
      echo "  $name   > $(log_file "$name"), pid > $(pid_file "$name")"
    fi
    LMB_DRY_RUN=1 bash "$TMP/$name.sh"
    if [ "$tier" = nodes ] && [ "$name" = "node-$((SHARDS - 1))-$((MEMBERS - 1))" ]; then
      info "then wait up to ${LEADER_TIMEOUT}s for a leader in each shard (methods: ${LMB_LEADER_METHODS:-hook metrics clustertool})"
    fi
  done
  info "dry run only: nothing was started. Launch scripts would go to $LMB_CMD_DIR/<name>.sh"
  exit 0
fi

# ---- start ------------------------------------------------------------------------------------------------------
preflight
mkdir -p "$LMB_LOG_DIR" "$LMB_PID_DIR" "$LMB_CMD_DIR" "$LMB_SHM_DIR"
rm -f "$LMB_CMD_DIR"/*.sh "$LMB_PID_DIR"/*.pid "$LMB_RUN_DIR/start-order"
generate_run_config "$LMB_RUN_DIR"
topo_load "$LMB_TOPO_FILE"

for e in "${PLAN[@]}"; do
  set -- $e; t="$1"; name="$2"; svc="$3"; shift 3
  write_cmd "$LMB_CMD_DIR" "$name" "$svc" "$@"
  echo "$t $name" >> "$LMB_RUN_DIR/start-order"
done
# Scrape targets for the optional Prometheus in deploy/local/docker-compose.yml (file_sd).
if command -v python3 >/dev/null 2>&1; then
  mkdir -p "$LMB_RUN_DIR/prometheus"
  python3 "$LMB_ROOT/deploy/monitoring/render.py" file-sd --topology "$LMB_TOPO_FILE" \
    --local-run "$LMB_RUN_DIR/start-order" --out "$LMB_RUN_DIR/prometheus/targets.json" \
    || warn "could not write Prometheus targets (deploy/monitoring/render.py)"
fi

on_failure() {
  if [ "$KEEP_ON_FAILURE" -eq 1 ]; then
    warn "--keep-on-failure: leaving the started processes running. Inspect with scripts/local-status.sh and scripts/local-logs.sh; stop with scripts/local-down.sh"
  else
    info "start failed: stopping what was started (use --keep-on-failure to inspect a live system)"
    "$LMB_ROOT/scripts/local-down.sh" --grace 10 || true
  fi
  err "local system did NOT come up (failed: ${FAILED_NAMES[*]:-?})"
  exit 1
}
trap 'echo; err "interrupted"; FAILED_NAMES=(interrupted); on_failure' INT TERM

info "starting ${#PLAN[@]} processes from $LMB_ROOT (data: $LMB_DATA)"
log "    profile: ${SHARDS} shard(s) x ${MEMBERS} member(s), ${GATEWAYS} gateway(s), ${MDS} MD, ${CONS} consolidator(s), ${CPS} control plane(s); $NOTE_MEMBERS"
[ "$(basename "$LMB_RUNNER")" = lmb-run ] || warn "runner is $LMB_RUNNER (not the JVM)"
START_ALL=$SECONDS
SEED_AFTER=""
if [ "$SEED" -eq 1 ]; then
  for tier in $(started_tiers); do case "$tier" in controlplane|access) SEED_AFTER="$tier" ;; esac; done
  [ -n "$SEED_AFTER" ] || warn "no control plane in this run: not seeding (scripts/local-seed)"
fi
for tier in $(started_tiers); do
  mapfile -t names < <(names_in_tier "$tier")
  info "tier $tier: ${names[*]}"
  for name in "${names[@]}"; do launch "$name"; done
  wait_ready "$TIER_TIMEOUT" "${names[@]}" || on_failure
  if [ "$tier" = "$SEED_AFTER" ]; then
    if [ "$(basename "$LMB_RUNNER")" = lmb-run ]; then run_seed; else warn "runner is not the JVM: not seeding"; fi
  fi
  if [ "$tier" = nodes ]; then
    info "waiting for a leader in each shard (timeout ${LEADER_TIMEOUT}s)"
    for ((s = 0; s < SHARDS; s++)); do
      skipped "node-$s-0" && continue
      start=$SECONDS; leader=""
      while [ $((SECONDS - start)) -lt "$LEADER_TIMEOUT" ]; do
        if shard_leader "$s"; then leader="$LMB_LEADER"; break; fi
        leader=""; sleep 1
      done
      if [ -n "$leader" ]; then
        ok "shard $s leader is member $leader ($((SECONDS - start))s)"
      elif [ "$STRICT_LEADERS" -eq 1 ]; then
        FAILED_NAMES=("shard-$s-leader")
        err "no leader found for shard $s within ${LEADER_TIMEOUT}s: $LMB_LEADER_DIAG"
        on_failure
      else
        warn "could not determine the leader of shard $s within ${LEADER_TIMEOUT}s; continuing (the nodes printed LMB_READY). Details: ${LMB_LEADER_DIAG:-none}"
      fi
    done
  fi
done
trap - INT TERM

# ---- summary ------------------------------------------------------------------------------------------------------
info "local venue is up in $((SECONDS - START_ALL))s"
[ "${#SKIPPED_MISSING[@]}" -gt 0 ] && warn "not started (main class not built yet): ${SKIPPED_MISSING[*]}"
for name in $(started_names); do
  case "$name" in
    gateway-*)      i="${name#gateway-}"
                    log "    $name  gRPC $(host_of "$name"):$(topo_get "lmb.gateway.$i.grpcPort" $((9100 + i)))  FIX $(host_of "$name"):$(topo_get "lmb.gateway.$i.fixPort" $((9200 + i)))" ;;
    marketdata-*)   i="${name#marketdata-}"
                    log "    $name  gRPC $(host_of "$name"):$(topo_get "lmb.md.$i.grpcPort" $((9510 + i)))  retransmit $(topo_get "lmb.md.$i.retransmitPort" $((9500 + i)))" ;;
    controlplane-*) i="${name#controlplane-}"
                    log "    $name  admin REST http://$(host_of "$name"):$(topo_get "lmb.controlplane.$i.restPort" $((9700 + i)))/" ;;
    posttrade-*)    log "    $name  FIX drop copy 127.0.0.1:$(topo_get lmb.posttrade.dropCopyFixPort 9800)" ;;
    ledger-*)       log "    $name  daily statements in $LMB_DATA/ledger/${name#ledger-}/statements" ;;
    access-*)       i="${name#access-}"
                    log "    $name  teams and keys http://127.0.0.1:$(( $(topo_get lmb.access.adminPort 9750) + i ))/api/firms  (scripts/lmb-team)" ;;
  esac
done
log "    metrics: http://127.0.0.1:<port>/metrics (scripts/local-status.sh --probe lists them)"
log "    logs:    $LMB_LOG_DIR   (scripts/local-logs.sh <name> -f)"
log "    stop:    scripts/local-down.sh   (--wipe also deletes $LMB_DATA)"
