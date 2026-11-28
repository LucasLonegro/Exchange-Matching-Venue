# shellcheck shell=bash
# Shared helpers for scripts/local-*.sh and scripts/kill-leader.sh. Sourced, never executed.
#
# Layout of a local run (LMB_LOCAL_DATA overrides the root, default <repo>/data/local):
#   data/local/logs/<name>.log      stdout+stderr of each process (previous run kept as <name>.log.prev)
#   data/local/pids/<name>.pid      pid of the JVM (lmb-run execs java, so the pid is the JVM's)
#   data/local/run/topology.properties   generated topology every process reads (-Dlmb.topology)
#   data/local/run/cmd/<name>.sh    exact launch script per process (relaunch: local-up.sh --restart <name>)
#   data/local/run/start-order      "<tier> <name>" lines in start order (down stops in reverse)
#   data/local/<service>-<id>/...   service state (lmb.data.dir=data/local; owned by the services)
#   /dev/shm/lmb-local-<user>-<tag>/<name>   Aeron directory per process (removed by local-down.sh)
# Process names: node-<shard>-<member>, gateway-<i>, marketdata-<i>, consolidator-<i>, feedsim-0,
# controlplane-<i>, access-0, posttrade-0, ledger-0, replayer-<shard>.

LMB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LMB_DATA="${LMB_LOCAL_DATA:-$LMB_ROOT/data/local}"
LMB_LOG_DIR="$LMB_DATA/logs"
LMB_PID_DIR="$LMB_DATA/pids"
LMB_RUN_DIR="$LMB_DATA/run"
LMB_CMD_DIR="$LMB_RUN_DIR/cmd"
LMB_TOPO_FILE="${LMB_TOPO_FILE:-$LMB_RUN_DIR/topology.properties}"
LMB_RUNNER="${LMB_RUNNER:-$LMB_ROOT/scripts/lmb-run}"
LMB_LIB="${LMB_LIB:-$LMB_ROOT/dist/target/lmb/lib}"
# Tag carried by every process we start (-Dlmb.local.tag=...): identifies our JVMs even when pid
# files are lost, and keeps two data directories (two checkouts) from stopping each other's processes.
LMB_TAG="lmb-local-$(printf '%s' "$LMB_DATA" | cksum | cut -d' ' -f1)"
LMB_SHM_DIR="${LMB_SHM_DIR:-/dev/shm/lmb-local-$(id -un)-${LMB_TAG#lmb-local-}}"

LMB_JAVA="java"
if [ -x "$LMB_ROOT/.tools/jdk/bin/java" ]; then
  LMB_JAVA="$LMB_ROOT/.tools/jdk/bin/java"
elif [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  LMB_JAVA="$JAVA_HOME/bin/java"
fi

# Service catalogue: short name -> main class (ARCHITECTURE.md section 5).
declare -gA LMB_MAIN=(
  [node]=io.lmb.cluster.ClusterNodeMain
  [backup]=io.lmb.cluster.ClusterBackupMain
  [gateway]=io.lmb.gateway.GatewayMain
  [marketdata]=io.lmb.marketdata.MarketDataPublisherMain
  [feedsim]=io.lmb.feeds.FeedSimulatorMain
  [consolidator]=io.lmb.feeds.ConsolidatorMain
  [controlplane]=io.lmb.controlplane.ControlPlaneMain
  [posttrade]=io.lmb.posttrade.PostTradeMain
  [ledger]=io.lmb.ledger.LedgerMain
  [access]=io.lmb.access.AccessMain
  [replayer]=io.lmb.replayer.ShadowReplayerMain
)
# Default max heap per service for the local profile. Whole default profile (14 JVMs) is about
# 1.7 GB of heap plus ~80 MB non-heap each (capped in deploy/local/common.env): under ~3 GB RSS.
declare -gA LMB_DEFAULT_HEAP=(
  [node]=160m [backup]=128m [gateway]=96m [marketdata]=96m [feedsim]=64m [consolidator]=64m
  [controlplane]=128m [posttrade]=96m [ledger]=96m [replayer]=96m [access]=64m
)
LMB_NONHEAP_ESTIMATE_MB=80

# Metric names (the single reconciliation point shared with the Prometheus rules).
if [ -f "$LMB_ROOT/deploy/monitoring/metric-names.env" ]; then
  # shellcheck source=../../deploy/monitoring/metric-names.env
  . "$LMB_ROOT/deploy/monitoring/metric-names.env"
fi

# ---- output ---------------------------------------------------------------------------------------
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_RED=$'\e[31m'; C_GRN=$'\e[32m'; C_YEL=$'\e[33m'; C_DIM=$'\e[2m'; C_BLD=$'\e[1m'; C_OFF=$'\e[0m'
else
  C_RED=""; C_GRN=""; C_YEL=""; C_DIM=""; C_BLD=""; C_OFF=""
fi
log()  { printf '%s\n' "$*"; }
info() { printf '%s==>%s %s\n' "$C_BLD" "$C_OFF" "$*"; }
ok()   { printf '  %sok%s   %s\n' "$C_GRN" "$C_OFF" "$*"; }
warn() { printf '%swarning:%s %s\n' "$C_YEL" "$C_OFF" "$*" >&2; }
err()  { printf '%serror:%s %s\n' "$C_RED" "$C_OFF" "$*" >&2; }
die()  { err "$*"; exit 1; }

# ---- topology (mirrors io.lmb.common.Topology: env/system override, then file, then default) -------
declare -gA LMB_TOPO_KV=()
LMB_TOPO_LOADED=""

lmb_env_name() { local k="${1^^}"; k="${k//./_}"; printf '%s' "${k//-/_}"; }

# topo_load [file]: parse a java.util.Properties file (key=value or key:value, # and ! comments;
# no line continuations, which the topology files do not use). Later keys win, as in Properties.
topo_load() {
  local file="${1:-$LMB_TOPO_FILE}" line key val
  LMB_TOPO_KV=()
  LMB_TOPO_LOADED="$file"
  [ -f "$file" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line#"${line%%[![:space:]]*}"}"
    case "$line" in ''|'#'*|'!'*) continue ;; esac
    if [[ "$line" =~ ^([^=:[:space:]]+)[[:space:]]*[=:[:space:]][[:space:]]*(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"; val="${BASH_REMATCH[2]}"
      val="${val%"${val##*[![:space:]]}"}"
      LMB_TOPO_KV["$key"]="$val"
    fi
  done < "$file"
}

topo_get() {
  local key="$1" def="${2:-}" envn
  [ -n "$LMB_TOPO_LOADED" ] || topo_load
  envn="$(lmb_env_name "$key")"
  if [ -n "${!envn:-}" ]; then printf '%s' "${!envn}"; return; fi
  if [ -n "${LMB_TOPO_KV[$key]+x}" ]; then printf '%s' "${LMB_TOPO_KV[$key]}"; return; fi
  printf '%s' "$def"
}

shard_count()   { topo_get lmb.shards 1; }
member_count()  { topo_get lmb.members 2; }
gateway_count() { topo_get lmb.gateways 2; }
md_count()      { topo_get lmb.md.instances 2; }
cons_count()    { topo_get lmb.consolidators 2; }
cp_count()      { topo_get lmb.controlplane.instances 2; }

node_host()         { topo_get "lmb.shard.$1.member.$2.host" 127.0.0.1; }
node_base_port()    { topo_get "lmb.shard.$1.member.$2.basePort" $((20000 + 1000 * $1 + 100 * $2)); }
node_metrics_port() { echo $(( $(node_base_port "$1" "$2") + 9 )); }   # Topology.METRICS_OFFSET

# Ports a process listens on (TCP), for pre-flight collision checks and status. Prints "port" lines.
# Node ports are mostly UDP (Aeron); they are checked separately.
tcp_ports_of() {
  local name="$1" svc i
  svc="${name%%-*}"; i="${name##*-}"
  case "$svc" in
    gateway)      topo_get "lmb.gateway.$i.grpcPort" $((9100 + i)); echo
                  topo_get "lmb.gateway.$i.fixPort" $((9200 + i)); echo ;;
    marketdata)   topo_get "lmb.md.$i.retransmitPort" $((9500 + i)); echo
                  topo_get "lmb.md.$i.grpcPort" $((9510 + i)); echo ;;
    controlplane) topo_get "lmb.controlplane.$i.restPort" $((9700 + i)); echo ;;
    access)       echo $(( $(topo_get lmb.access.adminPort 9750) + i ))
                  echo $(( $(topo_get lmb.access.internalPort 9760) + i )) ;;
    posttrade)    topo_get lmb.posttrade.dropCopyFixPort 9800; echo ;;
  esac
  metrics_port_of "$name"; echo
}

# metrics_port_of <name>: the Prometheus port a process serves on.
metrics_port_of() {
  local name="$1" svc rest s m i
  svc="${name%%-*}"; rest="${name#*-}"
  case "$svc" in
    node)         s="${rest%%-*}"; m="${rest#*-}"; node_metrics_port "$s" "$m" | tr -d '\n' ;;
    gateway)      i="$rest"; topo_get "lmb.gateway.$i.metricsPort" $((9300 + i)) ;;
    marketdata)   i="$rest"; topo_get "lmb.md.$i.metricsPort" $((9520 + i)) ;;
    consolidator) i="$rest"; topo_get "lmb.consolidator.$i.metricsPort" $((9600 + i)) ;;
    feedsim)      topo_get lmb.feedsim.metricsPort 9650 ;;
    controlplane) i="$rest"; topo_get "lmb.controlplane.$i.metricsPort" $((9710 + i)) ;;
    posttrade)    topo_get lmb.posttrade.metricsPort 9810 ;;
    ledger)       i="$rest"; echo $(( $(topo_get lmb.ledger.metricsPort 9820) + i )) | tr -d '\n' ;;
    access)       i="$rest"; echo $(( $(topo_get lmb.access.metricsPort 9770) + i )) | tr -d '\n' ;;
    # Topology has a single lmb.replayer.metricsPort; locally there is one replayer per shard, so
    # local-up.sh passes -Dlmb.replayer.metricsPort=<base + shard> (contract change request filed).
    replayer)     echo $(( $(topo_get lmb.replayer.metricsPort 9900) + rest )) | tr -d '\n' ;;
    *)            return 1 ;;
  esac
}

host_of() {
  local name="$1" svc rest
  svc="${name%%-*}"; rest="${name#*-}"
  case "$svc" in
    node)         node_host "${rest%%-*}" "${rest#*-}" ;;
    gateway)      topo_get "lmb.gateway.$rest.host" 127.0.0.1 ;;
    marketdata)   topo_get "lmb.md.$rest.host" 127.0.0.1 ;;
    controlplane) topo_get "lmb.controlplane.$rest.host" 127.0.0.1 ;;
    *)            printf '127.0.0.1' ;;
  esac
}

# ---- size helpers ------------------------------------------------------------------------------------
size_to_mb() {  # 160m -> 160, 1g -> 1024, 512k -> 1
  local v="${1,,}"
  case "$v" in
    *g) echo $(( ${v%g} * 1024 )) ;;
    *m) echo "${v%m}" ;;
    *k) echo $(( ${v%k} / 1024 )) ;;
    *)  echo $(( v / 1048576 )) ;;
  esac
}

# ---- processes -------------------------------------------------------------------------------------
pid_file() { printf '%s/%s.pid' "$LMB_PID_DIR" "$1"; }
log_file() { printf '%s/%s.log' "$LMB_LOG_DIR" "$1"; }

pid_of() {
  local f; f="$(pid_file "$1")"
  [ -f "$f" ] && tr -dc '0-9' < "$f"
}

# is_ours <pid>: the pid is alive and carries our tag (guards against pid reuse).
is_ours() {
  local pid="$1"
  [ -n "$pid" ] && [ -r "/proc/$pid/cmdline" ] || return 1
  tr '\0' '\n' < "/proc/$pid/cmdline" 2>/dev/null | grep -qxF -- "-Dlmb.local.tag=$LMB_TAG"
}

is_alive() { local p; p="$(pid_of "$1")"; is_ours "$p"; }

# is_ready <name>: the current log contains the LMB_READY line (ARCHITECTURE.md section 5).
is_ready() {
  local f; f="$(log_file "$1")"
  [ -f "$f" ] && grep -qE '^LMB_READY( |$)' "$f"
}

# All pids carrying our tag, including ones whose pid file was lost.
tagged_pids() { pgrep -f -- "-Dlmb.local.tag=$LMB_TAG" 2>/dev/null || true; }

# Names in start order, from the last local-up run.
started_names() {
  [ -f "$LMB_RUN_DIR/start-order" ] && awk '{print $2}' "$LMB_RUN_DIR/start-order"
}
started_tiers() {
  [ -f "$LMB_RUN_DIR/start-order" ] && awk '!seen[$1]++ {print $1}' "$LMB_RUN_DIR/start-order"
}
names_in_tier() {
  [ -f "$LMB_RUN_DIR/start-order" ] && awk -v t="$1" '$1 == t {print $2}' "$LMB_RUN_DIR/start-order"
}

# stop_names <grace-seconds> <name>...: SIGTERM all, wait, SIGKILL the rest. Prints one line each.
stop_names() {
  local grace="$1"; shift
  local name pid deadline any
  local -a live=()
  for name in "$@"; do
    pid="$(pid_of "$name")"
    if is_ours "$pid"; then
      kill -TERM "$pid" 2>/dev/null || true
      live+=("$name")
    fi
  done
  [ "${#live[@]}" -gt 0 ] || return 0
  deadline=$((SECONDS + grace))
  while :; do
    any=0
    for name in "${live[@]}"; do
      is_ours "$(pid_of "$name")" && any=1
    done
    [ "$any" -eq 0 ] && break
    [ "$SECONDS" -ge "$deadline" ] && break
    sleep 0.2
  done
  for name in "${live[@]}"; do
    pid="$(pid_of "$name")"
    if is_ours "$pid"; then
      kill -KILL "$pid" 2>/dev/null || true
      printf '  %skill%s %s (pid %s did not exit within %ss of SIGTERM)\n' "$C_YEL" "$C_OFF" "$name" "$pid" "$grace"
    else
      printf '  %sstop%s %s\n' "$C_GRN" "$C_OFF" "$name"
    fi
    rm -f "$(pid_file "$name")"
  done
}

# ---- leader detection --------------------------------------------------------------------------------
# shard_leader <shard>: prints the leader's member id, returns 1 if it cannot be determined.
# Methods, in order (LMB_LEADER_METHODS="hook metrics clustertool" by default):
#   hook        LMB_LEADER_CMD="<command>" is run with the shard id as argument and prints the member id.
#   metrics     each member's Prometheus endpoint (memberBasePort + 9): CLUSTER_ROLE == 2 (Aeron
#               Cluster.Role.LEADER) or CLUSTER_LEADER_MEMBER_ID >= 0 (metric-names.env).
#   clustertool Aeron's io.aeron.cluster.ClusterTool <cluster-dir> list-members on each member's
#               cluster directory (found under data/local, or LMB_CLUSTER_DIR_TEMPLATE with
#               {data} {shard} {member} placeholders). Starts a short-lived JVM per call.
LMB_LEADER_DIAG=""

metric_value() {  # metric_value <metric> <text>: first sample value of <metric> (any labels)
  local name="$1"
  awk -v n="$name" '
    /^#/ { next }
    {
      line = $0
      if (index(line, n) != 1) next
      rest = substr(line, length(n) + 1)
      if (rest ~ /^\{/) { close_at = index(rest, "}"); if (close_at == 0) next; rest = substr(rest, close_at + 1) }
      else if (rest !~ /^[ \t]/) next
      sub(/^[ \t]+/, "", rest); split(rest, parts, /[ \t]+/); print parts[1]; exit
    }' <<< "$2"
}

leader_by_metrics() {
  local s="$1" m host port body v n
  n="$(member_count)"
  command -v curl >/dev/null 2>&1 || { LMB_LEADER_DIAG+="metrics: curl not installed; "; return 1; }
  for ((m = 0; m < n; m++)); do
    host="$(node_host "$s" "$m")"; port="$(node_metrics_port "$s" "$m")"
    body="$(curl -fsS --max-time 1 "http://$host:$port/metrics" 2>/dev/null)" || {
      LMB_LEADER_DIAG+="metrics: no answer from node-$s-$m at $host:$port; "; continue; }
    if [ -n "${CLUSTER_ROLE:-}" ]; then
      v="$(metric_value "$CLUSTER_ROLE" "$body")"
      if [ -n "$v" ]; then
        if awk -v x="$v" 'BEGIN { exit !(x + 0 == 2) }'; then LMB_LEADER="$m"; return 0; fi
        continue
      fi
    fi
    if [ -n "${CLUSTER_LEADER_MEMBER_ID:-}" ]; then
      v="$(metric_value "$CLUSTER_LEADER_MEMBER_ID" "$body")"
      if [ -n "$v" ] && awk -v x="$v" 'BEGIN { exit !(x + 0 >= 0) }'; then
        LMB_LEADER="${v%%.*}"; return 0
      fi
    fi
    LMB_LEADER_DIAG+="metrics: node-$s-$m serves metrics but neither ${CLUSTER_ROLE:-?} nor ${CLUSTER_LEADER_MEMBER_ID:-?} (deploy/monitoring/metric-names.env); "
  done
  LMB_LEADER_DIAG+="metrics: no member of shard $s reports itself leader (election in progress or no quorum); "
  return 1
}

# cluster_dirs <shard> <member>: candidate Aeron cluster directories (containing cluster-mark.dat).
cluster_dirs() {
  local s="$1" m="$2" d
  if [ -n "${LMB_CLUSTER_DIR_TEMPLATE:-}" ]; then
    d="${LMB_CLUSTER_DIR_TEMPLATE//\{data\}/$LMB_DATA}"; d="${d//\{shard\}/$s}"; d="${d//\{member\}/$m}"
    [ -f "$d/cluster-mark.dat" ] && echo "$d"
    return 0
  fi
  # cluster-node's layout (NodeConfig.memberDir): <lmb.data.dir>/cluster-node/shard-S-member-M/cluster
  d="$LMB_DATA/cluster-node/shard-$s-member-$m/cluster"
  if [ -f "$d/cluster-mark.dat" ]; then echo "$d"; return 0; fi
  [ -d "$LMB_DATA" ] || return 0
  find "$LMB_DATA" -maxdepth 6 -name cluster-mark.dat -printf '%h\n' 2>/dev/null \
    | grep -E "(node|cluster|shard)[^/]*[-_.]${s}[-_./](member[-_]?)?${m}(/|$)|shard[-_]?${s}/(member|node)[-_]?${m}(/|$)" \
    || true
}

clustertool() {  # clustertool <dir> <command>
  timeout 20 "$LMB_JAVA" -Xmx64m -XX:+UseSerialGC -XX:TieredStopAtLevel=1 \
    -cp "$LMB_LIB/*" io.aeron.cluster.ClusterTool "$1" "$2" 2>&1
}

leader_by_clustertool() {
  local s="$1" m n dir out id
  n="$(member_count)"
  [ -d "$LMB_LIB" ] || { LMB_LEADER_DIAG+="clustertool: no distribution at $LMB_LIB; "; return 1; }
  for ((m = 0; m < n; m++)); do
    while IFS= read -r dir; do
      [ -n "$dir" ] || continue
      out="$(clustertool "$dir" list-members)"
      id="$(sed -n 's/.*leaderMemberId=\(-\{0,1\}[0-9]\{1,\}\).*/\1/p' <<< "$out" | head -n1)"
      if [ -n "$id" ] && [ "$id" -ge 0 ]; then LMB_LEADER="$id"; return 0; fi
      LMB_LEADER_DIAG+="clustertool: $dir list-members gave no leader; "
    done < <(cluster_dirs "$s" "$m")
  done
  LMB_LEADER_DIAG+="clustertool: no usable cluster-mark.dat for shard $s under $LMB_DATA (set LMB_CLUSTER_DIR_TEMPLATE); "
  return 1
}

# shard_leader sets LMB_LEADER (member id) and returns 0, or returns 1 with the reasons in
# LMB_LEADER_DIAG. Call it directly, not in $(...), or the diagnostics are lost with the subshell.
LMB_LEADER=""
shard_leader() {
  local s="$1" method id
  LMB_LEADER_DIAG=""; LMB_LEADER=""
  for method in ${LMB_LEADER_METHODS:-hook metrics clustertool}; do
    case "$method" in
      hook)
        [ -n "${LMB_LEADER_CMD:-}" ] || continue
        id="$(bash -c "$LMB_LEADER_CMD \"\$1\"" _ "$s" 2>/dev/null | tr -dc '0-9')"
        [ -n "$id" ] && { LMB_LEADER="$id"; return 0; }
        LMB_LEADER_DIAG+="hook: LMB_LEADER_CMD printed nothing; " ;;
      metrics)     leader_by_metrics "$s" && return 0 ;;
      clustertool) leader_by_clustertool "$s" && return 0 ;;
      *)           LMB_LEADER_DIAG+="unknown method '$method'; " ;;
    esac
  done
  return 1
}
