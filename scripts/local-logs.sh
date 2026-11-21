#!/usr/bin/env bash
# Reads the logs of the local system (data/local/logs/<name>.log).
#
#   scripts/local-logs.sh                     list logs: size, READY or not, last line
#   scripts/local-logs.sh gateway-0           last 100 lines of one log
#   scripts/local-logs.sh 'node-*' -f         follow several logs, each line prefixed with its name
#   scripts/local-logs.sh --errors [GLOB]     ERROR / WARN / exception lines across logs
#   scripts/local-logs.sh --grep REGEX [GLOB] lines matching REGEX across logs
#   -n N   lines of history (default 100)    --prev   the previous run's logs (<name>.log.prev)
set -euo pipefail
# shellcheck source=lib/local-common.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib/local-common.sh"

FOLLOW=0; LINES=100; MODE=show; PATTERN=""; GLOB=""; SUFFIX=".log"
while [ $# -gt 0 ]; do
  case "$1" in
    -f|--follow) FOLLOW=1 ;;
    -n)          [[ "${2:-}" =~ ^[0-9]+$ ]] || die "-n needs a number"; LINES="$2"; shift ;;
    --errors)    MODE=grep; PATTERN='ERROR|WARN|Exception|Error:|FATAL|^Caused by' ;;
    --grep)      [ -n "${2:-}" ] || die "--grep needs a regex"; MODE=grep; PATTERN="$2"; shift ;;
    --prev)      SUFFIX=".log.prev" ;;
    -h|--help)   sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*)          die "unknown option '$1' (see --help)" ;;
    *)           GLOB="$1" ;;
  esac
  shift
done

[ -d "$LMB_LOG_DIR" ] || die "no logs under $LMB_LOG_DIR (scripts/local-up.sh writes them)"
shopt -s nullglob
files=("$LMB_LOG_DIR"/${GLOB:-*}"$SUFFIX")
[ "${#files[@]}" -gt 0 ] || die "no log matches '${GLOB:-*}$SUFFIX' in $LMB_LOG_DIR"
name_of() { local b; b="$(basename "$1")"; printf '%s' "${b%"$SUFFIX"}"; }

if [ "$MODE" = grep ]; then
  for f in "${files[@]}"; do
    grep -nE -- "$PATTERN" "$f" | sed "s/^/[$(name_of "$f")] /" || true
  done | tail -n "$LINES"
  exit 0
fi

if [ -z "$GLOB" ] && [ "$FOLLOW" -eq 0 ]; then
  for f in "${files[@]}"; do
    n="$(name_of "$f")"
    if grep -qE '^LMB_READY( |$)' "$f"; then r=READY; else r="-"; fi
    printf '%-16s %8s %-6s %s\n' "$n" "$(du -h "$f" | cut -f1)" "$r" "$(tail -n1 "$f" | cut -c1-100)"
  done
  exit 0
fi

if [ "${#files[@]}" -eq 1 ]; then
  if [ "$FOLLOW" -eq 1 ]; then exec tail -n "$LINES" -F "${files[0]}"; fi
  exec tail -n "$LINES" "${files[0]}"
fi

if [ "$FOLLOW" -eq 0 ]; then
  for f in "${files[@]}"; do tail -n "$LINES" "$f" | sed "s/^/[$(name_of "$f")] /"; done
  exit 0
fi
pids=()
trap 'kill "${pids[@]}" 2>/dev/null; exit 0' INT TERM EXIT
for f in "${files[@]}"; do
  tail -n "$LINES" -F "$f" 2>/dev/null | awk -v p="[$(name_of "$f")] " '{ print p $0; fflush() }' &
  pids+=("$!")
done
wait
