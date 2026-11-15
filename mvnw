#!/usr/bin/env bash
# Project Maven launcher. Uses the JDK and Maven under .tools/ when present,
# otherwise falls back to whatever is on PATH.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -x "$ROOT/.tools/jdk/bin/java" ]; then
  export JAVA_HOME="$ROOT/.tools/jdk"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if [ -x "$ROOT/.tools/maven/bin/mvn" ]; then
  exec "$ROOT/.tools/maven/bin/mvn" "$@"
fi
exec mvn "$@"
