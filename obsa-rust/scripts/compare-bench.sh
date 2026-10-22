#!/usr/bin/env bash
# Same-machine comparison of the Rust adapter (OBSFileSystemRust) with the Huawei Java OBSA
# (OBSFileSystem) through identical Hadoop API workloads (java/benchmarks/ObsCompareBench.java).
#
#   scripts/compare-bench.sh <env-file> <reference-jar> [results.jsonl]
#
# The env file holds OBS_LIVE_ENDPOINT/ACCESS_KEY/SECRET_KEY/BUCKET (see docs/live-test.env.example;
# keep it outside the repository). The reference jar is a released hadoop-huaweicloud-*.jar, which
# bundles the OBS SDK. Each workload runs in a fresh JVM; per round the order is
# java, rust, rust, java (baseline, candidate, candidate, baseline) so drift affects both.
#
# Tunables (environment): ROUNDS (3), THREADS (8), HADOOP_VERSION (3.3.6), NATIVE_PROFILE (release),
# NATIVE_LIB_DIR (target/<profile>), JAVA (java), JAVA_OPTS (-Xmx4g), BENCH_PREFIX (obsa-bench),
# WORKLOADS (one entry per line; default below), RUST_CONF / JAVA_CONF (one key=value per line,
# passed only to that implementation, e.g. to align multipart size or upload buffering).
# A workload entry is "name:extra args", e.g. "write-read:--files 8 --size-mb 256".
# Credentials are never printed. Run scripts/compare-bench-summary.py on the results.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
env_file="${1:?usage: compare-bench.sh <env-file> <reference-jar> [results.jsonl]}"
ref_jar="${2:?reference jar required}"
out="${3:-$root/target/compare-bench/results-$(date +%Y%m%d-%H%M%S).jsonl}"
case "$(cd "$(dirname "$env_file")" && pwd)/" in "$root"/*) echo "refusing an env file inside the repository" >&2; exit 2;; esac
[ -f "$ref_jar" ] || { echo "reference jar not found: $ref_jar" >&2; exit 2; }

RUST_CONF="${RUST_CONF:-}"; JAVA_CONF="${JAVA_CONF:-}"
rounds="${ROUNDS:-3}"; threads="${THREADS:-8}"; hadoop="${HADOOP_VERSION:-3.3.6}"
profile="${NATIVE_PROFILE:-release}"; java_opts="${JAVA_OPTS:--Xmx4g}"; prefix="${BENCH_PREFIX:-obsa-bench}"
default_workloads=(
  "write-read:--files 8 --size-mb 256"
  "random-read:--size-mb 1024 --reads 4000 --read-kb 64"
  "random-read:--size-mb 1024 --reads 8000 --read-kb 4"
  "metadata:--files 2000"
  "rename-dir:--files 500"
)
if [ -n "${WORKLOADS:-}" ]; then IFS=$'\n' read -r -d '' -a workloads < <(printf '%s\0' "$WORKLOADS") || true
else workloads=("${default_workloads[@]}"); fi

java_bin="${JAVA:-java}"
# Hadoop's UserGroupInformation calls Subject.getSubject, which JDK 18+ allows only with this flag.
major="$("$java_bin" -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+)(\.([0-9]+))?.*/\1 \3/' | awk '{print ($1 == 1) ? $2 : $1}')"
[ "${major:-0}" -ge 18 ] && java_opts="$java_opts -Djava.security.manager=allow"

sep=":"; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) sep=";" ;; esac
native() { if [ "$sep" = ";" ]; then cygpath -w "$1"; else printf '%s' "$1"; fi; }
work="$root/target/compare-bench"; mkdir -p "$work" "$(dirname "$out")"

mvn -q -f "$root/hadoop-adapter/pom.xml" package -DskipTests -Dhadoop.version="$hadoop"
adapter="$(ls "$root"/hadoop-adapter/target/obsa-rust-hadoop-*.jar | head -1)"
cp_file="$work/classpath-$hadoop.txt"
mvn -q -f "$root/hadoop-adapter/pom.xml" dependency:build-classpath \
  -Dhadoop.version="$hadoop" -Dmdep.includeScope=provided -Dmdep.outputFile="$cp_file"
# Java 8 bytecode runs on any JDK the cluster uses.
javac --release 8 -nowarn -cp "$(cat "$cp_file")" -d "$work/classes" "$root/java/benchmarks/ObsCompareBench.java"
libdir="${NATIVE_LIB_DIR:-$root/target/$profile}"
ls "$libdir"/*obs_jni* >/dev/null 2>&1 || { echo "native library missing in $libdir (build $profile first)" >&2; exit 2; }

classpath() {  # impl -> classpath
  local impl_jar; if [ "$1" = rust ]; then impl_jar="$adapter"; else impl_jar="$ref_jar"; fi
  printf '%s%s%s%s%s' "$(native "$work/classes")" "$sep" "$(native "$impl_jar")" "$sep" "$(cat "$cp_file")"
}

set -a; . "$env_file"; set +a
: "${OBS_LIVE_BUCKET:?OBS_LIVE_BUCKET not set}"
echo "bucket=$OBS_LIVE_BUCKET endpoint=$OBS_LIVE_ENDPOINT hadoop=$hadoop native=$profile java=$major rounds=$rounds threads=$threads"
echo "results: $out"
for round in $(seq 1 "$rounds"); do
  for entry in "${workloads[@]}"; do
    name="${entry%%:*}"; extra="${entry#*:}"; [ "$extra" = "$entry" ] && extra=""
    for impl in java rust rust java; do
      confs=()
      impl_conf="$RUST_CONF"; [ "$impl" = java ] && impl_conf="$JAVA_CONF"
      while IFS= read -r kv; do [ -n "$kv" ] && confs+=(--conf "$kv"); done <<< "$impl_conf"
      # shellcheck disable=SC2086
      line="$("$java_bin" $java_opts -Djava.library.path="$(native "$libdir")" -cp "$(classpath "$impl")" \
        ObsCompareBench --impl "$impl" --bucket "$OBS_LIVE_BUCKET" --workload "$name" \
        --threads "$threads" --prefix "$prefix" "${confs[@]}" $extra 2>"$work/last-stderr.txt" | tail -1)" || true
      [ -n "$line" ] || line="{\"impl\":\"$impl\",\"workload\":\"$name\",\"ok\":false,\"error\":\"no output; see $work/last-stderr.txt\"}"
      echo "{\"round\":$round,\"args\":\"$extra\",${line#\{}" | tee -a "$out" | cut -c1-200
    done
  done
done
