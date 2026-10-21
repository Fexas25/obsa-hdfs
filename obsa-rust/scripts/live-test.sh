#!/usr/bin/env bash
# One-command live validation against a real bucket (Huawei Cloud OBS/PFS or
# any S3-compatible server). Reads OBS_LIVE_* settings from a local env file
# kept OUTSIDE the repository, never prints credentials, and runs:
#   - Rust live tests (object flow, credential refresh, optional SSE and PFS)
#   - Hadoop official contracts LiveS3* through the real JNI library
# Every test works under its own prefix and deletes it afterwards.
#
#   scripts/live-test.sh /path/to/live.env
#
# See docs/live-test.env.example for the variables. Works in Git Bash on
# Windows and on Linux/macOS; needs Rust, JDK and Maven.
set -euo pipefail
env_file="${1:?usage: scripts/live-test.sh /path/to/live.env}"
root="$(cd "$(dirname "$0")/.." && pwd)"
case "$(cd "$(dirname "$env_file")" && pwd)/" in
  "$root"/*) echo "refusing: keep the env file outside the repository" >&2; exit 2 ;;
esac
set -a
# shellcheck disable=SC1090
. "$env_file"
set +a
for name in OBS_LIVE_ENDPOINT OBS_LIVE_ACCESS_KEY OBS_LIVE_SECRET_KEY OBS_LIVE_BUCKET; do
  [ -n "${!name:-}" ] || { echo "missing $name in $env_file" >&2; exit 2; }
done
echo "endpoint=$OBS_LIVE_ENDPOINT bucket=$OBS_LIVE_BUCKET signature=${OBS_LIVE_SIGNATURE:-v2}" \
  "sse=${OBS_LIVE_SSE_TYPE:-off} pfs_bucket=${OBS_LIVE_PFS_BUCKET:-off}"
cd "$root"
status=0
echo "=== Rust live tests"
cargo test --locked -p obs-rust-core --test live_s3 -- --nocapture --test-threads=1 2>&1 \
  | grep -E '^test |passed|skipped|panicked|test result' || status=1
echo "=== Hadoop live contracts"
cargo build --locked -p obs-jni
rm -rf hadoop-adapter/target/surefire-reports
mvn -q -f hadoop-adapter/pom.xml test -Dtest='LiveS3*' 2>&1 | grep -E '^\[ERROR\]   ' | head -20 || true
for report in hadoop-adapter/target/surefire-reports/TEST-*.xml; do
  grep -o 'tests="[0-9]*" errors="[0-9]*" skipped="[0-9]*" failures="[0-9]*"' "$report" | head -1
done | awk -F'"' '{t+=$2;e+=$4;s+=$6;f+=$8} END {print "HADOOP tests="t" errors="e" skipped="s" failures="f; exit (e+f>0)}' \
  || status=1
[ "$status" = 0 ] && echo "LIVE TESTS PASSED" || echo "LIVE TESTS FAILED"
exit $status
