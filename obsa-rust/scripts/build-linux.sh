#!/usr/bin/env bash
# Linux build of the workspace and the JNI library.
#
#   scripts/build-linux.sh [--release] [--glibc VERSION] [--skip-tests]
#
# --glibc builds obs-jni with cargo-zigbuild against the given glibc baseline
# (2.17 covers CentOS 7 / EulerOS 2.x and newer) and copies the result to
# target/<profile>/libobs_jni.so, where test-jni.sh and Maven look for it.
# A plain native build links against the build machine's glibc (e.g. 2.34+
# on Ubuntu 22.04), which older production nodes cannot load.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
profile=debug; flag=(); glibc=""; tests=1
while [ $# -gt 0 ]; do
  case "$1" in
    --release) profile=release; flag=(--release) ;;
    --glibc) glibc="${2:?--glibc needs a version, e.g. 2.17}"; shift ;;
    --skip-tests) tests=0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
cd "$root"
cargo build --locked "${flag[@]}" --workspace
if [ "$tests" = 1 ]; then cargo test --locked "${flag[@]}" --workspace; fi
if [ -n "$glibc" ]; then
  command -v zig >/dev/null || { echo "zig is required for --glibc (see docs/LINUX_BUILD.md)" >&2; exit 1; }
  target="x86_64-unknown-linux-gnu"
  cargo zigbuild --locked "${flag[@]}" -p obs-jni --target "$target.$glibc"
  mkdir -p "target/$profile"
  cp "target/$target/$profile/libobs_jni.so" "target/$profile/libobs_jni.so"
fi
lib="target/$profile/libobs_jni.so"
newest=$(objdump -T "$lib" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1)
echo "Built $lib (requires up to $newest)"
