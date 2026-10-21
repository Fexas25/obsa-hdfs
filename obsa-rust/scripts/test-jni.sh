#!/usr/bin/env bash
# POSIX counterpart of test-jni.ps1: real-JVM JNI smoke tests against a built
# native library. Usage: scripts/test-jni.sh [debug|release]  (JAVA_HOME optional)
set -euo pipefail
profile="${1:-debug}"
case "$profile" in debug|release) ;; *) echo "profile must be debug or release" >&2; exit 2 ;; esac
root="$(cd "$(dirname "$0")/.." && pwd)"
java="${JAVA_HOME:+$JAVA_HOME/bin/}java"
javac="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
lib_dir="$root/target/$profile"
case "$(uname -s)" in
  Darwin) lib="$lib_dir/libobs_jni.dylib" ;;
  *) lib="$lib_dir/libobs_jni.so" ;;
esac
if [ ! -f "$lib" ]; then
  echo "Build the $profile JNI library first (cargo build --locked -p obs-jni, plus --release for release): $lib" >&2
  exit 1
fi
echo "JNI smoke native library: $lib"
classes="$root/target/java-smoke"
mkdir -p "$classes"
"$javac" -encoding UTF-8 -d "$classes" \
  "$root/java/org/apache/hadoop/fs/obs/NativeObs.java" "$root/java/tests/NativeSmokeTest.java"
"$java" "-Djava.library.path=$lib_dir" -cp "$classes" NativeSmokeTest
"$javac" -encoding UTF-8 -cp "$classes" -d "$classes" "$root/java/tests/NativeErrorSmokeTest.java"
"$java" "-Djava.library.path=$lib_dir" -cp "$classes" NativeErrorSmokeTest
