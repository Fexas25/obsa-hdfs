#!/usr/bin/env bash
# Load, verify (-Xverify:all) and initialize every adapter class against an
# older Hadoop, catching references to newer Hadoop APIs that would stop
# OBSFileSystemRust from loading there (e.g. on Huawei MRS with Hadoop 3.1.1).
#
#   scripts/check-hadoop-load.sh [HADOOP_VERSION...]   (default: 3.1.1)
#
# Works in Git Bash on Windows and on Linux/macOS. Needs JDK and Maven.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
versions=("$@"); [ ${#versions[@]} -gt 0 ] || versions=(3.1.1)
sep=":"; case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) sep=";" ;; esac
work="$root/target/hadoop-load-check"
mkdir -p "$work"
mvn -q -f "$root/hadoop-adapter/pom.xml" package -DskipTests
jar="$(ls "$root"/hadoop-adapter/target/obsa-rust-hadoop-*.jar | head -1)"
javac -d "$work" "$root/java/tests/HadoopLoadCheck.java"
# Windows JVMs need native paths on the classpath.
native() { if [ "$sep" = ";" ]; then cygpath -w "$1"; else printf '%s' "$1"; fi; }
status=0
for version in "${versions[@]}"; do
  cp_file="$work/classpath-$version.txt"
  mvn -q -f "$root/hadoop-adapter/pom.xml" dependency:build-classpath \
    -Dhadoop.version="$version" -Dmdep.includeScope=provided -Dmdep.outputFile="$cp_file"
  echo "=== Hadoop $version"
  result="$(java -Xverify:all -cp "$(native "$work")$sep$(native "$jar")$sep$(cat "$cp_file")" \
    HadoopLoadCheck "$(native "$jar")" 2>&1 | grep -v '^SLF4J\|^WARNING' || true)"
  echo "$result"
  echo "$result" | grep -q 'failures=0' || status=1
done
exit $status
