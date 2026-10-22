#!/usr/bin/env bash
# Prepare a fresh Linux ECS (EulerOS / CentOS / openEuler / Ubuntu) for scripts/compare-bench.sh:
# JDK 17, Maven, a Rust toolchain and the release native library, built from this checkout.
# Uses Huawei Cloud mirrors for packages, Maven and Rust so a same-region ECS needs no public
# internet beyond them. Run from the repository's obsa-rust directory:
#
#   bash scripts/ecs-setup.sh
#
# Not yet validated on a real ECS; adjust the mirrors if one is unreachable.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

if command -v dnf >/dev/null || command -v yum >/dev/null; then
  pm="$(command -v dnf || command -v yum)"
  sudo "$pm" install -y java-17-openjdk-devel maven git gcc make python3 \
    || sudo "$pm" install -y java-1.8.0-openjdk-devel maven git gcc make python3
elif command -v apt-get >/dev/null; then
  sudo apt-get update && sudo apt-get install -y openjdk-17-jdk-headless maven git build-essential python3
fi

# Maven through the Huawei Cloud mirror.
mkdir -p ~/.m2
[ -f ~/.m2/settings.xml ] || cat > ~/.m2/settings.xml <<'EOF'
<settings><mirrors><mirror><id>huaweicloud</id><mirrorOf>*</mirrorOf>
<url>https://repo.huaweicloud.com/repository/maven/</url></mirror></mirrors></settings>
EOF

# Rust through the Huawei Cloud rustup mirror; crates through its crates.io mirror.
if ! command -v cargo >/dev/null; then
  export RUSTUP_DIST_SERVER=https://repo.huaweicloud.com/rustup
  export RUSTUP_UPDATE_ROOT=https://repo.huaweicloud.com/rustup/rustup
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi
mkdir -p ~/.cargo
[ -f ~/.cargo/config.toml ] || cat > ~/.cargo/config.toml <<'EOF'
[source.crates-io]
replace-with = "huaweicloud"
[source.huaweicloud]
registry = "sparse+https://repo.huaweicloud.com/repository/crates.io-index/"
EOF

cargo build --locked --release -p obs-jni
ls -la target/release/libobs_jni.so
java -version 2>&1 | head -1
echo "ready: scripts/compare-bench.sh <env-file> <reference-jar>"
