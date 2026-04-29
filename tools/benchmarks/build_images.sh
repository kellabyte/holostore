#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/benchmark_common.sh"

TARGET="${TARGET:-both}"

usage() {
  cat <<EOF
usage: $0 [options]

Build benchmark Docker images from staged contexts under .tmp/benchmarks/build.

Options:
  --target holostore|etcd|both  Build images needed for this benchmark target
  --platform PLATFORM           Docker platform for benchmark images. Use
                                linux/arm64 for native Apple Silicon, or
                                native to match the Docker server architecture
                                (default: native).
  --holostore-build-mode host|docker
                                Build HoloStore on the host for Linux, or use
                                the slower Docker builder fallback
  --holostore-target TARGET     Rust Linux target for the HoloStore binary
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --platform) BENCHMARK_PLATFORM="$2"; BENCHMARK_PLATFORM_RESOLVED=0; shift 2 ;;
    --holostore-build-mode) BENCHMARK_HOLOSTORE_BUILD_MODE="$2"; shift 2 ;;
    --holostore-target) BENCHMARK_HOLOSTORE_TARGET="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$TARGET" in
  holostore|etcd|both) ;;
  *) echo "--target must be holostore, etcd, or both" >&2; exit 2 ;;
esac

build_benchmark_images "$TARGET"
