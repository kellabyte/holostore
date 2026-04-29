#!/usr/bin/env bash
set -euo pipefail

# Shared helpers for the Docker-based benchmark workflow. Benchmark images are
# built from staged contexts under .tmp so Docker never needs the repo root as a
# build context.

BENCHMARK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$BENCHMARK_DIR/../.." && pwd)"
BENCHMARK_TMP_DIR="${BENCHMARK_TMP_DIR:-"$ROOT_DIR/.tmp/benchmarks"}"
BENCHMARK_BUILD_DIR="${BENCHMARK_BUILD_DIR:-"$BENCHMARK_TMP_DIR/build"}"
BENCHMARK_PLATFORM="${BENCHMARK_PLATFORM:-native}"
BENCHMARK_HOLOSTORE_BUILD_MODE="${BENCHMARK_HOLOSTORE_BUILD_MODE:-host}"
BENCHMARK_HOLOSTORE_TARGET="${BENCHMARK_HOLOSTORE_TARGET:-}"
BENCHMARK_OPEN_FILES_LIMIT="${BENCHMARK_OPEN_FILES_LIMIT:-32768}"
BENCHMARK_PLATFORM_RESOLVED=0
BENCHMARK_PLATFORM_WARNED=0

HOLOSTORE_BENCH_IMAGE="${HOLOSTORE_BENCH_IMAGE:-holostore-bench-node:local}"
BENCH_RUNNER_IMAGE="${BENCH_RUNNER_IMAGE:-holostore-bench-runner:local}"
export HOLOSTORE_BENCH_IMAGE BENCH_RUNNER_IMAGE BENCHMARK_OPEN_FILES_LIMIT

resolve_compose() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
    return 0
  fi
  echo "docker compose or docker-compose is required" >&2
  exit 2
}

docker_server_arch() {
  docker version --format '{{.Server.Arch}}' 2>/dev/null || uname -m
}

benchmark_platform_for_arch() {
  case "$1" in
    amd64|x86_64) printf '%s\n' linux/amd64 ;;
    arm64|aarch64) printf '%s\n' linux/arm64 ;;
    *) echo "unsupported Docker/server architecture for benchmark platform: $1" >&2; exit 2 ;;
  esac
}

normalize_benchmark_platform() {
  local platform="$1"
  case "$platform" in
    ""|native|auto) benchmark_platform_for_arch "$(docker_server_arch)" ;;
    docker-default|default|none) printf '%s\n' "" ;;
    amd64|x86_64|linux/amd64) printf '%s\n' linux/amd64 ;;
    arm64|aarch64|linux/arm64|linux/arm64/v8) printf '%s\n' linux/arm64 ;;
    *) echo "unsupported benchmark platform: $platform" >&2; exit 2 ;;
  esac
}

resolve_benchmark_platform() {
  if [[ "$BENCHMARK_PLATFORM_RESOLVED" == "1" ]]; then
    return 0
  fi
  BENCHMARK_PLATFORM="$(normalize_benchmark_platform "$BENCHMARK_PLATFORM")"
  export BENCHMARK_PLATFORM
  BENCHMARK_PLATFORM_RESOLVED=1
}

warn_if_benchmark_platform_is_not_native() {
  local server_platform

  resolve_benchmark_platform
  if [[ -z "$BENCHMARK_PLATFORM" || "$BENCHMARK_PLATFORM_WARNED" == "1" ]]; then
    return 0
  fi

  server_platform="$(benchmark_platform_for_arch "$(docker_server_arch)")"
  if [[ "$BENCHMARK_PLATFORM" != "$server_platform" ]]; then
    cat >&2 <<EOF
warning: benchmark platform $BENCHMARK_PLATFORM does not match Docker server platform $server_platform.
This can run through emulation and is not suitable for native performance measurements.
EOF
  fi
  BENCHMARK_PLATFORM_WARNED=1
}

raise_open_file_limit() {
  local requested="$1"
  local hard soft target

  [[ -n "$requested" ]] || return 0
  hard="$(ulimit -H -n 2>/dev/null || true)"
  soft="$(ulimit -S -n 2>/dev/null || true)"
  target="$requested"
  if [[ "$hard" != "unlimited" && "$hard" =~ ^[0-9]+$ && "$hard" -lt "$target" ]]; then
    target="$hard"
  fi
  if [[ "$soft" =~ ^[0-9]+$ && "$soft" -ge "$target" ]]; then
    return 0
  fi
  if ! ulimit -S -n "$target" 2>/dev/null; then
    echo "warning: unable to raise open-file limit to $target; Zig link may fail with ProcessFdQuotaExceeded" >&2
  fi
}

prepare_benchmark_tmp() {
  mkdir -p \
    "$BENCHMARK_BUILD_DIR" \
    "$BENCHMARK_BUILD_DIR/cargo-home" \
    "$BENCHMARK_BUILD_DIR/cargo-target" \
    "$BENCHMARK_BUILD_DIR/host-cargo-target" \
    "$BENCHMARK_BUILD_DIR/bin" \
    "$BENCHMARK_BUILD_DIR/go"
}

reset_benchmark_context() {
  local context_dir="$1"
  if [[ "$context_dir" != "$BENCHMARK_BUILD_DIR"/* ]]; then
    echo "refusing to reset benchmark context outside $BENCHMARK_BUILD_DIR: $context_dir" >&2
    exit 2
  fi
  rm -rf "$context_dir"
  mkdir -p "$context_dir"
}

prune_benchmark_context() {
  local context_dir="$1"
  find "$context_dir" -type d -name __pycache__ -prune -exec rm -rf {} +
  find "$context_dir" -type f \( -name '*.pyc' -o -name '.DS_Store' \) -delete
}

build_benchmark_holostore_image() {
  prepare_benchmark_tmp
  resolve_benchmark_platform

  local context_dir="$BENCHMARK_BUILD_DIR/holostore-context"
  local staged_binary="$BENCHMARK_BUILD_DIR/holo-store"
  local -a docker_build_cmd

  docker_build_cmd=(docker build)
  if [[ -n "$BENCHMARK_PLATFORM" ]]; then
    docker_build_cmd+=(--platform "$BENCHMARK_PLATFORM")
  fi

  build_benchmark_holostore_binary "$staged_binary"

  reset_benchmark_context "$context_dir"
  cp "$staged_binary" "$context_dir/holo-store"
  cp "$BENCHMARK_DIR/docker/holostore-entrypoint.sh" "$context_dir/holostore-entrypoint.sh"
  prune_benchmark_context "$context_dir"

  "${docker_build_cmd[@]}" \
    -f "$BENCHMARK_DIR/docker/Dockerfile.holostore" \
    -t "$HOLOSTORE_BENCH_IMAGE" \
    "$context_dir"
}

build_benchmark_holostore_binary() {
  local out="$1"

  case "$BENCHMARK_HOLOSTORE_BUILD_MODE" in
    host) build_benchmark_holostore_binary_host "$out" ;;
    docker) build_benchmark_holostore_binary_docker "$out" ;;
    *) echo "BENCHMARK_HOLOSTORE_BUILD_MODE must be host or docker" >&2; exit 2 ;;
  esac
}

build_benchmark_holostore_binary_host() {
  local out="$1"
  local target cargo_target source_binary

  target="$(holostore_linux_target)"
  cargo_target="$BENCHMARK_BUILD_DIR/host-cargo-target"
  source_binary="$cargo_target/$target/release/holo-store"

  ensure_rust_target "$target"
  configure_host_linux_build_env "$target"

  echo "building holo-store on host target=$target"
  (
    cd "$ROOT_DIR"
    raise_open_file_limit "$BENCHMARK_OPEN_FILES_LIMIT"
    env "${HOST_LINUX_BUILD_ENV[@]}" \
      CARGO_TARGET_DIR="$cargo_target" \
      cargo build -p holo_store --release --bin holo-store --target "$target"
  )

  cp "$source_binary" "$out"
  chmod +x "$out"
}

build_benchmark_holostore_binary_docker() {
  local out="$1"
  local cargo_home="$BENCHMARK_BUILD_DIR/cargo-home"
  local cargo_target="$BENCHMARK_BUILD_DIR/cargo-target"
  local source_binary="$cargo_target/release/holo-store"
  local -a docker_run_cmd

  resolve_benchmark_platform
  docker_run_cmd=(docker run --rm)
  if [[ -n "$BENCHMARK_PLATFORM" ]]; then
    docker_run_cmd+=(--platform "$BENCHMARK_PLATFORM")
  fi

  echo "building holo-store in Docker; set BENCHMARK_HOLOSTORE_BUILD_MODE=host for host cross builds"
  "${docker_run_cmd[@]}" \
    -e CARGO_HOME=/cargo \
    -e CARGO_TARGET_DIR=/benchmark-build/cargo-target \
    -v "$ROOT_DIR":/src \
    -v "$BENCHMARK_BUILD_DIR":/benchmark-build \
    -v "$cargo_home":/cargo \
    -w /src \
    rust:1-bookworm \
    bash -lc '
      set -euo pipefail
      export PATH=/usr/local/cargo/bin:$PATH
      cargo build -p holo_store --release --bin holo-store
    '

  cp "$source_binary" "$out"
  chmod +x "$out"
}

holostore_linux_target() {
  resolve_benchmark_platform

  if [[ -n "$BENCHMARK_HOLOSTORE_TARGET" ]]; then
    printf '%s\n' "$BENCHMARK_HOLOSTORE_TARGET"
    return 0
  fi

  case "$BENCHMARK_PLATFORM" in
    linux/amd64) printf '%s\n' x86_64-unknown-linux-musl; return 0 ;;
    linux/arm64|linux/arm64/v8) printf '%s\n' aarch64-unknown-linux-musl; return 0 ;;
    "") ;;
    *) echo "unsupported BENCHMARK_PLATFORM for host HoloStore build: $BENCHMARK_PLATFORM" >&2; exit 2 ;;
  esac

  local arch
  arch="$(docker version --format '{{.Server.Arch}}' 2>/dev/null || uname -m)"
  case "$arch" in
    amd64|x86_64) printf '%s\n' x86_64-unknown-linux-musl ;;
    arm64|aarch64) printf '%s\n' aarch64-unknown-linux-musl ;;
    *) echo "unsupported Docker/server architecture for host HoloStore build: $arch" >&2; exit 2 ;;
  esac
}

ensure_rust_target() {
  local target="$1"
  if rustup target list --installed | grep -qx "$target"; then
    return 0
  fi
  rustup target add "$target"
}

configure_host_linux_build_env() {
  local target="$1"
  local target_env cargo_env cc_bin zig_target cc_wrapper ar_wrapper ar_var

  target_env="${target//-/_}"
  cargo_env="$(printf '%s' "$target_env" | tr '[:lower:]' '[:upper:]')"
  cc_bin="$(cross_cc_for_target "$target")"

  HOST_LINUX_BUILD_ENV=()
  if [[ -n "$cc_bin" && -x "$(command -v "$cc_bin" 2>/dev/null || true)" ]]; then
    HOST_LINUX_BUILD_ENV+=("CC_${target_env}=$cc_bin")
    HOST_LINUX_BUILD_ENV+=("CARGO_TARGET_${cargo_env}_LINKER=$cc_bin")
    return 0
  fi

  if command -v zig >/dev/null 2>&1; then
    zig_target="$(zig_target_for_rust_target "$target")"
    cc_wrapper="$BENCHMARK_BUILD_DIR/bin/${target}-zig-cc"
    ar_wrapper="$BENCHMARK_BUILD_DIR/bin/${target}-zig-ar"
    cat >"$cc_wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ -n "\${BENCHMARK_OPEN_FILES_LIMIT:-}" ]]; then
  ulimit -S -n "\$BENCHMARK_OPEN_FILES_LIMIT" 2>/dev/null || true
fi
args=()
seen_target=0
while [[ \$# -gt 0 ]]; do
  case "\$1" in
    --target=*)
      args+=("-target" "$zig_target")
      seen_target=1
      ;;
    --target)
      shift
      args+=("-target" "$zig_target")
      seen_target=1
      ;;
    *)
      args+=("\$1")
      ;;
  esac
  shift
done
if [[ "\$seen_target" == "0" ]]; then
  args=("-target" "$zig_target" "\${args[@]}")
fi
exec zig cc "\${args[@]}"
EOF
    cat >"$ar_wrapper" <<'EOF'
#!/usr/bin/env bash
exec zig ar "$@"
EOF
    chmod +x "$cc_wrapper" "$ar_wrapper"

    ar_var="AR_${target_env}"
    HOST_LINUX_BUILD_ENV+=("CC_${target_env}=$cc_wrapper")
    HOST_LINUX_BUILD_ENV+=("$ar_var=$ar_wrapper")
    HOST_LINUX_BUILD_ENV+=("CARGO_TARGET_${cargo_env}_LINKER=$cc_wrapper")
    HOST_LINUX_BUILD_ENV+=("CARGO_TARGET_${cargo_env}_RUSTFLAGS=-Clink-self-contained=no")
    return 0
  fi

  cat >&2 <<EOF
error: host Linux HoloStore build requires a cross C toolchain for $target.

Install Zig, then rerun:
  brew install zig

Or install a target compiler such as:
  $cc_bin

For the slower container compile fallback:
  BENCHMARK_HOLOSTORE_BUILD_MODE=docker ./tools/benchmarks/build_images.sh --target holostore
EOF
  exit 2
}

cross_cc_for_target() {
  case "$1" in
    x86_64-unknown-linux-musl) printf '%s\n' x86_64-linux-musl-gcc ;;
    aarch64-unknown-linux-musl) printf '%s\n' aarch64-linux-musl-gcc ;;
    x86_64-unknown-linux-gnu) printf '%s\n' x86_64-linux-gnu-gcc ;;
    aarch64-unknown-linux-gnu) printf '%s\n' aarch64-linux-gnu-gcc ;;
    *) printf '%s\n' "" ;;
  esac
}

zig_target_for_rust_target() {
  case "$1" in
    x86_64-unknown-linux-musl) printf '%s\n' x86_64-linux-musl ;;
    aarch64-unknown-linux-musl) printf '%s\n' aarch64-linux-musl ;;
    x86_64-unknown-linux-gnu) printf '%s\n' x86_64-linux-gnu ;;
    aarch64-unknown-linux-gnu) printf '%s\n' aarch64-linux-gnu ;;
    *) echo "unsupported Rust target for Zig cross build: $1" >&2; exit 2 ;;
  esac
}

build_benchmark_runner_image() {
  prepare_benchmark_tmp
  resolve_benchmark_platform

  local context_dir="$BENCHMARK_BUILD_DIR/runner-context"
  local benchtool_bin="$BENCHMARK_BUILD_DIR/benchtool"
  local go_home="$BENCHMARK_BUILD_DIR/go"
  local -a docker_run_cmd docker_build_cmd

  docker_run_cmd=(docker run --rm)
  docker_build_cmd=(docker build)
  if [[ -n "$BENCHMARK_PLATFORM" ]]; then
    docker_run_cmd+=(--platform "$BENCHMARK_PLATFORM")
    docker_build_cmd+=(--platform "$BENCHMARK_PLATFORM")
  fi

  "${docker_run_cmd[@]}" \
    -v "$ROOT_DIR":/src \
    -v "$BENCHMARK_BUILD_DIR":/benchmark-build \
    -v "$go_home":/go \
    -w /src/tools/benchmarks/benchtool \
    golang:1.22-bookworm \
    bash -lc '
      set -euo pipefail
      export PATH=/usr/local/go/bin:$PATH
      GOCACHE=/go/build-cache CGO_ENABLED=0 go build -buildvcs=false -o /benchmark-build/benchtool .
    '

  reset_benchmark_context "$context_dir"
  cp "$benchtool_bin" "$context_dir/benchtool"
  cp "$BENCHMARK_DIR/report.py" "$context_dir/report.py"
  cp "$BENCHMARK_DIR/docker/requirements.txt" "$context_dir/requirements.txt"
  prune_benchmark_context "$context_dir"

  "${docker_build_cmd[@]}" \
    -f "$BENCHMARK_DIR/docker/Dockerfile.runner" \
    -t "$BENCH_RUNNER_IMAGE" \
    "$context_dir"
}

build_benchmark_images() {
  local target="$1"

  resolve_benchmark_platform
  warn_if_benchmark_platform_is_not_native
  build_benchmark_runner_image
  case "$target" in
    holostore|both) build_benchmark_holostore_image ;;
    etcd) ;;
    *) echo "target must be holostore, etcd, or both" >&2; exit 2 ;;
  esac

  echo "Built benchmark images:"
  echo "  platform:  ${BENCHMARK_PLATFORM:-docker default}"
  echo "  runner:    $BENCH_RUNNER_IMAGE"
  if [[ "$target" == "holostore" || "$target" == "both" ]]; then
    echo "  holostore: $HOLOSTORE_BENCH_IMAGE"
  fi
  echo
  echo "Staged contexts:"
  echo "  $BENCHMARK_BUILD_DIR/runner-context"
  if [[ "$target" == "holostore" || "$target" == "both" ]]; then
    echo "  $BENCHMARK_BUILD_DIR/holostore-context"
  fi
}
