.PHONY: all build build-release build-release-mimalloc check-linearizability check-linearizability-stress

all: build

build:
	cargo build -p holo_store

build-release:
	cargo build -p holo_store --release

build-release-mimalloc:
	cargo build -p holo_store --release --features mimalloc

check-linearizability:
	./scripts/check_linearizability.sh

check-linearizability-stress:
	./scripts/check_linearizability_stress.sh
