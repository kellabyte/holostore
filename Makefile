.PHONY: all build build-release check-linearizability

all: build

build:
	cargo build -p holo_store

build-release:
	cargo build -p holo_store --release --features mimalloc

check-linearizability:
	./scripts/check_linearizability.sh
