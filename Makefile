.PHONY: all build build-release build-release-mimalloc check-linearizability check-linearizability-stress antithesis-build antithesis-package-build antithesis-up antithesis-smoke antithesis-full antithesis-suite antithesis-clean

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

antithesis-build:
	./scripts/antithesis_local_build.sh

antithesis-package-build:
	./scripts/antithesis_package_build.sh

antithesis-up:
	./scripts/antithesis_local_up.sh

antithesis-smoke:
	./scripts/antithesis_local_smoke.sh

antithesis-full:
	./scripts/antithesis_local_full_suite.sh

antithesis-suite: antithesis-full

antithesis-clean:
	./scripts/antithesis_local_clean.sh
