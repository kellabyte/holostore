#!/usr/bin/env bash
set -euo pipefail

# Start the local Antithesis Compose stack, ensuring bind-mounted state lives
# under the repo's .tmp directory.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/antithesis_common.sh"

prepare_antithesis_tmp
compose_in_antithesis up "$@"

