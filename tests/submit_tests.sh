#!/bin/bash
# Prepare or submit an explicit campaign-shard pilot with mcprod.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ $# -lt 2 || $# -gt 3 || (${3:-} != "" && ${3:-} != "--submit") ]]; then
    echo "Usage: $0 <pilot-spec.json> <new-run-dir> [--submit]" >&2
    exit 2
fi

"$repo/mcprod" run plan --spec "$1" --output-dir "$2"
if [[ ${3:-} == "--submit" ]]; then
    exec "$repo/mcprod" run submit --run "$2"
fi
