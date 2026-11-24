#!/bin/bash
# Offline tests for mcprod and the native worker adapters.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"
export PYTHONPATH="$repo/src${PYTHONPATH:+:$PYTHONPATH}"

python3 -m unittest discover -s tests -p 'test_*.py'
bash tests/test_cmssw_helpers.sh
bash tests/test_lhe_shuffle_split.sh --host
bash tests/test_octet_pdg_tool.sh
