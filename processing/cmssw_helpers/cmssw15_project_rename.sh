#!/bin/bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 CMSSW_BASE" >&2
    exit 2
fi

cmssw_base="$1"
cmsset_default="${CMSSET_DEFAULT_SH:-/cvmfs/cms.cern.ch/cmsset_default.sh}"

if [[ ! -d "${cmssw_base}/src" ]]; then
    echo "CMSSW_15 project src directory not found: ${cmssw_base}/src" >&2
    exit 2
fi
if [[ ! -r "${cmsset_default}" ]]; then
    echo "CMS environment setup script not readable: ${cmsset_default}" >&2
    exit 2
fi

source "${cmsset_default}"
export SCRAM_ARCH=el9_amd64_gcc12
cd "${cmssw_base}/src"

# A relocated project must be renamed before its runtime environment is trusted.
exec scram build ProjectRename
