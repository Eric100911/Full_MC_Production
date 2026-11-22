#!/bin/bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 CMSSW_BASE COMMAND [ARG ...]" >&2
    exit 2
fi

cmssw_base="$1"
shift
cmsset_default="${CMSSET_DEFAULT_SH:-/cvmfs/cms.cern.ch/cmsset_default.sh}"
caller_dir="${PWD}"

if [[ ! -d "${cmssw_base}/src" ]]; then
    echo "CMSSW_12 project src directory not found: ${cmssw_base}/src" >&2
    exit 2
fi
if [[ ! -r "${cmsset_default}" ]]; then
    echo "CMS environment setup script not readable: ${cmsset_default}" >&2
    exit 2
fi

source "${cmsset_default}"
export SCRAM_ARCH=el8_amd64_gcc10
cd "${cmssw_base}/src"
eval "$(scramv1 runtime -sh)"
cd "${caller_dir}"

exec "$@"
