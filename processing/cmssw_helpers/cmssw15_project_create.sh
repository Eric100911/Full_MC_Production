#!/bin/bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 WORKDIR CMSSW_VERSION" >&2
    exit 2
fi

workdir="$1"
cmssw_version="$2"
cmsset_default="${CMSSET_DEFAULT_SH:-/cvmfs/cms.cern.ch/cmsset_default.sh}"

if [[ ! -d "${workdir}" ]]; then
    echo "CMSSW project workdir not found: ${workdir}" >&2
    exit 2
fi
if [[ ! -r "${cmsset_default}" ]]; then
    echo "CMS environment setup script not readable: ${cmsset_default}" >&2
    exit 2
fi

source "${cmsset_default}"
export SCRAM_ARCH=el9_amd64_gcc12
cd "${workdir}"
exec scramv1 project CMSSW "${cmssw_version}"
