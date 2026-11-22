#!/bin/bash
# Run the SPS_CS / DPS2_CS merge audit gate on IHEP (workspace paths are IHEP-absolute).
# The audit reads each merge manifest via xrdcp, so it must run when the shared IHEP
# login node is NOT fork-exhausted (avoid false "failed" counts).
#
# Usage: audit_cs_merge_gate.sh <campaign: sps_cs|dps2_cs>
set -euo pipefail

CAMPAIGN="${1:?usage: audit_cs_merge_gate.sh <sps_cs|dps2_cs>}"
IH_PFX=/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production
REPO="$IH_PFX/repo"
PROXY="$IH_PFX/credentials/20260811T141811Z/credentials/x509_user_proxy"
WS="$IH_PFX/runs/${CAMPAIGN}_full"
export X509_USER_PROXY="$PROXY"

echo "[audit] $(date -u +%H:%M:%S) starting merge gate audit for $CAMPAIGN (load check first)"
LOAD=$(ssh -o ConnectTimeout=15 -o ControlMaster=no ihep "cut -d' ' -f1 /proc/loadavg" 2>/dev/null | tr -d ' ')
echo "[audit] IHEP login loadavg = ${LOAD:-unknown}"
if [ -n "$LOAD" ] && awk "BEGIN{exit !(\$1 > 30)}" <<<"$LOAD"; then
  echo "[audit] load ${LOAD} > 30, aborting (would false-fail under fork pressure); retry later"
  exit 2
fi

ssh -o ConnectTimeout=15 -o ControlMaster=no ihep "cd $REPO && export X509_USER_PROXY=$PROXY && python3 dag_generator.py audit-split --stage merge --workspace $WS"
echo "[audit] gate result above; gate file: $WS/gates/merge_JJP_$(echo "$CAMPAIGN" | tr a-z A-Z).json"
