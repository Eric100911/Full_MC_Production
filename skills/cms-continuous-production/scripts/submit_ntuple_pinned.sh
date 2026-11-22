#!/bin/bash
# Submit all ntuple HepJobs for a merged campaign workspace, pinned round-robin to
# the 11 AVX2-safe nodes (ams017/018/019/020 + cws047/048/049/051/052/053/054).
# Unpinned submission lands ~50% on cws050/cws055 (Ivy Bridge, no AVX2) and dies
# rc=132 in the cmssw-el9 container. Merge is unaffected (cmssw-el8).
#
# Usage (on IHEP): submit_ntuple_pinned.sh <campaign> <workspace> <proxy>
#   campaign : JJP_SPS_CS | JJP_DPS2_CS
#   workspace: /scratchfs2/.../runs/<campaign_lower>_full
set -euo pipefail

CAMPAIGN="${1:?usage: submit_ntuple_pinned.sh <campaign> <workspace> <proxy>}"
WS="${2:?usage: submit_ntuple_pinned.sh <campaign> <workspace> <proxy>}"
PROXY_SOURCE="${3:?usage: submit_ntuple_pinned.sh <campaign> <workspace> <proxy>}"
SAFE_NODES=(ams017 ams018 ams019 ams020 cws047 cws048 cws049 cws051 cws052 cws053 cws054)
export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH

[[ -s "$PROXY_SOURCE" ]] || { echo "ERROR: missing proxy $PROXY_SOURCE" >&2; exit 2; }

# 1. merge gate must exist (ntuple depends on merge)
[[ -s "$WS/gates/merge_${CAMPAIGN}.json" ]] || { echo "ERROR: missing gate $WS/gates/merge_${CAMPAIGN}.json" >&2; exit 3; }

# 2. rebuild the proxy bundle (fresh, in-workspace, non-/tmp path)
if command -v voms-proxy-info >/dev/null 2>&1; then
  LEFT=$(voms-proxy-info -file "$PROXY_SOURCE" --timeleft 2>/dev/null || echo 0)
  (( LEFT >= 600 )) || { echo "ERROR: proxy expires in under 10 minutes" >&2; exit 2; }
fi
TMP=$(mktemp -d "$WS/.proxy.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/credentials"
install -m 600 "$PROXY_SOURCE" "$TMP/credentials/x509_user_proxy"
tar -czf "$WS/bundles/proxy_bundle.tar.gz.tmp" -C "$TMP" credentials
chmod 600 "$WS/bundles/proxy_bundle.tar.gz.tmp"
mv -f "$WS/bundles/proxy_bundle.tar.gz.tmp" "$WS/bundles/proxy_bundle.tar.gz"

# 3. count tasks from the workspace
N=$(python3 -c "import json,sys; print(len(json.load(open('$WS/tasks/${CAMPAIGN}/ntuple_tasks.json'))))")
echo "[submit_ntuple_pinned] $CAMPAIGN: $N ntuples, pinned round-robin over ${#SAFE_NODES[@]} safe nodes"

mkdir -p "$WS/logs/ntuple/${CAMPAIGN}"
RUN_SCRIPT="$WS/scripts/run_ntuple_${CAMPAIGN}.sh"

ok=0; fail=0
for idx in $(seq 0 $((N-1))); do
  node="${SAFE_NODES[$((idx % ${#SAFE_NODES[@]}))]}.ihep.ac.cn"
  if hep_sub "$RUN_SCRIPT" -g cms -gwn CMS -wt mid \
    -wn "$node" -argu "$idx" -n 1 \
    -o "$WS/logs/ntuple/${CAMPAIGN}/ntuple_${idx}.out" \
    -e "$WS/logs/ntuple/${CAMPAIGN}/ntuple_${idx}.err" >/dev/null 2>&1; then
    ok=$((ok+1))
  else
    fail=$((fail+1)); echo "FAIL idx=$idx -> $node"
  fi
done
echo "[submit_ntuple_pinned] done: ok=$ok fail=$fail total=$((ok+fail))"
