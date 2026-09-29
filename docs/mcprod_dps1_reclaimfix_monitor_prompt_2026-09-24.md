# DPS1 replacement-pilot monitoring prompt — 2026-09-24

The first submitted pair remains preserved as a failed attempt: roots
`13486220` and `13486223` on `bigbird27.cern.ch` both failed in reclaim because
the pinned `e1fdf48` runtime passed an unsupported `text` argument to
`run_profile`. The fix is committed as `1449aa9`; a new clean CERN/IHEP release
was prepared from that commit. The intervening `r1` plans were never submitted:
the local Condor configuration had no `SCHEDD_NAME`, and no submission record
or root ID was created for them.

The active replacement runs were submitted to `bigbird26.cern.ch` and were both
active at 2026-09-24 10:06 CEST. Their exact identities and AFS paths are:

| Pilot | Run directory | Schedd / root DAGMan cluster |
|---|---|---|
| CERN execution | `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924_reclaimfix/runs/dps1_cern_full_20260924_r2/cern` | `bigbird26.cern.ch` / `12740955` |
| CERN-to-IHEP cutover | `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924_reclaimfix/runs/dps1_cutover_20260924_r2/cern` | `bigbird26.cern.ch` / `12740959` |

The submission acknowledgements, plan records, inventory checksum, source stat,
proxy check, runtime records, and status snapshots are retained in
`/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924_reclaimfix/operator-logs/`.
The latest exact-root snapshots are
`status-20260924T0806Z-dps1-cern-full.json` and
`status-20260924T0806Z-dps1-cutover.json`; both report `active`, with the root
DAGMan proc running. The clean committed checkout passed all 148
`test_core_*.py` tests and the production-style worker mock (20 checks, 0
failures).

Both pilots use one counted source (`sps-jpsi-103`, 153,293 actual events),
20 planned events, 5 events per LHE block (four expected blocks), two
processing nodes, a 9-event merge target, and the `normal` plus
`phi_mpi_off` JJP_DPS1 subprocesses. The specs explicitly set the `lhe` stage
to 1 CPU and 3072 MB RAM. This is an existing-LHE pilot, so there must be no
LHE-generation worker; the initial PLAN task requests 1 CPU / 6000 MB and is a
different stage.

Runtime release: `production-1449aa9-dps1-reclaimfix`, pinned to clean core
revision `1449aa9f3c6550c31e9acd3b3bb2e4f0d0ba8e4c`. The CERN release and logs
are on AFS. The validated IHEP release is at
`/scratchfs2/cms/wangchi/mcprod-runtimes/production-1449aa9-dps1-reclaimfix/ihep`.
Its descriptor records `site: ihep`, `source_dirty: false`, and the pinned
`NtupleAnalyzer` revision `89a37823c89ea451b8affb8a8b9df5ae0cae5f46`.

## Prompt

```text
Monitor the two active DPS1 replacement pilots with ./mcprod, using the exact
AFS run directories, schedd names, and root IDs in this file. Start with one
exact status snapshot for each run. The last verified state (2026-09-24 10:06
CEST) was active/running on bigbird26.cern.ch roots 12740955 and 12740959.
Do not query CLI --help; use docs/README.md and docs/testing.md as the command
reference, and update them if a verified interface is missing or stale.

Keep every status response in the AFS operator-logs directory with a UTC
timestamp in its filename. Query only the recorded schedd/root pair; do not
use recent-time-window searches. After each pair of snapshots, wait 900 seconds
using the product's wait mechanism before polling again. Do not busy-poll.

Use the public command from the clean AFS controller checkout:
/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/mcprod-1449aa9-runtime-controller

  ./mcprod run status --run <exact-AFS-run-directory>

The CERN execution pilot is complete only when mcprod reports successful
completion, the run contains .complete, and committed archive receipts and
declared MiniAOD products/sidecars reconcile with the frozen event accounting.
Report the final exact root, expected/observed counts, outputs, and AFS logs.

The cutover pilot must pass the same CERN completion checks before any handoff.
Then export exactly once:

  ./mcprod handoff export --run <cutover-AFS-run-directory>

Save the JSON response in operator-logs and inspect handoff.json. Require the
expected full-mc-handoff-v1 schema, runtime_release
production-1449aa9-dps1-reclaimfix, core_revision
1449aa9f3c6550c31e9acd3b3bb2e4f0d0ba8e4c, one expected JJP_DPS1 group, and an
empty rejected list. Reconcile every component task and MiniAOD sidecar with
the committed receipts. If any group is rejected, missing, duplicate, or
unexpected, stop without transferring.

Transfer only the verified handoff:

  ./mcprod handoff transfer --run <cutover-AFS-run-directory> \
    --site-config /afs/cern.ch/user/c/chiw/condor/mcprod-pilot/sites/ihep.json

Keep its JSON response on AFS. On IHEP, use the returned handoff URL and
persistent, unique paths:

  export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
  export IHEP_WORK_ROOT=/scratchfs2/cms/$USER/hepjobs/Full_MC_Production
  export IHEP_RUN=$IHEP_WORK_ROOT/runs/dps1_cutover_20260924_r2
  export IHEP_HANDOFF=$IHEP_RUN/handoff.json
  export IHEP_BATCH_DIR=$IHEP_RUN/ihep
  export IHEP_RUNTIME_DIR=/scratchfs2/cms/wangchi/mcprod-runtimes/production-1449aa9-dps1-reclaimfix/ihep
  export X509_USER_PROXY=/scratchfs2/cms/wangchi/mcprod-pilot/proxy/x509up

Before writing, verify the unique handoff/batch paths do not exist, the proxy
has more than 10 minutes remaining, and runtime.json matches the release, IHEP
site, clean source, and core revision above. Fetch the returned handoff URL
without overwriting an existing file. Use python3.11 and the pinned runtime
controller to prepare one batch, saving output under $IHEP_RUN/logs:

  test ! -e "$IHEP_HANDOFF" && test ! -e "$IHEP_BATCH_DIR"
  mkdir -p "$IHEP_RUN/logs"
  xrdcp "$HANDOFF_URL" "$IHEP_HANDOFF"
  python3.11 "$IHEP_RUNTIME_DIR/controller/core/mcprod" batch prepare \
    --handoff "$IHEP_HANDOFF" --output-dir "$IHEP_BATCH_DIR" \
    --proxy "$X509_USER_PROXY" --runtime-dir "$IHEP_RUNTIME_DIR" \
    --log-level info | tee "$IHEP_RUN/logs/batch_prepare.json"

Inspect batch_manifest.json before its one-time submit. Confirm task identities
match the verified handoff, outputs are exact, and CMSSW15 ntuple placement is
pinned to the documented AVX2-safe hosts. Save the submit response and exact
HepJob ID under $IHEP_RUN/logs. Never resubmit an ambiguous attempt.

This pinned runtime does not contain the newer hep_q status-parser fix from
the dirty development worktree. Do not use `mcprod batch status` or `batch
resume` from it. For each 15-minute IHEP snapshot, query only the recorded job
with `hep_q -i <job-id>` and retain the response under $IHEP_RUN/logs. Retry an
empty/error query once; if still empty, classify it as unknown and stop. Inspect
0000.out/.err, batch_manifest.json, and the exact merge/ntuple/selection/
efficiency checkpoints. Never resume or resubmit until the attempt and outputs
are reconciled. Accept batch completion only when all expected receipts are
`ok`, task identities match the handoff, and every declared sidecar and ROOT
product passes read-only readiness checks.

At each 15-minute checkpoint, report both CERN roots and, after cutover submit,
the exact HepJob ID/state and inspected persistent outputs/checkpoints. If a
CERN root is held, failed, terminal-unsuccessful, or unknown, stop and report
the precise evidence before any recovery action. Do not remove or resume either
DAG merely because it appears idle. Stop monitoring only when the CERN pilot
and the full CERN-to-IHEP cutover are verified complete, or when operator input
is required.
```
