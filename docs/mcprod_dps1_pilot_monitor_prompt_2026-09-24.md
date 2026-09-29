# DPS1 `mcprod` pilot monitoring prompt — 2026-09-24

Copy the prompt below to resume monitoring. The two CERN roots were submitted
to the same pinned schedd; all their scheduler and worker logs are under AFS.

| Pilot | Run directory | Schedd / root DAGMan cluster |
|---|---|---|
| CERN execution | `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924/runs/dps1_cern_full_20260924/cern` | `bigbird27.cern.ch` / `13486220` |
| CERN-to-IHEP cutover | `/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924/runs/dps1_cutover_20260924/cern` | `bigbird27.cern.ch` / `13486223` |

Both were active at the initial exact-root check. At 2026-09-24 09:02 CEST,
both roots terminated unsuccessfully in `WAVE_000001`; `RECLAIM_000001` raised
`TypeError: run_profile() got an unexpected keyword argument 'text'`. Their
`state.json` files remain `running`, `.complete` is absent, and each rescue
file is `run.dag.rescue001`. The PLAN, MIX, MINIAOD, and ARCHIVE result JSONs
are `ok`; only the RECLAIM result is `failed`. No handoff has been exported.
An exact-root status read at 09:14 CEST reconfirmed both as terminal and
unsuccessful (`query: history`, scheduler state `completed`). The JSON
snapshots are retained in the sibling `operator-logs/` directory as
`status-20260924T0714Z-dps1-cern-full.json` and
`status-20260924T0714Z-dps1-cutover.json`.

The narrow fix is in the current development worktree and passes the 39 focused
reclaim/contract tests and all 161 `test_core_*.py` tests; the 4 HELAC report
tests also pass. It is not in the immutable `e1fdf48` runtime. Do not handoff,
resume, or resubmit either pilot until the operator approves a clean pinned
runtime and a safe recovery/new-run strategy. After that decision, update this
prompt with any replacement run IDs, runtime release, and root IDs.

Runtime release is
`production-v3-dps1pilot`, pinned to clean core revision
`e1fdf487a5f376417268ee67c05f3cd8c27121ab`. The IHEP runtime is already staged
at `/scratchfs2/cms/wangchi/mcprod-runtimes/production-v3-dps1pilot/ihep`.
Each pilot uses one counted source file (`sps-jpsi-103`, 153,293 actual
events), capped at 20 planned events and split into four 5-event LHE blocks;
the JJP_DPS1 modes are `normal` and `phi_mpi_off`, with two processing nodes.
The merge target is 9 events to match the prior observed 5+4-event component
result. This is an existing-LHE pilot, not an LHE-generation run.
The submitted `plan` worker requests 1 CPU/6000 MB; the 1 CPU/3072 MB change
in the current development worktree applies to stage `lhe` and is not in the
clean `e1fdf48` pilot runtime. No LHE-generation task is scheduled here.

## Prompt

```text
Reconcile the two DPS1 mcprod pilots first. Use the public entrypoint ./mcprod
from /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core.
Do not query command help. Monitor each run by its exact recorded schedd/root
pair and take an exact status snapshot immediately. As of 2026-09-24 09:02 CEST,
both roots are terminal but unsuccessful because the pinned worker runtime
failed in RECLAIM_000001 with
`TypeError: run_profile() got an unexpected keyword argument 'text'`. Both
state.json files remain `running`; neither run has .complete. If that is still
the state, stop after recording the evidence: do not handoff, resume, remove,
or resubmit. The fix currently passes focused tests but is not in the pinned
runtime. Ask the operator whether to create a clean pinned release and choose
a recovery/new-run strategy. Only after that approval and a new root submission
should you resume 15-minute snapshots. Use the product's wait mechanism for
900 seconds between snapshots; do not busy-poll.

Run the one-shot status command for both exact AFS run directories:

  ./mcprod run status --run /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924/runs/dps1_cern_full_20260924/cern
  ./mcprod run status --run /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/.worktrees/production-core/generated/mcprod-pilots/dps1_20260924/runs/dps1_cutover_20260924/cern

The exact roots are bigbird27.cern.ch/13486220 and
bigbird27.cern.ch/13486223. Verify that each status response still names its
recorded pair. Keep each status response in
generated/mcprod-pilots/dps1_20260924/operator-logs/ on CERN AFS. For
diagnosis, inspect only the corresponding run's submission.json, state.json,
run.dag.dagman.out, run.dag.dagman.log, logs/advance_*.log,
logs/events.log, task .out/.err files, and committed ledger records. Do not use
a recent-time-window query, submit a duplicate, remove a DAG, or resume an
ambiguous attempt. If a run is held, failed, or unknown, stop and report the
exact evidence before any recovery action.

The CERN-execution pilot ends only when mcprod reports complete and its AFS
run has .complete plus committed archive receipts. Report its final state,
event accounting, logs, and exact root ID.

For the cutover pilot, continue the following handoff once its CERN run is
complete. First verify .complete and the committed records. Then run:

  ./mcprod handoff export --run <cutover-run-dir>

Capture the JSON summary in the AFS operator-logs directory and inspect
<cutover-run-dir>/handoff.json. Require schema full-mc-handoff-v1, the pinned
runtime_release/core_revision above, exactly one expected DPS1 task group, and
an empty rejected array. Confirm its component MiniAOD sidecars are complete
and their event counts match the frozen records. If any group is rejected,
missing, or unexpected, stop; do not transfer or submit it.

Transfer only the verified handoff:

  ./mcprod handoff transfer --run <cutover-run-dir> \
    --site-config /afs/cern.ch/user/c/chiw/condor/mcprod-pilot/sites/ihep.json

Persist the transfer JSON in the AFS operator-logs directory. On IHEP, use
the returned `.handoff` URL and these persistent paths:

  export IHEP_WORK_ROOT=/scratchfs2/cms/$USER/hepjobs/Full_MC_Production
  export IHEP_HANDOFF=$IHEP_WORK_ROOT/runs/dps1_cutover_20260924/handoff.json
  export IHEP_BATCH_DIR=$IHEP_WORK_ROOT/runs/dps1_cutover_20260924/ihep
  export IHEP_RUNTIME_DIR=/scratchfs2/cms/wangchi/mcprod-runtimes/production-v3-dps1pilot/ihep
  export X509_USER_PROXY=/scratchfs2/cms/wangchi/mcprod-pilot/proxy/x509up

Before writing, confirm the unique handoff and batch paths do not already
exist. Verify the IHEP proxy is readable and has more than 10 minutes left,
and validate runtime.json: site ihep, release production-v3-dps1pilot, core
revision e1fdf487a5f376417268ee67c05f3cd8c27121ab, source_dirty false. Fetch
the returned triple-slash URL with xrdcp into the persistent IHEP run
directory without overwriting an existing file. Set
`export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH` on IHEP.
Validate the release bundles with `validate_runtime_release` from the runtime
controller's `src` tree. Use python3.11 and the pinned runtime controller to
prepare one batch, saving JSON output into persistent IHEP logs:

  test ! -e "$IHEP_HANDOFF" && test ! -e "$IHEP_BATCH_DIR"
  mkdir -p "$IHEP_BATCH_DIR/logs"
  xrdcp "$HANDOFF_URL" "$IHEP_HANDOFF"
  python3.11 "$IHEP_RUNTIME_DIR/controller/core/mcprod" batch prepare \
    --handoff "$IHEP_HANDOFF" \
    --output-dir "$IHEP_BATCH_DIR" \
    --proxy "$X509_USER_PROXY" \
    --runtime-dir "$IHEP_RUNTIME_DIR" \
    --log-level info | tee "$IHEP_BATCH_DIR/logs/batch_prepare.json"

Inspect batch_manifest.json before submitting. It should contain one combined
JJP group with merge, ntuple, selection, and efficiency subtasks; the ntuple
must remain pinned to an AVX2-safe node. Submit it once and save the submit
response and HepJob ID in persistent IHEP logs:

  python3.11 "$IHEP_RUNTIME_DIR/controller/core/mcprod" batch submit \
    --manifest "$IHEP_BATCH_DIR/batch_manifest.json" \
    | tee "$IHEP_BATCH_DIR/logs/batch_submit.json"

Never submit an already-submitted/ambiguous job again. For each 15-minute
IHEP snapshot, query only the recorded ID with `hep_q -i <job-id>` and retain
its output in the persistent batch log directory. Retry an empty/error query
once; if it remains empty, stop as unknown rather than assuming completion.

Important status safety note: the deployed worker runtime is pinned to clean
commit e1fdf48, while the hep_q -i fix exists only in the current development
worktree and is not part of that release. Do not call batch status or batch
resume from the pinned runtime: its older status helper can mistake hep_q
usage text for a failed live job. Instead, query only the recorded ID on IHEP
with `hep_q -i <job-id>`. Retry an empty/error result once; if it remains
empty, classify it as unknown, not complete or failed. Inspect persistent
IHEP logs/0000.out and 0000.err, batch_manifest.json, and the exact
checkpoints/0000/{merge,ntuple,selection,efficiency}.json receipts. Do not
resume or resubmit until the exact attempt and outputs have been reconciled.
Treat the batch as complete only when all four receipts have status `ok`, their
task identities match the frozen handoff, and every declared output sidecar
and ROOT product passes the read-only readiness/stat checks.

For each snapshot, report both CERN root states and, after cutover submission,
the exact HepJob ID/state plus the persistent outputs/checkpoints inspected.
Continue at 15-minute intervals while active. Stop polling only when both
pilots are terminal and the cutover handoff/products have been verified, or
when an explicit blocker needs operator direction.
```
