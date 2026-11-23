# Test Procedures

## Offline checks

Run `./tests/run_all_tests.sh` for the Python regressions and the native
CMSSW-helper, LHE splitting, and octet-ID checks. The fused production tests
inspect the generated SOURCE/SHARD DAGs and verify checkpoint recovery,
terminal cleanup, and the IHEP handoff using local fixtures.

An explicit pilot spec can be rendered with
`./tests/submit_tests.sh <pilot-spec.json> <new-run-dir>`. The script submits
only with `--submit`. Use the recorded schedd and root cluster to monitor it.

## New production and cutover pilots with `mcprod`

Prepare the site-specific runtime from a clean committed core revision and
store it, the run directory, and operator logs on CERN AFS. A submitted mcprod
run records its attempts and current root DAGMan identity in `submission.json`;
use `./mcprod run status --run <run-dir>` for that recorded attempt and
`tools/dag_progress.py` for nested SOURCE and SHARD progress.
`python3 tools/mcprod_snapshot.py --run <run-dir>` gives a read-only snapshot
of the recorded v3 attempt.

Live Condor reads need network access. In a restricted Codex sandbox, request
approved unsandboxed execution *before the first attempt* at `myschedd show`,
`condor_q`, `condor_history`, `mcprod_snapshot.py`, `dag_snapshot.py`,
`dag_progress.py`, or `./mcprod run status`. These commands can load the CERN
Condor configuration and contact a schedd even when they do not change jobs;
the sandbox may fail during DNS/configuration lookup (for example, through
`myschedd.sh`). An error or empty result from that environment does not prove
the queue is empty. If approved network execution is unavailable, inspect
local DAGMan logs and rescue files but report live queue status as unknown.
Offline DAG generation, local log inspection, and tests using an isolated
`CONDOR_CONFIG=/dev/null` do not need this escalation. A submission or rescue
still requires separate operator authorization; network access alone is not
authorization to change the queue.

`mcprod run status` can confirm success after the exact root leaves both queue
and bounded history when `.complete`, controller state, and root DAGMan metrics
agree on completion and cluster ID. The snapshot reader also recognizes the
`Workflow batch-id` marker in newer DAGMan logs. `dag_progress.py` labels a
SOURCE skip and DAGMan terminal events from the available logs.

`mcprod run submit` checks the proxy pinned in the generated worker submit files
before reserving namespace ownership or submitting the root DAG. The proxy certificate must
have more than 48 hours remaining according to `voms-proxy-info --timeleft`.
The PRE scripts and `run resume` do not repeat this check.

The generated Condor event log, DAGMan logs, PRE-script logs, worker
stdout/stderr, and diagnostic archives are all placed below the AFS run
directory.

LHE worker requests are 1 CPU and 3072 MB RAM. Keep explicit `scratch_bytes`
ceilings and their derived disk requests intact. A campaign using an existing
counted LHE has a `plan` task but no LHE-generation worker, so the LHE resource
setting is not exercised by that pilot.

For an existing-LHE DPS1 pilot, preserve the inventory checksum and source ID,
use a positive finite `planned_events`, and state the expected source files,
block size/count, subprocess modes, processing-node count, and merge target
before submission. A ready IHEP cutover requires all of the following:

1. `./mcprod run status --run <run-dir>` reports the CERN run complete and
   the AFS run contains `.complete`, successful SOURCE and PROCESS receipts,
   and the accepted terminal reclaim receipts. The IHEP mixed HepMC stays in
   the run's archive.
2. `mcprod handoff export` reports no rejected groups; inspect its explicit
   `tasks`, `runtime_release`, and `core_revision` before transfer.
3. `mcprod handoff transfer` uses the explicit IHEP site configuration.
4. The IHEP receiving runtime is clean and matches both the handoff release and
   core revision. Stage the handoff JSON into an IHEP-visible persistent
   workspace before preparing its batch.
5. Inspect `batch_manifest.json` before the one-time submit. Keep batch logs and
   checkpoints on persistent IHEP storage, install the proxy into the
   job-private `credentials/x509_user_proxy`, and pin CMSSW15 ntuple tasks to
   the AVX2-safe hosts listed in the HepJob reference.

`mcprod batch submit` checks every distinct proxy source in the prepared worker
wrappers before submitting the first HepJob job. Each proxy certificate must
have more than 48 hours remaining. `batch resume` does not repeat this check.

Monitor exact recorded root/job IDs. An empty queue result or an aggregate
`0 completed`/`0 removed` counter is not a job outcome; retry an empty query
once, then reconcile against persistent logs, task results, checkpoints, and
EOS outputs. `batch status` reads only a row matching the recorded HepJob ID;
an absent or ambiguous row may remain `unknown` in the scheduler field. That
field alone does not keep a task open: when every task has matching
four-phase checkpoint identities with `status=ok`, and every declared output
has a valid sidecar and expected size, with applicable count and provenance
fields matching, mark the batch complete and stop querying. Do not repeat
scheduler checks or resubmit solely because the scheduler field remains
`unknown`. Never remove or resume a DAG/batch merely because it looks idle.
For a completed small DPS1 cutover example, see
`docs/mcprod_ihep_cutover_pilot_lessons_2026-09-29.md`. The 2026-09-24 DPS1
monitor prompts are historical and their IDs must not be reused.

### Retry and recovery for v3 campaign shards

The `PROCESS_*` stderr, diagnostic archive, and result JSON identify failed
detector variants. A retry loads the frozen manifest, checks the IHEP HepMC
payload and sidecar, and runs only missing variants. A payload uploaded before a
worker or controller crash can be reconciled through its recorded checksum.
An incomplete POSC transfer can be retried. A committed payload or sidecar
with conflicting provenance, count, size, or checksum stops the task. Preserve
its exact URLs and evidence. An operator can restore the verified pair before
resuming, or authorize a new run when the original cannot be restored. Never
overwrite a conflicting checkpoint during a retry.

If publication completed and POST failed, use the existing
`mcprod recovery prepare-rescue` procedure for the recorded failed root. The
preview reads the durable records. Applying recovery can commit a missing final
sidecar from its verified candidate and accept an exact reconstructed result.
If a variant is absent, the worker retry performs that variant. A terminal
reclaim failure leaves the run incomplete. Its frozen block list can be
replayed, and a missing exact payload is recorded as `already-missing`.

For a separately authorized pilot with one or two processing jobs:

1. Confirm the frozen source count, event counts, block size and count, merge
   target, subprocess coverage, and site validation record.
2. Verify the native profiles, two-thread simulation, IHEP POSC publication
   and checksum queries, and checkpoint creation before MiniAOD.
3. Exercise a controlled retry after HepMC publication. Confirm that completed
   variants are reused. Record peak scratch, memory, and walltime.
4. Verify that terminal cleanup removes the allocated and unused CERN LHE
   blocks after all consumers finish. Confirm the IHEP HepMC still validates.
5. Export the handoff and verify the ordered MiniAOD inputs with the existing
   IHEP `edmCopyPickMerge` and ntuple stages.

### Fused CERN-to-IHEP pilot (2026-11-21)

Run `fused_cern_ihep_20261121T131217Z` validated the fused workflow at
`9a8fec3bf6f6bb0b5c9a80574d7348acf79500e4` with release
`fused_cern_ihep_20261121T131217Z-9a8fec3`. CERN DAGMan `13889279` on
`bigbird18.cern.ch` exited successfully. IHEP HepJob `84070069` ran on
`ams017.ihep.ac.cn` and committed all four stage checkpoints.

- The JJP_DPS1 pilot selected 25 events from counted source `sps-jpsi-103`
  (seed 103, 153,293 events). The inventory SHA-256 was
  `9c393a706aa3ad4368a3ca3c10a822cdd65f7c4daee86950df3e4e9044369e68`.
  PLAN produced five blocks of five events. Each of two fused jobs used distinct
  `normal` and `phi_mpi_off` blocks and ran one `run3_2022_legacy_premix_v1`
  variant. The fifth block was unused.
- SHARD submitted from accepted PLAN blocks while SOURCE was still active.
  SOURCE reclaimed the staged CERN original after archive verification.
  MIX committed five-event HepMC files directly to IHEP before MiniAOD.
  Live detector logs showed two threads and two streams.
- Controlled eviction of `13889285.0` after HepMC publication recovered the
  checkpoint and ran the missing MiniAOD without rerunning MIX. A separate
  replay of the completed task for `13889284.0` reused both outputs without
  running either native stage.
- Terminal cleanup removed all five blocks after SOURCE and SHARD completed.
  Both IHEP HepMC checkpoints still validated after cleanup. The run wrote
  `.complete` and exported one handoff group with zero rejections.
- `edmCopyPickMerge` opened blocks `000000` and `000001` in that order.
  The merged MiniAOD contained ten events, and NTUPLE wrote ten entries.
  JJP selection selected zero entries. The efficiency shard contained both
  expected Parquet inputs for aggregation. All four output payloads matched
  their verified sizes and Adler-32 checksums.

The IHEP source fallback rejected the pinned TPS-Onia2MuMu `test/crabData`
gitlink. Runtime preparation succeeded with the prebuilt CMSSW15 bundle.
The AFS pilot bundle retains the scheduler records, receipts, worker logs,
checkpoint checksums, and resource samples under
`generated/fused_cern_ihep_20261121T131217Z/` in the pilot worktree.

## Historical procedures

See [Historical production workflows](legacy-production.md).
