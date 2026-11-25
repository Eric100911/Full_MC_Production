# Test procedures

## Offline validation

```bash
for script in processing/run_chain.sh tests/run_all_tests.sh tests/submit_tests.sh; do
  bash -n "$script"
done
python3 -m compileall -q src/full_mc
./tests/run_all_tests.sh
./tests/submit_tests.sh <pilot-spec.json> <new-run-dir>
```

The pilot command renders the root DAG. Inspect the SOURCE dependencies,
vanilla PLAN file transfer, single external processing DAG, shared submit and event log, and RECLAIM edge.
The static production tests supply SOURCE manifests, render the processing
DAG, and run `condor_submit_dag -no_submit -no_recurse` with
`CONDOR_CONFIG=/dev/null` for both levels. They exercise actual native LHE
splitting, tail allocation, whole-job retry, publication failures, and MiniAOD
handoff grouping using local files or mocked network responses.

## Authorized CERN pilot

Prepare a clean committed runtime. Keep the runtime, generated run and
Condor logs on submit-visible AFS. Before submission record the target event
count, counted source IDs and checksum, events and expected blocks per
source, merge target, and subprocess coverage. Use a positive
`planned_events` for a counted-input pilot.

Use `./mcprod run status --run <run-dir>` and
`python3 tools/mcprod_snapshot.py --run <run-dir>` with the schedd and root ID
recorded in `submission.json`. In a restricted Codex sandbox, use approved
unsandboxed execution on the first live Condor command. A failed network
query does not establish queue state. Offline file inspection and offline DAG
rendering can be performed without contacting the scheduler.

Verify that SOURCE logs show split counts and the tails from each source.
Compare PLAN's `proc/plan.json` to the processing DAG and reconcile skipped
blocks. In worker logs verify the requested CMSSW threads and streams, the
actual mixed and MiniAOD event counts, and success after a whole-job retry
where applicable. After all PROC jobs finish, verify that RECLAIM removed
the staged LHE blocks and manifests, and that IHEP archives and MiniAODs
remain readable. Confirm that `results/RECLAIM.json` has status `ok`.
Correlate it with successful root metrics for the recorded cluster.

Use `mcprod run resume` with the recorded failed run and its native DAGMan
rescue file. Resumed jobs run from their first worker step. Keep
`proc/processing.dag` and the Condor-generated rescue files in the run
directory.

## IHEP handoff and batch

Run `mcprod handoff export` on the completed CERN run. Check the ordered
MiniAOD references, merge groups, runtime release and core revision. Transfer
the handoff with the site configuration and prepare the IHEP batch from a
matching clean runtime. Verify the ntuple bundle and pin CMSSW15 jobs to the
AVX2-safe hosts in the HepJob reference. Verify the merged count with
`edmCopyPickMerge` output, then reconcile it with the ntuple count.

`mcprod batch submit` checks every distinct proxy source in the prepared worker
wrappers before submitting the first HepJob job. Each proxy certificate must
have more than 48 hours remaining. `batch resume` does not repeat this check.

For newly prepared batches, look under
`<batch-dir>/logs/<job-index>/attempt-<number>/` on shared IHEP storage. The
attempt number and HepJob ID are recorded together in `batch_manifest.json`.
`hepjob.err` reports the path to the per-execution diagnostics archive or
reports an archive failure. The worker wrapper saves the available stage
`commands.log` and native command stdout/stderr before node-local scratch is
released. It preserves the worker exit code. For an ntuple failure, inspect
`worker/ntuple/commands.log` and `worker/ntuple/command_logs/` inside the archive.

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
