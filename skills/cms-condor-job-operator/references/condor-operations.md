# Condor operations reference

## Schedd selection

For a new submission:

```bash
myschedd bump
myschedd show
condor_submit_dag <dag>
```

Capture the schedd and printed DAGMan cluster ID together. `myschedd bump`
selects a schedd for subsequent commands; it does not move an already submitted
DAG.

For an existing DAG, specify the recorded schedd:

```bash
condor_q -name <schedd> <dag-cluster> -nobatch
condor_q -name <schedd> -dag <dag-cluster> -nobatch
```

## DAG progress

For a one-shot status summary in `Full_MC_Production`, prefer:

```bash
python3 skills/cms-condor-job-operator/scripts/dag_snapshot.py \
  <dag-cluster> --schedd <schedd>
```

This command is read-only and does not poll. Use the full dashboard for
diagnosis or when the user requests topology:

```bash
./tools/dag_progress.py <dag-cluster> \
  --schedd <schedd> \
  --structure=collapsed
```

Use `--structure=full --details` for a small DAG. Use the collapsed view for a
large production. Combine it with:

```bash
tail -n 100 <dag>.dagman.out
tail -n 100 <dag>.nodes.log
```

Read nested `blocks_processing.dag.dagman.out` files when the top DAG only
shows a submitted SubDAG.

## Useful ClassAds

Inspect a live job:

```bash
condor_q -name <schedd> <cluster>.<proc> -long
condor_q -name <schedd> <cluster>.<proc> \
  -af ClusterId ProcId JobStatus HoldReason HoldReasonCode \
      HoldReasonSubCode LastHoldReason NumJobStarts NumShadowStarts \
      RemoteHost RequestCpus RequestMemory MemoryUsage \
      RemoteWallClockTime CommittedTime
```

Inspect a completed job:

```bash
condor_history -name <schedd> <cluster>.<proc> -long
```

Important `JobStatus` values are:

```text
1 idle
2 running
3 removed
4 completed
5 held
6 transferring output
7 suspended
```

## Idle triage

Check in this order:

1. Confirm whether the proc is genuinely idle or is an unready DAG node.
2. Inspect `NumJobStarts`, `LastRemoteHost`, and eviction or termination
   events. A running job returning to idle commonly indicates eviction.
3. Run `condor_q -better-analyze` for matching constraints.
4. Compare requested CPU, memory, disk, OS, site, and universe requirements.
5. Check DAGMan queue controls independently:
   `DAGMAN_MAX_JOBS_SUBMITTED`, `DAGMAN_MAX_JOBS_IDLE`,
   `DAGMAN_MAX_SUBMITS_PER_INTERVAL`, and `DAGMAN_SUBMIT_DELAY`.
6. Check schedd health only after job- and DAG-level limits are excluded.

Do not switch schedds merely because a child job is temporarily idle.

## Held-job triage

Record the exact hold reason, code, and subcode before changing anything.
Inspect the worker stderr and user log around the hold timestamp. Common groups
include:

- executable or input sandbox failure;
- output-transfer failure;
- proxy or authentication expiry;
- worker resource excess;
- application nonzero exit interpreted by policy;
- DAGMan or scheduler-universe submission failure.

Release only after the cause is understood and the existing sandbox can
successfully retry. Otherwise fix the inputs and use rescue/recovery or a new
submission.

## Slow-job triage

Compare wall time with CPU time and a completed peer of the same node type.

- High CPU/wall ratio suggests computation.
- Low CPU/wall ratio with remote `root://` inputs suggests XRootD I/O or wait.
- Stable memory below request does not justify increasing memory.
- Sequential open/close timestamps reveal per-file remote-read latency.

Treat disconnect warnings as evidence only when they align with a failure or a
stall. Some XRootD disconnect messages occur during otherwise successful file
transitions.

## Transfer audit

Inspect the submit description and history for:

```text
should_transfer_files
when_to_transfer_output
transfer_input_files
transfer_output_files
transfer_output_remaps
output
error
log
```

Remember that Condor-managed stdout/stderr are not automatically equivalent to
files created in worker scratch. A POST script may run in a different working
directory after the worker sandbox has already been cleaned.

Use persisted DAGMan node logs and `condor_history` when the live proc has left
the queue. Report missing logs independently of physics-output success.

## Removal and recovery

Before removal:

```bash
condor_q -name <schedd> -dag <dag-cluster> -nobatch
find <dag-dir> -maxdepth 2 \
  \( -name '*.rescue*' -o -name '*.dagman.out' -o -name '*.nodes.log' \) \
  -print
```

Prefer a rescue/recovery submission when completed nodes and deterministic
outputs can be reused. When explicit removal is required, target the resolved
clusters on the original schedd and verify they disappear:

```bash
condor_rm -name <schedd> <exact-cluster-ids>
condor_q -name <schedd> <exact-cluster-ids> -nobatch
```
