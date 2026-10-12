---
name: cms-condor-job-operator
description: Inspect, triage, audit, and safely operate HTCondor and DAGMan jobs on CERN submit hosts. Use when a user asks about idle, held, evicted, retried, removed, shrinking, stalled, or slow jobs; wants DAG progress by DAGMan cluster ID; needs to select a schedd with myschedd bump; or requests submission, recovery, rescue, resubmission, or removal. Apply this general workflow to pilots and productions, including nested SubDAGs and output/log-transfer audits.
---

# CMS Condor Job Operator

Operate from evidence tied to an explicit schedd and cluster ID. Treat cluster
IDs as schedd-local and distinguish queue visibility from DAG topology.

## Choose the operation

- For status, progress, or diagnosis, perform the read-only triage workflow.
- For completed-job or transfer questions, add the audit workflow.
- For a new submission, select and record a schedd before submitting.
- For removal, rescue, recovery, or resubmission, inspect recoverable state
  before changing the queue.
- For a repository-specific pilot, let its pilot skill define generation and
  acceptance criteria; use this skill for schedd and Condor operations.

Read [`references/condor-operations.md`](references/condor-operations.md) for
commands, state interpretation, and mutation safeguards.

Before any live Condor read from a restricted Codex sandbox, request approved
unsandboxed network execution. This includes `myschedd`, queue/history
commands, and snapshot or progress scripts that call them. Do not first run the
same command inside the sandbox to demonstrate an expected DNS/configuration
failure. Offline file inspection does not require escalation; queue mutations
still require explicit user authorization.

## Resolve identity first

Collect:

1. Schedd hostname.
2. Top-level DAGMan cluster ID.
3. DAG path or generated bundle when available.
4. Requested operation and whether it is read-only or mutating.

If a schedd was recorded at submission, always pass it explicitly with
`-name`. Do not search by a recent time window when the DAGMan cluster ID is
known. Use the ID as the root of the query.

For a new submission, run `myschedd bump`, then `myschedd show`, and record the
result immediately before submission. Never assume that bump migrates an
existing DAG; continue to operate an existing DAG on its original schedd.

## Triage

For a normal status request, run one concise snapshot and return immediately:

```bash
python3 tools/mcprod_snapshot.py --run <run-dir>
```

For mcprod, this selects the unique attempt named by `submission.json`'s
`current_attempt`, queries only that root cluster on its recorded schedd, and
falls back to matching persistent DAGMan and ledger evidence after the root
leaves the queue. Optional `--schedd` and `--cluster` arguments assert the
recorded identity. This is the one-shot, read-only monitoring path for mcprod.

For an existing legacy DAG, use:

```bash
python3 skills/cms-condor-job-operator/scripts/dag_snapshot.py \
  <dag-cluster> --schedd <schedd>
```

Add `--dag-file <path>` when the root has left the queue. Do not poll or wait
unless the user explicitly asks for continuous monitoring. For topology, use
`./tools/dag_progress.py <dag-cluster> --schedd <schedd> --structure=collapsed`.

For diagnosis or a requested detailed view, add queue and DAG evidence:

```bash
condor_q -name <schedd> -dag <dag-cluster> -nobatch
./tools/dag_progress.py <dag-cluster> --schedd <schedd> --structure=collapsed
```

Then inspect the top DAGMan output, nested SubDAG output, node log, and relevant
job ClassAds. Report separately:

- planned DAG nodes;
- successful, failed, futile, queued, ready, and unready DAG nodes;
- live idle, running, held, and suspended procs;
- DAGMan and SubDAG processes;
- current critical or long-tail nodes.

Do not describe a lower `condor_q` total as lost work until the DAGMan logs show
missing, failed, or removed nodes. Successful jobs normally leave the live
queue.

For idle, held, eviction, retry, and slow-I/O interpretation, follow the
decision checks in the reference. Verify a suspected configuration change in
the live DAGMan or worker log; generated submit files alone do not prove that a
running job loaded it.

## Audit

Use history and persisted logs after jobs leave the queue. Check:

- exit status and termination reason;
- hold and release history;
- eviction count and resource usage;
- stdout/stderr and DAGMan/POST logs;
- output-transfer configuration and transferred files;
- deterministic output manifests or provenance;
- whether completed products match the node's declared outputs.

Separate three outcomes:

1. computation succeeded and transfer succeeded;
2. computation succeeded but logs or outputs were not retained;
3. computation or validation failed.

Do not infer event counts from filenames or job counts.

## Mutating operations

Require clear user authorization before submission, removal, release, or
resubmission. Before removing a DAG:

1. Resolve the exact schedd and top DAG ID.
2. Inspect completed nodes, active descendants, staged outputs, rescue files,
   and deterministic reusable products.
3. Prefer rescue or recovery when completed work can be retained.
4. Remove exact cluster IDs on the original schedd.
5. Query the same IDs afterward and report whether removal was confirmed.

Do not use `condor_submit_dag -force` unless discarding prior state is the
explicit goal.

## Report

Lead with whether progress is normal, degraded, or blocked. Include:

- schedd and DAGMan ID;
- DAG totals and live queue totals;
- active or blocking nodes;
- evidence-backed cause;
- estimated remaining time only when a comparable completed node exists;
- recommended next action and whether it changes live state.
