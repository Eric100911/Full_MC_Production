# mcprod local wave seam refactor

Date: 2026-09-20  
Status: implementation plan; no runtime code has been changed  
Scope: the `production-core` wave controller and its CERN worker DAGs

This plan replaces the remote-validation proposal in
`mcprod_seam_eos_access_plan_2026-09-20.md`. The older document remains the
incident record. This document describes the implementation we intend to build.

## What changes

The submit-host PRE script should have one job: read local records, commit the
previous wave, and prepare the next wave. It should never contact EOS.

Today, the source seam checks every staged block with a separate `xrdfs stat`.
The failed pilot had 511 blocks from four sources, so the seam attempted 515
remote size checks: 511 blocks and four retained originals. We do not have the
exception from the failed PRE script, so this remains the leading explanation,
not a proven root cause.

The PLAN worker already gives us a cleaner contract. It uploads each block,
checks the remote size, and only then returns `result.json` to AFS. That result
is the producer manifest. The seam can validate it against the immutable task
without reading EOS again.

The same rule applies to later stages:

```text
PLAN worker
  writes blocks to EOS and returns an AFS manifest
          |
          v
PRE seam
  validates local records and prepares the next wave
  no xrdfs, xrdcp, or transfer container
          |
          v
MIX worker
  downloads selected blocks and checks size plus LHE count
          |
          v
MINIAOD -> ARCHIVE -> RECLAIM
  workers validate, preserve, and delete remote products
```

There is no aggregation worker and no `wave_manifest.json` on EOS. Such a job
would read the same N objects again and would duplicate the PLAN result that
already exists.

## Correctness contract

The design splits validation into two moments.

At publication time, the producer verifies the upload and writes a manifest.
At consumption time, the consumer verifies what it downloaded. The PRE seam
only checks that the producer manifest agrees with the frozen task.

For a PLAN result, the seam checks:

- task ID, stage, status, and runtime release;
- source pool, source ID, and input event count;
- exact block IDs and ordering;
- output URL prefix and `.lhe.gz` suffix;
- positive byte sizes and valid per-block event counts;
- full blocks followed by at most one partial tail;
- the planned event total and retained-original metadata.

It deliberately does not call `client.size()`.

The target shape is:

```python
def validate_result_envelope(task, outcome, runtime_release):
    if outcome.get("task_id") != task["task_id"]:
        raise RuntimeError("worker result has the wrong task ID")
    if outcome.get("stage") != task["stage"]:
        raise RuntimeError("worker result has the wrong stage")
    if outcome.get("status") != "ok":
        raise RuntimeError("worker result is not successful")
    if outcome.get("runtime_release") != runtime_release:
        raise RuntimeError("worker result has the wrong runtime release")


def validate_planner_ref(ref, *, source, parameters, index):
    expected_id = (
        f"BLOCK:{source['pool']}:{source['source_id']}:{index:06d}"
    )
    expected_prefix = parameters["block_output_dir"].rstrip("/") + "/"
    events = ref.get("events")

    if ref.get("artifact_id") != expected_id:
        raise RuntimeError("planner block identity is invalid")
    if ref.get("kind") != "lhe-block":
        raise RuntimeError("planner output is not an LHE block")
    if not str(ref.get("url", "")).startswith(expected_prefix):
        raise RuntimeError("planner block URL is outside its output directory")
    if not str(ref["url"]).endswith(".lhe.gz"):
        raise RuntimeError("planner block is not compressed LHE")
    if not isinstance(events, int) or isinstance(events, bool) or events <= 0:
        raise RuntimeError("planner block event count is invalid")
    if events > int(parameters["events_per_block"]):
        raise RuntimeError("planner block exceeds the configured block size")
    if int(ref.get("size_bytes", 0)) <= 0:
        raise RuntimeError("planner block has no declared byte size")
```

MiniAOD and archive results receive equivalent structural checks. Archive
members must match the frozen archive inputs by artifact ID, URL, kind, event
count, and declared size. Those checks are local because the archive worker has
already downloaded the members and verified the published archive.

## Validate again when a file is used

Trusting a producer manifest does not mean trusting a later download blindly.
`materialize_input()` should reject a local file whose size differs from the
manifest:

```python
def materialize_input(reference, work_dir, transfer=None):
    destination = _materialize(reference, work_dir, transfer)
    expected = reference.get("size_bytes")
    if expected is not None and destination.stat().st_size != int(expected):
        destination.unlink(missing_ok=True)
        raise RuntimeError(
            f"downloaded size disagrees with {reference['artifact_id']}"
        )
    return destination
```

Before showering, MIX should also count every LHE block separately. Checking
the combined source is not enough because it would not identify which manifest
entry is wrong.

```python
for identity in slot["inputs"]:
    reference = references[identity]
    local_path = materialize_input(reference, work_dir)
    actual_events = count_lhe_events(local_path)
    if actual_events != int(reference["events"]):
        raise RuntimeError(
            f"{identity}: read {actual_events} LHE events, "
            f"expected {reference['events']}"
        )
    paths.append("file:" + str(local_path))
```

A size or event-count mismatch is a hard MIX failure and uses the normal Condor
retry. It is not covered by `report-and-truncate`. That policy remains available
for a physics or MiniAOD shortfall after valid LHE inputs have been processed.

The existing exhaustive-phi check on `consumed_lhe_events` stays in place. It
tests exposure semantics; it is not a replacement for validating each input
block before showering.

## Move deletion into a worker

Processing waves currently delete staged products from the PRE script. This is
another large set of submit-host EOS calls, so validation alone is not enough.

Add a `reclaim` worker stage using the `utilities-el9` profile and the existing
`lhe_coordination` category:

```python
STAGE_RUNTIME = {
    # existing stages omitted
    "archive": "utilities-el9",
    "reclaim": "utilities-el9",
}

CATEGORIES = {
    # existing stages omitted
    "archive": "lhe_coordination",
    "reclaim": "lhe_coordination",
}
```

Each processing SubDAG ends with this dependency chain:

```python
dependencies.append(([item["task_id"] for item in mini_records], archive_id))
dependencies.append(([archive_id], reclaim_id))
```

The reclaim task receives exact artifact references and the exact staging
prefix for this run. It performs no discovery or alternate-path probing.

```python
def reclaim_one(client, ref, staging_prefix):
    prefix = staging_prefix.rstrip("/") + "/"
    url = str(ref["url"])
    if not url.startswith(prefix):
        raise ValueError(f"refusing to reclaim outside run staging: {url}")

    removed = []
    for kind, target in (("payload", url),
                         ("sidecar", ref.get("sidecar_url"))):
        if not target:
            continue
        try:
            client.remove(target)
            disposition = "removed"
        except FileNotFoundError:
            disposition = "already-missing"
        removed.append({"kind": kind, "url": target,
                        "disposition": disposition})
    return {"artifact_id": ref["artifact_id"], "objects": removed}
```

The implementation should run the deletion loop inside one utilities container
so thousands of deletions do not start thousands of containers. Missing files
are successful idempotent cleanup. Authentication and other remote errors fail
the task.

Only an explicit `sidecar_url` is deleted. PLAN blocks currently have no EOS
sidecars, so the controller must not manufacture `<block>.json` deletion calls.

The seam advances pool cursors and job offsets only after the reclaim result is
present and valid. A retry after partial deletion is therefore safe.

When all processing targets are met, the controller uses the existing spare
wave slot for a terminal reclaim task. It removes unused blocks only after local
archive receipts prove that the corresponding original LHE files were archived.
If there is nothing left to remove, completion can be committed locally without
creating an empty SubDAG.

## Keep growing records out of state.json

Moving only `blocks` and `resident` would not solve the scaling problem.
`archives` and completed merge groups also grow with the run and are rewritten
at every seam.

New runs use a versioned state and immutable local records:

```text
ledger/
  inventory/<pool>/<source_id>.json
  archives/<step>.json
  groups/<campaign>/<profile>/<ordinal>.json
  waves/<step>.json
```

Pool, source, campaign, and profile names must pass the existing DAG token
validation before they are used as path components.

An inventory record wraps the producer manifest without changing it:

```json
{
  "schema": "mcprod-source-inventory-v1",
  "accepted_step": 0,
  "source_index": 3,
  "pool": "pool_2jpsi_g",
  "source_id": "source_000003",
  "producer_result": {
    "task_id": "PLAN_000003",
    "stage": "plan",
    "status": "ok",
    "runtime_release": "...",
    "outputs": [],
    "originals": []
  }
}
```

Only inventory records whose `accepted_step` is less than
`state["next_step"]` are committed. A record written immediately before a crash
therefore remains invisible until the state commit succeeds.

The new mutable state has this shape:

```json
{
  "state_schema": "mcprod-state-v2",
  "status": "running",
  "next_step": 12,
  "pending": {},
  "pool_cursors": {},
  "job_offsets": {},
  "merge_buffers": {},
  "merge_group_counts": {}
}
```

`pending` remains bounded by the configured wave width. Each merge buffer is
bounded by its configured merge target. Completed blocks, archives, and groups
do not return to `state.json`.

Allocation loads committed source manifests in run-spec source order and keeps
the block order recorded by PLAN. Storage use is derived from unconsumed blocks
and originals that do not yet appear in a committed archive receipt.

Completed merge groups are written as individual immutable files. The counter
in `merge_group_counts` determines which ordinals are committed. Handoff export
reads those files and may materialize `merge_groups.json` as a derived operator
snapshot; the wave seam does not rewrite the aggregate file.

Archive restoration reads committed archive receipts rather than the removed
`state["archives"]` list.

This state format is for newly generated runs. Existing run directories retain
their frozen runtime and are not migrated.

## Make folding retry-safe

Every immutable write uses create-or-compare behavior. An identical file means
an earlier attempt reached that point. Different content at the same path is a
hard conflict.

```python
def write_immutable_json(path, payload):
    path = Path(path)
    if path.exists():
        if read_json(path) != payload:
            raise RuntimeError(f"immutable record conflict: {path}")
        return
    atomic_json(path, payload)
```

A wave is folded in this order:

```python
def finish_pending(root, spec, state):
    pending = state["pending"]

    # On a first attempt, read results/<task>.json. On a retry after archival,
    # recover them from the immutable wave receipt.
    results = load_and_validate_pending_results(root, spec, pending)

    write_inventory_and_archive_records(root, spec, state, pending, results)
    next_state, groups = calculate_next_state(root, spec, state, pending, results)
    write_group_records(root, groups)

    receipt = build_wave_receipt(pending, results, groups)
    write_immutable_json(root / "ledger/waves" /
                         f"{pending['step']:06d}.json", receipt)

    remove_raw_result_fragments(root, pending["task_ids"])
    atomic_json(root / "state.json", next_state)
```

Raw result fragments are removed only after the complete wave receipt is
durable. If the process crashes before that point, the raw results remain. If it
crashes afterwards, the receipt contains everything required to retry.

`state["next_step"]` is the commit marker. A separate `applied_wave` value would
duplicate that information and is not needed.

If the final state commit succeeds but creating `.complete` is interrupted, the
next PRE invocation sees `status == "complete"`, recreates the sentinel, and
returns the normal skip code.

## Capture PRE failures

The generated `advance.sh` redirects both streams to a stable per-step log while
preserving the timeout and exit code:

```bash
#!/bin/bash
set -euo pipefail

step="${1:?missing wave step}"
printf -v step_name '%06d' "${step}"
log="<run-dir>/logs/advance_${step_name}.log"
exec >>"${log}" 2>&1

printf '[%s] advance-wave step=%s runtime=%s\n' \
    "$(date --iso-8601=seconds)" "${step}" "<runtime-release>"

if [[ -f "<run-dir>/.complete" ]]; then
    exit 99
fi

exec timeout 3600 <mcprod> internal advance-wave \
    --run-dir <run-dir> --step "${step}"
```

The real generated script keeps the existing fixed `PATH`, proxy checks,
`X509_USER_PROXY`, and `X509_CERT_DIR`. It must not print proxy contents.

## Implementation order

1. Add PRE logging and tests for exit 0, exit 99, ordinary failure, and timeout.
2. Add downloaded-size and per-block LHE-count checks.
3. Add the reclaim stage and processing/final-cleanup dependencies.
4. Add immutable ledger records and `mcprod-state-v2`.
5. Replace seam-side remote validation with local structural validation.
6. Update handoff export and archive restoration to read committed records.
7. Generate a fresh runtime and run a new pilot. Do not cut an existing run over
   to the new state format.

The code can land in reviewable commits, but production should switch only when
the complete invariant holds: `advance-wave` performs no EOS operation.

## Tests and acceptance

Add focused tests for these cases:

- A valid PLAN result advances even when its test URLs do not exist.
- Patching every `TransferClient` operation to raise does not affect
  `advance-wave`.
- Wrong task metadata, block IDs, URL prefixes, order, counts, or sizes leave
  state unchanged.
- A downloaded byte-size mismatch removes the bad local copy and fails.
- A per-block LHE event-count mismatch fails before showering.
- Reclaim rejects a path outside the run prefix.
- Reclaim treats missing payloads and sidecars as successful retries.
- Reclaim does not issue a sidecar deletion when `sidecar_url` is absent.
- The processing DAG contains `MIX -> MINIAOD -> ARCHIVE -> RECLAIM`.
- The terminal cleanup wave runs before `.complete` is written.
- Crashes after each immutable write, after the wave receipt, after raw-result
  cleanup, and after the state commit do not duplicate a transition.
- A synthetic inventory with at least 50,000 blocks produces the same block
  allocation, job IDs, and RNG seeds as the current allocator while keeping
  `state.json` bounded by wave configuration.
- Handoff export reads only committed group ordinals.
- Archive restoration ignores uncommitted archive records.

Run the repository-required checks from `AGENTS.md`, plus the production-core
worker, lifecycle, handoff, and storage-safety tests. Generate and inspect a DAG
that exercises source, processing, reclaim, and terminal cleanup phases.

The live acceptance pilot must contain at least two source waves, one processing
reclaim, and terminal cleanup. Record the usual event count, source-file count,
events per block, expected blocks per source, merge target, and subprocess
coverage before submission. Acceptance requires:

- PRE logs contain the requested step and any exception text;
- no PRE log shows `xrdfs`, `xrdcp`, or a transfer-container launch;
- worker logs show input validation and reclamation;
- state cursors move only after reclaim succeeds;
- unused blocks disappear only after their originals are present in a committed
  archive receipt;
- a rescued or retried wave produces the same ledger and state as an
  uninterrupted wave.

## Boundaries

This refactor covers the `production-core` CERN wave seam and the worker tasks
needed to remove its EOS access. It does not change the legacy
`dag_generator.py` workflow, IHEP processing internals, physics selection, block
allocation policy, or `report-and-truncate` physics semantics.
