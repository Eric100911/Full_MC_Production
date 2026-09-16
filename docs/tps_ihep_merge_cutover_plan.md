# TPS CERN MIX / IHEP MERGE+NTUPLE Cutover Plan

## Objective

Move the unfinished `JJP_TPS_MC_v4_1` MiniAOD merge and ntuple stages from
CERN HTCondor to IHEP HepJob while preserving completed processing and merge
outputs. Do not submit the same merge group at both sites.

## Frozen live state

Recorded on 2026-08-06 after the cutover decision:

- Schedd: `bigbird17.cern.ch`
- Root DAGMan cluster: `9474187.0`
- SubDAG: `generated/JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/JJP_TPS_MC_v4_1/job_6/blocks_processing.dag`
- Root DAGMan is held by the user. Do not release it before a cutover-safe
  processing recovery path has been prepared.
- 34 live MERGE jobs are held: 33 were running and group 357 had just become
  idle when the hold was applied.
- MIX payloads were not held or removed: 98 were running and 2 were idle.
- Snapshot before the hold: 657/1900 MIX complete, 93/400 MERGE complete,
  93/400 NTUPLE complete, with no failed nodes.
- No component MiniAOD, merged MiniAOD, ntuple, sidecar, rescue file, or old
  DAG state was deleted.

The held MERGE group indices are:

```text
67 68 69 70 71 72 73 74 75 76 77 78 79 80 82 83
356 357 358 359 360 361 362 363 364 365 366 367 368 369 370 371 372 373
```

Exact held job mapping:

```text
9475471.0 -> 067    9475479.0 -> 068    9475504.0 -> 069
9475529.0 -> 070    9475573.0 -> 071    9475538.0 -> 072
9475658.0 -> 073    9475649.0 -> 074    9475644.0 -> 075
9475684.0 -> 076    9476185.0 -> 077    9476229.0 -> 078
9476263.0 -> 079    9476286.0 -> 080    9476304.0 -> 082
9476290.0 -> 083    9476249.0 -> 356    9476308.0 -> 357
9476244.0 -> 358    9476280.0 -> 359    9476259.0 -> 360
9476238.0 -> 361    9476172.0 -> 362    9475678.0 -> 363
9475665.0 -> 364    9475639.0 -> 365    9475634.0 -> 366
9475651.0 -> 367    9475621.0 -> 368    9475534.0 -> 369
9475518.0 -> 370    9475548.0 -> 371    9475536.0 -> 372
9475491.0 -> 373
```

## Why cut over

CERN MERGE nodes request 1 CPU, 3 GB memory, and 20 GB disk. Live nodes had
run for roughly 5--6 hours while using about 0.5--0.7 GB memory and only about
100 seconds of CPU, which is consistent with cross-site XRootD I/O wait. The
DPS1 IHEP validation merged comparable five-component groups in roughly 2--4
minutes of execution using HepJob `short` walltime.

## Safety rules

- Keep `9474187.0` held until the next session explicitly selects and verifies
  a way to continue unfinished MIX nodes without allowing CERN MERGE nodes to
  race the IHEP handoff.
- Do not blindly release the 34 held MERGE jobs.
- Do not submit IHEP MERGE for a group until all five processing sidecars pass
  the MIX gate.
- Before treating a group as missing, audit its planned merged MiniAOD and
  merge sidecar. A held job may already have staged a reusable product.
- Retain full IHEP URLs in `root://cceos.ihep.ac.cn:1094///store/...` form.
- Do not run `finalize-split --apply` during the cutover.
- Do not edit the original DAG or rescue file in place. Put recovery DAGs,
  filtered manifests, and reports in new timestamped workspaces.

## Next-session procedure

### 1. Reconfirm the frozen identity

```bash
condor_q -name bigbird17.cern.ch 9474187.0 -nobatch
condor_q -name bigbird17.cern.ch \
  -constraint 'DAGManJobId == 9474187 && regexp("^MERGE_", DAGNodeName)' \
  -af ClusterId ProcId JobStatus DAGNodeName HoldReason
python3 skills/cms-condor-job-operator/scripts/dag_snapshot.py \
  9474187 --schedd bigbird17.cern.ch \
  --dag-file generated/JpsiJpsiPhi_MC_Production_v4_2_full/plan_subdags/JJP_TPS_MC_v4_1/job_6/blocks_processing.dag
```

Also audit the 100 MIX payloads that were active at the hold boundary. They can
finish independently while DAGMan is held, so queue disappearance is not
evidence of lost work; use the node log and processing sidecars.

### 2. Choose a cutover-safe MIX continuation

Before releasing the old controller, prepare a new timestamped processing-only
recovery path for unfinished MIX nodes. Derive its DONE set from the persisted
node log and valid processing sidecars. The recovery path must not contain
MERGE, NTUPLE, or FINAL nodes.

Inspect the generated recovery DAG before submission:

```bash
rg -n '^(JOB|PARENT|CATEGORY|MAXJOBS|FINAL)' <processing-only-recovery.dag>
```

Acceptance requirements:

- only missing MIX nodes are runnable;
- completed or valid component outputs are not overwritten;
- there are no MERGE, NTUPLE, or FINAL nodes;
- source blocks and EDM EventIDs are unchanged from the frozen coordinator
  manifest;
- logs use a new persistent path.

Do not release `9474187.0` merely to continue MIX. Submit the inspected
processing-only recovery independently, then keep the old controller held
until final reconciliation.

### 3. Inventory TPS merge groups

Use the frozen coordinator manifest under `job_6` as the task authority. For
each of the 400 groups, record:

- five processing sidecars and their `actual_miniaod_events`;
- merged MiniAOD existence and positive size;
- merge sidecar status, output URL, and `actual_events`;
- ntuple existence and positive size;
- split ntuple sidecar URL.

Classify each group as:

1. complete through NTUPLE;
2. valid MERGE, missing NTUPLE;
3. five valid components, missing MERGE;
4. waiting for one or more MIX components.

The MERGE event expectation is the sum of the five valid processing sidecars;
do not assume 5,000 events.

### 4. Generate the IHEP handoff

Create filtered coordinator-manifest copies in a new workspace. Add the same
split metadata already validated for DPS1 (`output_mode=mix-only`, analysis,
cleanup, target storage, and triple-slash target URL), without modifying the
original coordinator manifest.

Start with a canary containing:

- two of the formerly running CERN MERGE groups with no valid staged merge;
- one group with an existing valid merge but missing ntuple, when available.

Run `audit-split --stage mix`, copy the resulting handoff to IHEP, and generate
an IHEP `merge-ntuple` workspace with:

```text
group: cms
gateway: CMS
MERGE walltime: short
NTUPLE walltime: mid
array index: %{ProcId}
```

Run the pre-submit MERGE audit first. Submit only its generated retry cluster,
so existing valid merges are not rerun.

### 5. Pass gates serially within IHEP

For each batch:

1. submit MERGE;
2. record HepJob cluster ID and UTC start time;
3. monitor with `hep_q -i <cluster>`;
4. inspect logs and run the MERGE audit;
5. submit NTUPLE only after the MERGE gate is complete;
6. verify every ntuple is positive-size and run the NTUPLE audit;
7. run cleanup preview without `--apply`.

The DPS1 run exposed two details that must be checked for TPS:

- CMSSW ntuple jobs can hit `Illegal instruction` on some worker CPUs; retry
  on another worker and consider a conservative CPU/runtime build if repeated.
- Post-container XRootD sidecar upload can fail even after ROOT stageout. Verify
  the ROOT first and recover the exact planned sidecar from an authenticated
  IHEP login node rather than rerunning a successful analyzer.

### 6. Expand only after the canary

After the canary MERGE and NTUPLE gates pass, submit eligible groups in bounded
batches. Re-run the inventory as MIX recovery produces more components. Never
include category-4 groups until all five processing sidecars pass.

### 7. Reconcile the old CERN DAG

After all 400 TPS groups have valid ntuple gates:

- take a final snapshot of `9474187.0` and its held children;
- verify all externally completed MERGE/NTUPLE URLs and sidecars;
- generate a new reconciliation/rescue artifact rather than editing the old
  rescue file in place;
- remove or retire the exact held controller and MERGE clusters only after the
  external completion evidence is recorded;
- query the same cluster IDs afterward to confirm the intended state.

Do not release the old DAG into normal execution after IHEP work has started,
because that would allow duplicate CERN MERGE/NTUPLE jobs.

## Completion criteria

- All 1900 TPS processing nodes have valid component MiniAOD sidecars.
- All 400 merge sidecars pass and their `actual_events` equal the sum of their
  five processing sidecars.
- All 400 ntuples are positive-size and have sidecars naming the planned URL.
- No CERN and IHEP MERGE job raced for the same group.
- Every CERN and HepJob cluster ID, UTC interval, log directory, and gate is
  recorded.
- Cleanup remains dry-run only unless separately authorized.
