---
name: cms-continuous-production
description: Operate continuous Full_MC_Production workflows across CERN HTCondor and IHEP HepJob. Use when running or monitoring a multi-batch production (inventory LHE/MIX/MERGE/NTUPLE EOS state, build IHEP merge-ntuple handoff batches, classify cutover progression, recover split_ntuple_manifest sidecars, expand eligible groups, or reconcile a retired CERN DAG). Delegate general schedd/hep_q triage, DAG progress, and removal to the sibling cms-condor-job-operator skill. Delegate efficiency/budget analysis to cms-lhe-capacity-planner.
---

# CMS Continuous Production Operator

Operate long-running production workflows that split the chain across two sites:
CERN HTCondor handles MIX (processing); IHEP HepJob handles MERGE and NTUPLE.

## Delegation

- **Schedd/Condor triage, `myschedd bump`, DAG progress, removal, transfer audit** →
  use `cms-condor-job-operator`.
- **Source-ratio planning, per-campaign block budgets, storage estimates** →
  use `cms-lhe-capacity-planner`.
- **Pilot generation, submission, and validation** →
  use `cms-ihep-condor-pilot`.

## Core rules

- Use IHEP XRootD URLs with the explicit endpoint and **triple slash** before the
  LFN: `root://cceos.ihep.ac.cn:1094///store/...`. Do not normalize to two
  slashes or drop the port.
- Run XRootD operations from an **authenticated IHEP login node** (not a
  restricted sandbox). The post-container `xrdfs mkdir` can fail with
  `Operation not permitted`; sidecar recovery (`recover_ntuple_sidecars.py`)
  must run from an authenticated node.
- The **frozen coordinator manifest** is the task authority. Never infer event
  counts from filenames or file sizes. Do not probe campaign-directory
  variants on the worker.
- The `split_workflow.py` state machine defines the gate model:
  **MIX gate (split manifest) → MERGE gate → NTUPLE gate → finalize**.
  Audit each stage before advancing. The merge gate is a hard precondition
  baked into the ntuple submit script.
- **Do NOT re-audit while a retry cluster is in flight.** Retry manifests are
  now snapshot-isolated (timestamped paths), but the stable submit script is
  rewritten on every audit. Re-auditing while jobs are running can still shift
  the submit target. Wait for the cluster to exit the queue first.

## HepJob submission (IHEP)

`hep_sub` needs `export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH`.
ntuple must be pinned round-robin to the **11 AVX2-safe nodes**
(ams017/018/019/020 + cws047/048/049/051/052/053/054) via `-wn`; merge runs
unpinned (cmssw-el8, unaffected). Rebuild the proxy bundle into the workspace
before submitting — never the shared `/tmp/x509up_u$UID` path. Batch builds must
be serialized (IHEP login-node fork exhaustion). Full lessons:
`references/hepjob-submission.md`; generated split workspaces provide the
canonical `submit_ntuple_<campaign>.sh`. For older workspaces, use
`scripts/submit_ntuple_pinned.sh <campaign> <workspace> <proxy>`.

## Classification categories

| Category | Status | Next action |
|----------|--------|------------|
| **cat1** | NTUPLE sidecar + ROOT both valid | Done |
| **cat2** | Valid MERGE, missing NTUPLE | Submit ntuple |
| **cat3** | Five valid component blocks, missing MERGE | Submit merge |
| **cat4** | One or more MIX components missing | Wait for MIX or re-run |

## Workflow scripts

All live under `skills/cms-continuous-production/scripts/`.

### `tps_cutover_inventory.py`

Audits live EOS state against the frozen coordinator manifest and classifies
every merge group as cat1–4. Campaign, job index, and the EOS output layout are
derived from the coordinator manifest. Re-runnable with a local manifest cache.
Env overrides: `INV_EOS`, `INV_COORD`, `INV_WORK`, `INV_WORKERS`, `INV_TIMEOUT`,
`INV_TMP`.

```
# from CERN (reads AFS coord manifest by default):
INV_COORD=/path/to/coord_manifest.json \
INV_WORK=/path/to/output/workspace \
INV_WORKERS=8 \
python3 skills/cms-continuous-production/scripts/tps_cutover_inventory.py

# from IHEP (point at a handoff coordinator):
INV_COORD=/scratchfs2/.../handoffs/<batch>/plan_subdags/.../coord_manifest_....json \
INV_EOS=root://cceos.ihep.ac.cn:1094 \
INV_WORK=/scratchfs2/.../workspace \
python3 skills/cms-continuous-production/scripts/tps_cutover_inventory.py
```

Output: `<work>/inventory/<campaign>_job<job_index>_inventory.json` with summary
counts and per-group detail, plus a separate `*_failures.json` report. The
inventory is read-only: it does not submit, retry, or remove jobs.

### `ihep_build_batch.py`

Fast, site-local IHEP batch builder. Filters the coordinator manifest to a
chosen set of merge groups, runs `audit-split --stage mix`, generates the
merge-ntuple workspace, and runs the pre-submit merge audit — all on IHEP.
The coordinator manifest is the authority: campaign, job index, and the EOS
storage layout are derived from it. Site paths default from `IH_PFX`, `IH_TS`,
`IH_EOS`, `IH_LHE_POOL_LFN` and are overridable per flag.

```
python3 skills/cms-continuous-production/scripts/ihep_build_batch.py \
  --coord /path/to/coord_manifest.json \
  [--ts <workspace-ts>] [--proxy <x509>] \
  <batch_name> <comma-separated-groups>
```

### `recover_ntuple_sidecars.py`

Recovers post-container `split_ntuple_manifest` sidecars that failed to upload
(`Operation not permitted` in the sandbox). Verifies the ntuple ROOT is
positive-size, then writes the correct sidecar from the login node. EOS
endpoint defaults to `root://cceos.ihep.ac.cn:1094`, overridable with
`--eos` / `RECOVER_EOS`.

```
X509_USER_PROXY=/path/to/proxy \
python3 skills/cms-continuous-production/scripts/recover_ntuple_sidecars.py <workspace> [--force]
```

## Batch workflow (production cycle)

1. **Inventory** → classify all groups (`tps_cutover_inventory.py`).
2. For each cat3 batch: **build** (`ihep_build_batch.py`), **submit merge**
   (`bash submit_merge_<campaign>_retry.sh`), **audit merge gate**
   (`dag_generator.py audit-split --stage merge`), retry if incomplete.
3. **Submit ntuple** only after the merge gate exists.
4. **Recover sidecars** (`recover_ntuple_sidecars.py`) after ntuple ROOTs land.
5. **Audit ntuple gate** → `gates/ntuple_<campaign>.json`.
6. Optional **count candidate yield**: `tools/count_ntuple_candidates.py
   <run>/tasks/<campaign>/ntuple_tasks.json` (with `X509_USER_PROXY`), then
   cross-check `events_total` against the merge gate's summed `actual_events`.
7. Optional **finalize** (cleanup): `dag_generator.py finalize-split` (dry-run
   default; `--apply` only with explicit authorization). Note: `--apply` must be
   run with `X509_USER_PROXY` set, or `xrdfs rm` fails `Operation not permitted`
   on `cmsuser`-owned block outputs.
7. **Reconcile** the old CERN DAG: take final snapshot, verify external
   completion evidence, record a reconciliation artifact, and remove/retire
   the held controller only after evidence is recorded.

## Known failure modes

| Symptom | Cause | Response |
|---------|-------|----------|
| `Illegal instruction (rc=132)` in ntuple | Ivy Bridge node **cws050/cws055 only** (no AVX2; cws052 is safe) | Resubmit pinned to an AVX2-safe node via `submit_ntuple_pinned.sh` — see `references/hepjob-submission.md` |
| `Operation not permitted` sidecar upload | Post-container sandbox EOS mkdir | Run `recover_ntuple_sidecars.py` from login node |
| ProcId outside task count | Retry manifest rewritten while cluster in flight | Wait for cluster to finish, re-audit, submit clean retry |
| `[FATAL] Invalid address` | Restricted sandbox XRootD | Reproduce on normal IHEP shell with same proxy |
| xrdcp returns 3010/3011 | EOS directory permissions or proxy expiry | Check proxy, `xrdfs stat` the parent dir, `xrdfs mkdir -p` if missing |
