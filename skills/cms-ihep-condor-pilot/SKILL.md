---
name: cms-ihep-condor-pilot
description: Generate, submit, monitor, and triage small Full_MC_Production pilot DAGs for the CMS T2_CN_Beijing/IHEP workflow. Use when working in this repository on existing-LHE block SubDAG pilots, MiniAOD merge pilots, ntuple-from-merged-MiniAOD pilots, HTCondor DAGMan submission/monitoring, IHEP XRootD URL handling, or sandbox-vs-live XRootD diagnosis.
---

# CMS IHEP Condor Pilot

Use this skill for operational pilot work in the `Full_MC_Production` repo.
Prefer it for small, reversible pilots and live DAGMan checks.

## Core rules

- Use IHEP full ROOT URLs in triple-slash form:

  ```text
  root://cceos.ihep.ac.cn:1094///store/...
  ```

  Do not normalize to `:1094//store/...` or `:1094/store/...`.
- Use `xrdfs root://cceos.ihep.ac.cn:1094/ ls /store/...` for directory
  listings.
- Do not diagnose IHEP XRootD from the restricted Codex sandbox. Sandbox
  `xrdfs`/`xrdcp` can fail with `[FATAL] Invalid address` even when live shell
  access works. Use approved unsandboxed commands for XRootD and HTCondor.
- Put scratch, live logs, and preserved test outputs under `/tmp/chiw/`.
  Keep DAG bundles under repo `generated/` or another submit-visible persistent
  filesystem.
- For known existing-LHE pilots, use `--skip-lhe-generation --no-scan-existing`
  with configured exact paths. Avoid remote-scan-dependent generation from
  sandboxed or unreliable-network contexts.

## Existing-LHE MiniAOD merge pilot

Before submission, state:

- campaign and source pools;
- source-file count (`--jobs`);
- events per block;
- expected blocks per source;
- expected mixed processing blocks;
- MiniAOD merge target;
- expected merged MiniAOD/ntuple count.

Use the recipe and triage commands in
[`references/pilot-recipes.md`](references/pilot-recipes.md) when generating or
submitting the pilot.

## Required inspection before submission

Inspect the DAG and node configs before `condor_submit_dag`:

```bash
rg -n '^(JOB|SUBDAG|SCRIPT|PARENT|CATEGORY|MAXJOBS|FINAL)' <dag>
python3 -m json.tool <output-dir>/metadata.json | sed -n '1,220p'
python3 -m json.tool <output-dir>/node_configs/planning/PLAN_*.json | sed -n '1,120p'
python3 -m json.tool <output-dir>/node_configs/coordination/COORD_*.json | sed -n '1,180p'
```

Check for:

- no `LHE_*` jobs when the user requested existing LHEs;
- configured LHE URLs use `root://cceos.ihep.ac.cn:1094///store/...`;
- coordinator config has `enable_ntuple: true`;
- merge mode has `miniaod_merge_events > 0`;
- log root is under `/tmp/chiw/` unless the user intentionally chose another
  non-AFS log filesystem.

## Live monitoring

After submission, verify live state rather than relying on generated configs:

```bash
condor_q <dag-cluster> -nobatch
tail -80 <dag>.dagman.out
tail -80 <dag>.nodes.log
```

For generated block SubDAGs, inspect the coordinator output after the
coordinator node finishes:

```bash
rg -n '^(JOB|PARENT|CATEGORY|MAXJOBS|FINAL)' <output-dir>/plan_subdags/<campaign>/job_0/blocks_processing.dag
python3 -m json.tool <output-dir>/plan_subdags/<campaign>/job_0/coord_manifest_<campaign>_0.json
```

## Common failure interpretation

- `xrdfs`/`xrdcp` gives `[FATAL] Invalid address` only in the sandbox: rerun
  outside the sandbox before changing paths.
- Existing-LHE pilot contains `LHE_*` jobs: generation fell back to producing
  LHEs; regenerate with `--skip-lhe-generation --no-scan-existing` and inspect
  the DAG again.
- Top-level DAG warns `miniaod_merge`/`ntuple` category has no assigned nodes:
  this is expected before the coordinator generates the block SubDAG.
- Large remote MiniAOD merge smoke is slow: prefer a small pilot DAG that
  produces tiny MiniAODs, then merges those.
