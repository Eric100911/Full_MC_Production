---
name: cms-ihep-condor-pilot
description: Generate, submit, rerun, and validate small Full_MC_Production pilot DAGs for the CMS T2_CN_Beijing/IHEP workflow. Use when choosing a representative pilot, preparing its runtime, inspecting generated block SubDAG topology, running the worker mock, submitting a pilot, or checking its physics outputs and event accounting. Delegate general schedd selection, myschedd bump, HTCondor triage, DAG progress, removal, and transfer audit to the sibling cms-condor-job-operator skill.
---

# CMS IHEP Condor Pilot

Use this skill for pilot-specific work in the `Full_MC_Production` repo.
Prefer small, reversible pilots that exercise the production path end to end.

## Use the general Condor skill

Before selecting a schedd, submitting, monitoring, triaging, auditing transfer,
or removing a pilot, read and follow
[`../cms-condor-job-operator/SKILL.md`](../cms-condor-job-operator/SKILL.md).
Let that skill own `myschedd bump`, explicit schedd/cluster identity, live queue
interpretation, history, rescue, and mutation safeguards.

Keep this skill responsible for:

- representative campaign and source selection;
- event, file, block, merge, and subprocess coverage;
- worker mock and generated-DAG inspection;
- pilot-specific output and event-accounting acceptance.

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
- For small existing-LHE `generate-test` pilots, keep positive `--max-events`.
  It auto-caps each PLAN node through `--lhe-max-events-per-plan` after shuffle
  ordering. Do not pair full-size existing LHE files with tiny
  `--lhe-events-per-block` values and an uncapped planner.
- Planner caps and processing budgets are independent. For tiny block pilots,
  set `--target-mixed-events`, `--normal-max-lhe-events`, and
  `--phi-max-lhe-events` explicitly so every repeated source occurrence can be
  filled from the planned blocks.

## Existing-LHE MiniAOD merge pilot

Before submission, state:

- campaign and source pools;
- source-file count (`--jobs`);
- events per block;
- expected blocks per source;
- expected mixed processing blocks;
- MiniAOD merge target;
- expected merged MiniAOD/ntuple count.

Use the generation and inspection recipe in
[`references/pilot-recipes.md`](references/pilot-recipes.md) when generating or
submitting the pilot.

## Test before submission

For worker-runtime changes, run the production-style local mock:

```bash
./tests/mock_test_worker.sh
```

Make the mock reproduce the worker container, transferred bundle, proxy,
working directory, CMSSW project initialization, and staged configuration.
Also run the repository checks required by `AGENTS.md` for the changed paths.
Do not accept a host-only shortcut as proof of worker compatibility.

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

Use the general Condor skill with the recorded schedd and DAGMan cluster ID.
Verify live state rather than relying on generated configs.

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
- PLAN node splits tens of thousands of tiny blocks: regenerate the pilot with
  positive `--max-events` or explicit `--lhe-max-events-per-plan`.
- Large remote MiniAOD merge smoke is slow: prefer a small pilot DAG that
  produces tiny MiniAODs, then merges those.

## Pilot acceptance

After completion, verify:

- expected processing, merge, ntuple, final, and summary nodes succeeded;
- output manifests and provenance were retained;
- declared HepMC, MiniAOD, and Ntuple products exist;
- event counts come from the appropriate product-aware tool, not filenames;
- `report-and-truncate` warnings and unused-source accounting are present when
  exercised;
- stdout, stderr, worker logs, POST logs, and final inventory are transferred
  or their absence is explicitly reported;
- source files and repeated source slots consumed distinct planned blocks.

For source-efficiency and LHE-budget conclusions, use the sibling
[`../cms-lhe-capacity-planner/SKILL.md`](../cms-lhe-capacity-planner/SKILL.md).
