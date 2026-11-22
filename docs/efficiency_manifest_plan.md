# Prepare JJP Efficiency Manifest Workflow

> **Unimplemented historical proposal.** The proposed
> `prepare-efficiency-manifest` subcommand does not exist in `dag_generator.py`.
> Do not run commands from this plan or treat its fixed 1,000-file assumptions
> as current production requirements. Current manifest behavior is documented
> in `README.md` and `docs/testing.md`.

## Summary

Add a manifest/checking path for existing `MC_Production_v3` JJP efficiency
ntuples. The comparison group is `JJP_ALL`: `JJP_SPS_CS`, `JJP_SPS_G`,
`JJP_DPS1`, `JJP_DPS2_CS`, `JJP_DPS2_G`, and `JJP_TPS`. Each subprocess is
expected to have exactly 1000 `output_ntuple.root` files, and downstream
efficiency/modeling uncertainty comparison remains handled by
`run-multileppat-efficiency`.

## Key Changes

- Add a `dag_generator.py prepare-efficiency-manifest` subcommand that reuses
  existing campaign expansion, `MC_Production_v3` constants, JJP-only
  validation, and manifest URL conventions.
- Default behavior:
  - `--campaign JJP_ALL`
  - `--jobs 1000`
  - output `ntuple_manifest.json`
  - verify each subprocess has exactly 1000 expected ntuples under
    `output/<campaign>/<job_id>/output_ntuple.root`
  - fail with a clear missing/extra summary if the remote or local sample is
    incomplete.
- Support `--local-output-base` for local/test manifests and `--no-verify` for
  fast manifest rendering when XRootD is unavailable.
- Write a compact `efficiency_manifest_summary.json` beside the manifest with
  campaign names, expected count, found count, missing paths, extra numeric job
  dirs, and output manifest path.
- Do not reimplement efficiency calculation or modeling uncertainty math in
  this repo.

## Documentation

- Update `README.md` with the production command for the guaranteed 1000-file
  `MC_Production_v3` sample and the downstream call:
  `run-multileppat-efficiency --input-file-manifest ntuple_manifest.json --samples <JJP_ALL list> ...`
- Update `AGENTS.md` to state that JJP efficiency preparation uses
  `prepare-efficiency-manifest`, expects 1000 ntuples per subprocess, and treats
  per-subprocess efficiency differences as the modeling-uncertainty input.
- Refresh `integration_plan.md` only if needed to remove stale references that
  conflict with the implemented command.

## Test Plan

- Static checks:
  `python3 -m py_compile dag_generator.py`
  `bash -n processing/run_chain.sh tests/run_all_tests.sh tests/submit_tests.sh`
- Add a local smoke test creating a small fake local output tree, then run:
  `python3 dag_generator.py prepare-efficiency-manifest --campaign JJP_ALL --jobs 3 --local-output-base <tmp> --output-dir <tmp/out>`
- Verify the smoke manifest has six JJP keys and three paths per key.
- Verify the checker fails when one expected ntuple is missing.
- Run one existing DAG smoke:
  `python3 dag_generator.py generate-test --campaign JJP_DPS1 --jobs 1 --max-events 5 --enable-ntuple --efficiency-ntuple --dry-run`

## Assumptions

- Efficiency preparation is JJP-only because the existing efficiency ntuple
  config and downstream contract are JpsiJpsiPhi-focused.
- `JJP_ALL` is the modeling comparison set, including `JJP_TPS`.
- The 1000 ntuples are indexed as job IDs `0` through `999` in
  `MC_Production_v3/output/<campaign>/<job_id>/output_ntuple.root`.
