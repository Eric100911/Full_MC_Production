# Repository Guidelines

This branch targets the `T2_CN_Beijing` workflow. Keep this file short and
durable; detailed procedures belong in `docs/testing.md` and path details in
`docs/directory_path_reference.md`.

## Project Map

- `dag_generator.py` is the main entry point for listing campaigns, validating
  environment, preparing runtime bundles, and generating DAGs.
- `processing/run_chain.sh` is the worker-side chain from showering through
  MiniAOD; ntuple is usually a separate DAG node.
- `tools/plan_lhe_blocks.py` and `tools/coordinate_lhe_blocks.py` implement the
  LHE block planner/coordinator and generated block SubDAGs.
- Exact counted LHE inventories, together with versioned specs under
  `common/campaign_job_specs/`, are authoritative for inventory-driven
  campaigns; preserve their checksums and selected source IDs.
- `tools/dag_progress.py` reports live and completed progress for nested DAGMan
  and block SubDAG workflows.
- Shared storage paths and processing defaults live in
  `common/node_config_defaults.json`.
- Campaign CMSSW configs live under `common/cmssw_configs/`.
- The upstream ntuple analyzer is the `external/TPS-Onia2MuMu` submodule; use
  the prebuilt `common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz` when
  available.

## Repository Skills

Repository skill sources live under `skills/`. Explicit `$skill-name`
invocation may not be available, so read the matching `SKILL.md` directly when
the task matches:

- Use `skills/cms-ihep-condor-pilot/SKILL.md` to generate, rerun, and validate
  Full_MC_Production pilots.
- Use `skills/cms-condor-job-operator/SKILL.md` for `myschedd bump`, DAGMan
  progress, idle/held triage, transfer audits, rescue, recovery, and removal.
- Use `skills/cms-lhe-capacity-planner/SKILL.md` for inventory-based source
  ratios, utilization, block capacity, and retained-storage estimates.
- Pilot operations must reuse the Condor skill for live queue inspection and
  mutations.

## Development Commands

- List workflows: `python3 dag_generator.py list --kind all`
- Static validation: `python3 dag_generator.py validate --campaign JJP_DPS1 --scan-existing`
- Smoke DAG: `python3 dag_generator.py generate-test --campaign JJP_DPS1 --output-dir tests/generated/smoke --output smoke.dag`
- Runtime bundle: `python3 dag_generator.py prepare-runtime --output-dir <dir> --include-ntuple --cmssw15-runtime-tarball <tarball>`
- Default checks: `./tests/run_all_tests.sh`
- Local worker mock: `./tests/mock_test_worker.sh`
- LHE splitter test: `./tests/test_lhe_shuffle_split.sh`

For pilot submission, monitoring, output verification, and optional MiniAOD
merge smoke tests, follow `docs/testing.md`.

## Coding and Naming

- Python: PEP 8, 4-space indentation, `snake_case`, uppercase constants for
  fixed paths/settings.
- Bash: `bash` with `set -euo pipefail` where practical; prefer long-form
  flags already used by the repo.
- Preserve existing names:
  - pools such as `pool_jpsi_CSCO_g`;
  - shower modes such as `phi_mpi_off`;
  - analysis types `JJP` and `JUP`;
  - DAG categories `lhe`, `processing`, `ntuple`, `lhe_planning`,
    `lhe_coordination`, `block_processing`, and `miniaod_merge`.
- Use `.lhe.gz` and `.hepmc.gz` for compressed products. Discovery code must
  check both compressed and uncompressed LHE extensions.
- Block processing inputs use `BLOCK:<pool>:<group_id>:<idx>`.
- Block output IDs must include both source-job and block indices, e.g.
  `JOB000123_BLOCK000045`, to avoid collisions.

## Storage and XRootD Rules

- Do not infer or probe alternate storage layouts in worker jobs. Use exact
  configured paths from `common/node_config_defaults.json`.
- IHEP full XRootD URLs use the explicit endpoint and triple slash before the
  LFN:

  ```text
  root://cceos.ihep.ac.cn:1094///store/...
  ```

  Do not normalize this to `:1094//store/...` or `:1094/store/...`.
- `xrdfs` listings may use the endpoint plus a plain path, e.g.
  `xrdfs root://cceos.ihep.ac.cn:1094/ ls /store/...`.
- Restricted Codex sandboxes can make `xrdfs`/`xrdcp` fail with
  `[FATAL] Invalid address`; reproduce XRootD issues on a normal CERN/IHEP
  shell or approved unsandboxed command with the same proxy and executable.
- Run `cmsenv` only from a valid CMSSW project area, normally its `src`
  directory.
- Put scratch files under `/tmp/chiw/`. Keep generated DAG bundles on AFS or
  another submit-visible persistent filesystem.

## Testing Expectations

For code changes, run at minimum:

```bash
bash -n processing/run_chain.sh tests/run_all_tests.sh tests/submit_tests.sh
python3 -m py_compile dag_generator.py tools/coordinate_lhe_blocks.py tools/dag_progress.py
python3 -m unittest tests.test_dag_progress
python3 tests/test_coordinate_lhe_blocks.py
python3 tests/test_campaign_job_specs.py
```

Also run one `generate-test --dry-run` or generated-DAG inspection relevant to
the changed workflow. If touching:

- DAG staging/categories: verify emitted `CATEGORY` and `MAXJOBS` lines.
- Block SubDAGs: verify planner/coordinator configs and generated dependencies.
- MiniAOD merge: verify `processing -> miniaod_merge -> ntuple` ordering and
  provenance manifest content.
- Ntuple packaging: confirm `prepare-runtime --include-ntuple` uses the
  prebuilt CMSSW15 runtime or submodule fallback.

## Production Operations

- Before submitting a pilot or production DAG, state event count, source-file
  count, events per block, expected blocks per source, merge target, and
  subprocess coverage. In block mode, `--jobs` counts source LHE files, not
  final processing jobs.
- For known existing-LHE pilots, prefer `--skip-lhe-generation --no-scan-existing`
  with configured exact paths when running from environments where remote scans
  are unreliable.
- For small existing-LHE `generate-test` pilots, keep a positive `--max-events`
  so it auto-caps each planner via `--lhe-max-events-per-plan`; otherwise tiny
  `--lhe-events-per-block` values can split the whole full-size source file.
- Repeated inputs within one subprocess must consume distinct non-overlapping
  blocks. Deterministic shuffling must be preserved.
- Treat counted inventory events and the campaign job-spec checksum as
  authoritative. Do not infer event counts from filenames or file sizes, and
  distinguish selected-inventory capacity from the whole remote pool.
- Keep HTCondor CPU requests consistent with the CMSSW configuration actually
  loaded by workers. A 2-CPU processing request requires the relevant CMSSW
  steps to use the intended two-thread/two-stream settings; verify this in live
  worker logs.
- `report-and-truncate` may preserve a valid common mixed-event prefix while
  reporting source shortfalls. The current 0.15 unused-HepMC threshold is a
  warning and planning diagnostic, not a hard production target or halt
  condition.
- Treat DAGMan queue controls independently:
  `DAGMAN_MAX_JOBS_SUBMITTED`/`DAGMAN_MAX_JOBS_IDLE` bound queue width,
  `DAGMAN_MAX_SUBMITS_PER_INTERVAL` controls batch size, and
  `DAGMAN_SUBMIT_DELAY` adds per-node delay.
- Record the schedd and top DAGMan cluster ID together. Query progress from
  that root ID on the recorded schedd rather than using a recent time window;
  completed jobs normally leave the live `condor_q` total.
- Before removing or forcing a DAG, inspect completed nodes, active children,
  deterministic outputs, and rescue files. Prefer rescue/recovery resubmission.
- Verify operational changes in live DAGMan and worker logs; generated configs
  alone are not proof that a running process loaded the new values.

## Config and Security

- Keep storage paths centralized in `common/node_config_defaults.json`.
- Physics constants and campaign definitions live in `dag_generator.py`.
- Do not add campaign-specific logic to the upstream
  `external/TPS-Onia2MuMu/test/ConfFile_cfg.py`; use campaign-layer configs in
  `common/cmssw_configs/`.
- Do not commit proxies, tokens, Kerberos artifacts, CRAB work areas, generated
  ROOT outputs, or temporary scratch data.
- PRs should state whether the change affects DAG generation, worker runtime,
  storage interaction, ntuple packaging, or output format, and include the
  validation commands used.
