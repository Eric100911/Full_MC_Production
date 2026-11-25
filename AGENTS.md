# Repository Guidelines

This branch targets the `T2_CN_Beijing` workflow. Keep this file short and
durable; detailed procedures belong in `docs/testing.md` and path details in
`docs/directory_path_reference.md`.

## Project Map

- `mcprod` is the primary entry point for campaign cataloguing, runtime
  preparation, run planning, handoff, IHEP batches, aggregation, and workers.
- `src/full_mc/` contains the refactored domain, planning, lifecycle,
  scheduler, runtime, storage, and worker adapters.
- `processing/run_chain.sh`, `lhe_generation/run_helac.sh`, and
  `tools/plan_lhe_blocks.py` are native worker components.
- Exact counted LHE inventories, together with versioned specs under
  `common/campaign_job_specs/`, are authoritative for inventory-driven
  campaigns; preserve their checksums and selected source IDs.
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

## Technical language

For Chinese technical explanations, review reports, planning documents, and
agent-facing discussion, follow `docs/technical_language_style.md`.

Use Chinese as the main explanatory language. Preserve English identifiers when
they map directly to code, but explain uncommon English terms and ordinary
English words used with repository-specific/software-engineering meanings on
first use.

## Development Commands

- List campaigns: `./mcprod catalog`
- Plan a run: `./mcprod run plan --spec <campaign.json> --output-dir <work-dir>`
- Prepare a runtime: `./mcprod runtime prepare --site cern --release <release>`
- Prepare an IHEP batch: `./mcprod batch prepare --handoff <handoff.json> --output-dir <batch-dir>`
- Prepare aggregation: `./mcprod analysis aggregate --run <run-dir> --prepare`
- Offline pilot DAG: `./tests/submit_tests.sh <pilot-spec.json> <new-run-dir>`
- Default checks: `./tests/run_all_tests.sh`
- Static production regression: `PYTHONPATH=src python3 -m unittest tests.test_core_static_production`
- LHE splitter test: `./tests/test_lhe_shuffle_split.sh`

For pilot submission, monitoring, output verification, and optional MiniAOD
merge smoke tests, follow `docs/testing.md`.

Use `./mcprod` for v3 production and pilots. Keep runtime releases, run directories,
Condor logs, and operator records on submit-visible CERN AFS. The `lhe` worker
requests 1 CPU and 3072 MB RAM; keep its explicit scratch/disk ceiling separate.
For mcprod command syntax and lifecycle, consult `docs/README.md` and
`docs/testing.md` before querying `--help`. Use CLI help only to resolve an
undocumented or conflicting interface, then update the durable docs with the
verified command so the same discovery is not repeated.

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
for script in processing/run_chain.sh tests/run_all_tests.sh tests/submit_tests.sh; do
  bash -n "$script"
done
python3 -m compileall -q src/full_mc
./tests/run_all_tests.sh
```

Inspect the generated DAG for SOURCE → PLAN → PROC_SUBDAG → RECLAIM,
processing category limits, and terminal cleanup before accessing a production scheduler.

## Production Operations

- Pin a clean runtime release and keep its `git_revision` with the schedd and
  root cluster ID. For new mcprod pilots, keep `run.json`, `submission.json`,
  DAGMan logs, worker logs, and handoff records under the persistent AFS run.
- Before submitting a pilot or production DAG, state event count, source-file
  count, events per block, expected blocks per source, merge target, and
  subprocess coverage. Use a positive `planned_events` with a small counted
  input for an existing-LHE pilot. Keep runtime releases, run records, and
  Condor logs on submit-visible AFS.
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
- Live Condor status, queue/history, submission, and recovery commands require
  network access. From a restricted Codex sandbox, request approved
  unsandboxed execution on the first attempt for `myschedd`, `condor_q`,
  `condor_history`, DAG snapshot/progress, `condor_submit_dag`, or `mcprod`
  live-status/submission calls; do not first retry them inside the sandbox.
  Never treat sandbox DNS/configuration failures as an empty queue. Local log
  inspection and offline DAG checks may stay sandboxed.
- For `condor_history`, always pass the recorded schedd with `-name` and use
  exact `cluster.proc` IDs or a narrowly bounded `-constraint`. Keep both the
  returned rows (`-limit`) and scanned ads (`-scanlimit`) bounded; never use a
  broad owner/time-window query when exact job IDs are available.
- Do not use direct `ssh` to a Condor schedd for job or worker diagnosis. The
  schedd is the control-plane service: use schedd-qualified Condor queries,
  DAGMan logs, and persisted worker logs. For a running vanilla/container
  worker job only, `condor_ssh_to_job -name <schedd> <cluster.proc>` is the
  supported execute-node access path when the site enables it; this is
  Condor-protocol access to the worker, not an SSH login to the schedd.
- Before removing or forcing a DAG, inspect completed nodes, active children,
  deterministic outputs, and rescue files. Prefer rescue/recovery resubmission.
- Verify operational changes in live DAGMan and worker logs; generated configs
  alone are not proof that a running process loaded the new values.

## IHEP HepJob Submission

- `hep_sub` needs `export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH`.
- ntuple runs CMSSW_15 (cmssw-el9): pin round-robin to the 11 AVX2-safe nodes
  (ams017/018/019/020 + cws047/048/049/051/052/053/054) with `-wn`; cws050/cws055
  are the only bad nodes (rc=132). merge (cmssw-el8) runs unpinned.
- Install the proxy to the job-private `${PWD}/credentials/x509_user_proxy`, never
  the shared `/tmp/x509up_u$UID` (concurrent same-UID jobs collide).
- Serialize IHEP batch builds; `hep_q`/`xrdfs` empty results under fork exhaustion
  need a retry before reading as zero.
- Full lessons:
  `skills/cms-continuous-production/references/hepjob-submission.md`.

## Config and Security

- Keep storage paths centralized in `common/node_config_defaults.json`.
- Campaign definitions and profiles live under `common/campaigns/` and
  `common/profiles/`. Keep installed resource copies synchronized.
- Do not add campaign-specific logic to the upstream
  `external/TPS-Onia2MuMu/test/ConfFile_cfg.py`; use campaign-layer configs in
  `common/cmssw_configs/`.
- Do not commit proxies, tokens, Kerberos artifacts, CRAB work areas, generated
  ROOT outputs, or temporary scratch data.
- PRs should state whether the change affects DAG generation, worker runtime,
  storage interaction, ntuple packaging, or output format, and include the
  validation commands used.
