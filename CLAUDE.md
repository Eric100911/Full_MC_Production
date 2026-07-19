# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Companion file: `AGENTS.md` — short, durable reference for storage rules, XRootD
URL conventions, testing expectations, branch scope, and PR descriptions.
Detailed testing procedures live in `docs/testing.md`; path details in
`docs/directory_path_reference.md`.

## Plan Mode

When entering plan mode, make comprehensive plans based on careful code reading of all relevant files. Plans should include relevant code snippets with line number references to existing functions, classes, and patterns that should be reused. Do not propose new implementations when suitable ones already exist.

## Prepare for Commit

Use the `/prepare-for-commit` skill. It stages all changes, writes a multi-line gitmoji commit message, and presents it for review — without committing.

## Project Architecture

This is an HTCondor DAGMan-based MC production workflow for heavy-flavor physics, executing the chain:

`LHE(HELAC-Onia) → Pythia8 shower → HepMC mixing → CMSSW GEN-SIM → RAW → RECO → MiniAOD → Ntuple`

Two analysis types are supported: **JJP** (`J/psi + J/psi + phi`) and **JUP** (`J/psi + Upsilon + phi`).

### Core modules

- **`dag_generator.py`** — Main CLI entry point. Defines `LHEPool`, `Campaign`, `MachineEnv` dataclasses and all subcommands (`list`, `validate`, `generate`, `generate-test`, `generate-helac-matrix`, `prepare-runtime`). Campaign/pool definitions are Python literals in this file, not loaded from external config.
- **`hepjob_workflow.py`** — IHEP/lxlogin HepJob backend adapter. Reuses campaign/pool definitions and bundle-building utilities from `dag_generator.py`. Generates bash job scripts instead of HTCondor submit files.
- **`common/octet_pdg.py`** — HELAC octet PDG encoding converter: translates between old `9900xxxx` codes and Pythia8 `99nqnsnrnLnJ` encoding. Also provides a `scan` subcommand for auditing LHE files.
- **`lhe_generation/run_helac.sh`** — Worker-side HELAC-Onia execution script. Unpacks `helac_package.tar.gz`, builds HepMC/HELAC, generates LHE, optionally shuffle-splits into blocks, stages out to XRootD.
- **`lhe_generation/lhe_shuffle_split.cc`** — C++14 tool for stratified LHE shuffle and 1000-event block splitting. Supports `--filename-prefix` for seed-specific output naming. Pre-compiled inside cmssw/el7 container and bundled with both LHE and planner runtimes.
- **`tools/plan_lhe_blocks.py`** — Per-pool LHE block planner: compresses, shuffle-splits, stages blocks, writes `plan_manifest_<pool>_<seed>.json`. Runs as a Condor job after HELAC generation.
- **`tools/coordinate_lhe_blocks.py`** — Multi-source campaign coordinator: reads per-pool plan manifests, applies strict-min block matching, generates `blocks_processing.dag` SubDAG with `MIX_BLOCK` processing nodes.
- **`tools/compile_node_config.py`** — Config compiler/validator for fully expanded per-pool LHE paths. Validation happens before submission, never through worker-side layout guessing.
- **`processing/run_chain.sh`** — Worker-side processing chain: shower → mix → CMSSW steps → optional ntuple → stage-out. Recompiles Pythia shower tools on the worker to avoid glibc/ABI mismatches.
- **`processing/pythia_shower/`** — C++ Pythia8+HepMC3 shower tools (`shower_normal.cc`, `shower_phi.cc`, `event_mixer_multisource.cc`) with a Makefile.
- **`processing/templates/`** — HTCondor submit description files (`.sub`) per machine environment and DAG node type. Templates use wrapper scripts rather than inline bash.
- **`processing/condor_wrappers/`** — Lightweight bash wrappers (`run_processing.sh`, `run_ntuple_only.sh`) invoked by submit templates.
- **`common/compression_util.py`** — Python gzip helpers: `accepts_lhe_ext()`, `gzip_file_atomic()`, `gunzip_file_atomic()`.
- **`common/compression_helpers.sh`** — Bash equivalents: `is_gz_file()`, `decompress_if_needed()`, `accepts_lhe_ext()`. Source from worker scripts.
- **`common/cmssw_configs/`** — Python CMSSW configuration fragments for GEN-SIM and per-analysis-type ntuple configs. The JJP ntuple config is a thin adaptation of the upstream reference; see **Ntuple Config** below for sync procedure and troubleshooting.
- **`external/TPS-Onia2MuMu`** — Git submodule: the ntuple analyzer source (v2.0). Used as fallback when a prebuilt CMSSW15 runtime tarball is not available.
- **`common/paths.sh`** — Centralized workspace-relative path definitions (proxy resolution, temp dir, log dir). Source from shell scripts that need user-local paths; no hardcoded usernames.
- **`common/packages/`** — Pre-built tarballs: `helac_package.tar.gz` (required), `cmssw15_tpsonia2mumu_runtime.tar.gz` (optional, preferred for ntuple).
- **`tests/`** — Shell-based test harness: `run_all_tests.sh` (main entry), `submit_tests.sh` (per-campaign smoke DAGs), `submit_lhe_matrix.sh` (LHE pool matrix), `test_octet_pdg_tool.sh` (PDG mapping self-check), plus legacy component-level test scripts.
- **`tools/`** — Utilities: `check_gensim_vtxsmeared_config.py`, `summarize_helac_finished.py`, `compress_existing_lhe.py` (backfill LHE compression), `run_compress_lhe.sh` (batch compression worker wrapper).

### Machine environments

Selected via `--machine-env` on every `dag_generator.py` command. Defined in `MACHINE_ENVS` OrderedDict (dag_generator.py:123):

| Name | Backend | Storage |
|------|---------|---------|
| `lxplus_t2_ihep` (alias: `t2_cn_beijing`) | HTCondor DAGMan on CERN lxplus | IHEP T2 via XRootD |
| `hepthu` | HTCondor DAGMan on hepthu | Local filesystem |
| `local_condor` | Local HTCondor | Local filesystem |
| `ihep` | HepJob on IHEP/lxlogin | IHEP T2 via XRootD |

`lxplus_t2_ihep` splits MiniAOD and ntuple into separate DAG nodes; `hepthu` keeps ntuple inline to avoid cross-node local file access.

### Key invariants

- Worker nodes never read from AFS business directories at runtime — everything arrives via tarball bundles.
- Shower mode names are normalized through `canonical_mode()` (dag_generator.py:247): aliases like `phi`, `sps`, `phi_mode1` all map to `phi_mpi_off`.
- The `--efficiency-ntuple` flag only works for JJP campaigns and writes an `ntuple_manifest.json` consumable by the external `run-multileppat-efficiency` tool.
- Pool scan results are cached via the `DAG_GENERATOR_POOL_SCAN_CACHE` environment variable.
- Proxy handling: worker startup copies the bundled proxy to `/tmp/x509up_u$UID`; DAGMan on lxplus uses a persistent proxy copy on AFS. Proxy resolution (`detect_proxy_path`) uses `$X509_USER_PROXY` → `voms-proxy-info --path`; /tmp proxies trigger a warning (Condor workers cannot access them).
- Ntuple output directory structure for `--use-subprocess-naming`: `JpsiJpsiPhi/Ntuple/{subprocess_id}/{subprocess_id}-Ntuple-{version}-{job_id}.root` under the target EOS base.
- CMSSW15 runtime tarball: built with `scram b clean && scram b -j 8` (full project rebuild) to ensure `.edmplugincache` is regenerated. Validation checks for `pluginHeavyFlavorAnalysisTPS-Onia2MuMu.so` and `.edmplugincache`.
- LHE files may be stored compressed (`.lhe.gz`) or uncompressed (`.lhe`). Pool scanning, listing, and resolution try `.lhe.gz` first then fall back to `.lhe`. New LHE output defaults to `.lhe.gz` when `--compress-lhe` is set. HepMC outputs may be compressed (`.hepmc.gz`). HepMC intermediates passed to CMSSW always remain plain text.
- LHE shuffle-split (`--lhe-shuffle-split`) produces `block_NNNNNN.lhe` files and a `shuffle_split_manifest.json` in a `lhe_blocks/` subdirectory. The original single LHE is always preserved for backward-compatible processing.
- Block SubDAG mode (`--enable-lhe-block-subdags`) introduces per-HELAC-job planners and campaign-level coordinators that generate `SUBDAG EXTERNAL` processing DAGs. Block files are named `block_<seed>_<NNNNNN>.lhe.gz` for cross-seed uniqueness. Processing nodes consume blocks via `BLOCK:<pool>:<seed>:<idx>` input specs.
- Planner: `tools/plan_lhe_blocks.py` runs after each HELAC job, compresses LHE, shuffle-splits, stages blocks, and writes `plan_manifest_<pool>_<seed>.json`.
- Coordinator: `tools/coordinate_lhe_blocks.py` runs after all per-source planners for a multi-source campaign, matches blocks with strict-min policy, and generates a `blocks_processing.dag` SubDAG with `MIX_BLOCK` processing nodes.
- New DAG categories: `lhe_planning` (planner jobs), `lhe_coordination` (coordinator jobs), `block_processing` (block-level processing inside SubDAGs).
- The `--filename-prefix` option on `lhe_shuffle_split` allows seed-specific block filenames (e.g. `100_block_000000.lhe`).
- Storage configuration is centralized in `common/node_config_defaults.json` (EOS host, path base, pool subdirectory mappings). Runtime scripts read this via the JSON config pattern (`write_node_config()` in dag_generator.py). The constants `EOS_HOST`, `EOS_PATH_BASE`, `EOS_BASE`, `CHIW_EOS_OUTPUT_BASE` in `dag_generator.py` derive from this file.
- Existing LHE pools use exact `lhe_pool_directories.<pool>.path` values with the explicit IHEP `:1094` endpoint. DAG generation copies the mapping into every relevant node config. `EOS:<pool>:...` resolution lists only that exact directory and fails if it is missing or empty.
- `TARGET_EOS_BASE` environment variable overrides the default EOS base in all worker scripts (`run_chain.sh`, `run_helac.sh`). Set via `--target-base-url` in dag_generator.py CLI, which flows through submit template VARS → wrapper script → environment.
- `--existing-lhe-base` is an explicit override. It appends only the pool storage name to the supplied base; it does not trigger legacy-layout probing.
- Helmholtz wrapper scripts use `set -euo pipefail`; LHE wrapper uses a JSON config file (3 positional args: proxy bundle, lhe bundle, config JSON) read by an inline Python script.
- The `ntuple_jjp_efficiency_cfg.py` was merged into `ntuple_jjp_cfg.py`. Efficiency mode is now controlled by the `analysisMode` parameter in the unified config, not a separate config file. `keepAllSingleObjectCandsInMC` defaults to `True`. The `--efficiency-ntuple` flag controls manifest JSON file creation, not config selection.

## Production Operations

- Do not synthesize, guess, or probe pool-directory variants in worker jobs.
  Compile exact paths into `common/node_config_defaults.json`, validate them
  before submission, and make runtime resolution fail fast on missing or
  ambiguous paths.
- Use IHEP XRootD URLs with the explicit endpoint and triple slash before the
  LFN: `root://cceos.ihep.ac.cn:1094///store/...`. Do not normalize this to
  two slashes or drop the endpoint port. `xrdfs` listings may use the endpoint
  plus a plain path: `xrdfs root://cceos.ihep.ac.cn:1094/ ls /store/...`.
  Before attributing `No route to host` or listing failures to the endpoint,
  check the X509 proxy and reproduce with the same `xrdfs`/`xrdcp` binary and
  environment. Restricted sandboxes can cause `[FATAL] Invalid address` errors;
  reproduce XRootD issues on a normal CERN/IHEP shell with the same proxy.
- `cmsenv` is valid only inside an initialized CMSSW project, normally from its
  `src` directory.
- Before every pilot or production submission, report the configured number of
  events, source LHE files, events per block, expected blocks per source,
  merge target, and included subprocesses. In block SubDAG mode, `--jobs`
  counts source LHE files, while planner manifests determine the number of
  processing blocks.
- Reuse of a source block across different subprocess classes is acceptable.
  Repeated inputs within one subprocess must consume distinct, non-overlapping
  blocks. Keep shuffling deterministic and use
  `JOB<source-index>_BLOCK<block-index>` output IDs to avoid collisions.
- Full JJP production must explicitly include TPS through `JJP_ALL`. The
  `JpsiJpsiPhi_MC_Production_v4` MiniAOD and ntuple products belong under
  `/store/user/chiw/JpsiJpsiPhi_MC_Production_v4/`.
- DAGMan throttles have separate meanings:
  `DAGMAN_MAX_JOBS_SUBMITTED`/`DAGMAN_MAX_JOBS_IDLE` bound queue width,
  `DAGMAN_MAX_SUBMITS_PER_INTERVAL` controls submission batches, and
  `DAGMAN_SUBMIT_DELAY` sleeps per node. Production uses 100 submissions per
  interval and zero per-node delay; avoid hidden throttling when ceilings are
  intentionally non-binding.
- Before removing a running DAG, inspect completed nodes, child jobs, staged
  outputs, and available rescue state. Prefer rescue/recovery resubmission so
  completed nodes remain done. Do not use `condor_submit_dag -force` unless the
  explicit goal is to discard prior state.
- Confirm configuration changes in live DAGMan or worker logs after submission.
  A generated file does not prove an already-running process loaded it.
- Keep generated DAGs, rescue files, logs, and campaign products out of Git.
  Stage only intentional source, test, and documentation changes.
- For known existing-LHE pilots from environments where remote scans are
  unreliable, prefer `--skip-lhe-generation --no-scan-existing` with configured
  exact paths.
- For small existing-LHE `generate-test` pilots, keep a positive `--max-events`
  so it auto-caps each planner via `--lhe-max-events-per-plan`; otherwise tiny
  `--lhe-events-per-block` values can split the whole full-size source file.

## Ntuple Config

The JJP ntuple config (`common/cmssw_configs/ntuple_jjp_cfg.py`) is a thin adaptation
layer over the upstream TPS-Onia2MuMu reference. The submodule at
`external/TPS-Onia2MuMu` pins the ntuple format version and defines the data contract.

The former `ntuple_jjp_efficiency_cfg.py` has been merged into `ntuple_jjp_cfg.py`.
Efficiency mode is controlled by the `analysisMode` VarParsing parameter; the
`--efficiency-ntuple` flag in `run_chain.sh` now only controls whether an
`ntuple_manifest.json` is written for the external `run-multileppat-efficiency` tool,
not which cmsRun config is used.

### Syncing with upstream

Do not add campaign-specific logic to the upstream
`external/TPS-Onia2MuMu/test/ConfFile_cfg.py`; use campaign-layer configs in
`common/cmssw_configs/` instead.

When the submodule is updated to a new tag:

1. Diff `external/TPS-Onia2MuMu/test/ConfFile_cfg.py` against
   `common/cmssw_configs/ntuple_jjp_cfg.py`.
2. Apply campaign-specific adjustments on top:
   - `keepAllSingleObjectCandsInMC` default → `True`
   - Verify the MC GlobalTag matches the MiniAOD production CMSSW version
   - Remove or adjust VarParsing defaults not relevant to this workflow

### Troubleshooting

| Symptom | Check |
|---------|-------|
| `MC_GenPart_*` arrays all empty | `X_config` → `inputGEN` must be `prunedGenParticles`, not `genParticles`. MiniAOD drops the `genParticles` collection. |
| Zero HLT-matched muons in efficiency | `X_config` → `FiltersForJpsi` must be `["hltJpsiMuonL3Filtered3p5", "hltDoubleMu43LowMassL3Filtered"]`. The old labels (`hltVertexmumuFilterJpsiMuon3p5`, `hltDisplacedmumuFilterDoubleMu43LowMass`) match no trigger objects. |

## Runtime Environments
- **Image**: `cmssw/el7:x86_64` (CentOS 7, glibc 2.17)
- **Compiler toolchain**: LCG_88b (`/cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh`) — GCC 6.2 for gfortran and C++14 builds
- **C++ standard**: C++14 (`-std=c++14`) — GCC 6.2 predates the `-std=c++17` flag
- Bundled C++ binaries (`lhe_shuffle_split`) are compiled inside this container at bundle-prep time and run with LCG_88b sourced
- Python tools run on the host system (EL9, Python 3)

### Test environment
- Local tests (`tests/test_lhe_shuffle_split.sh`) compile and run inside the same `cmssw/el7` + LCG_88b container via `singularity exec`
- Synthetic LHE generation (`tests/generate_synthetic_lhe.py`) runs on the host Python
- Mock tests (`tests/mock_test_worker.sh`) validate the infrastructure chain (prepare-runtime → bundle → wrapper → config → execution) using the same tooling and patterns as production. **Mock tests must use the same commands, bundles, and config format as production.** If the production container image is unavailable locally, bare execution is acceptable only when the host OS matches the container OS (both el9). Never substitute a different container or alter the worker scripts for test convenience.

## Common Commands

### Environment & proxy

```bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
./check_proxy.sh --init
./check_proxy.sh --status

# Git submodule (needed for ntuple source-package fallback)
git submodule update --init --recursive
```

### Listing & validation

```bash
python3 dag_generator.py list --kind all
python3 dag_generator.py validate --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS --scan-existing
python3 dag_generator.py validate --campaign JJP_DPS1 --strict-analysis-packages
```

### Generating DAGs

```bash
# Full production DAG
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag --max-events -1

# Smoke test DAG (1 job, 5 events, ntuple disabled)
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --output-dir tests/generated/smoke --output smoke.dag

# Smoke DAG with ntuple
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 1 --max-events 5 --enable-ntuple --efficiency-ntuple \
  --output-dir tests/generated/jjp_efficiency_smoke --output mc_test.dag

# Runtime bundles only (no DAG)
python3 dag_generator.py prepare-runtime \
  --machine-env lxplus_t2_ihep --output-dir tests/generated/runtime_bundle_check

# With compressed LHE output (default gzip level 1)
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag \
  --compress-lhe --lhe-compression-level 3

# Backfill compress existing LHE pool
python3 tools/compress_existing_lhe.py --pool-dir /path/to/lhe_pool --dry-run
python3 tools/compress_existing_lhe.py --pool-dir /path/to/lhe_pool --keep --level 3

# With LHE shuffle-split (1000-event blocks)
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag \
  --lhe-shuffle-split --lhe-events-per-block 1000

# Smoke test with shuffle-split
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --output-dir tests/generated/shuffle_smoke --output smoke.dag \
  --lhe-shuffle-split --lhe-events-per-block 100

# Block SubDAG mode — single-source SPS
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 10 --enable-lhe-block-subdags --no-scan-existing \
  --output-dir generated/jjp_sps_cs_subdag --output mc_sps_subdag.dag

# Block SubDAG mode — multi-source DPS with coordinator
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --jobs 10 --enable-lhe-block-subdags --no-scan-existing \
  --output-dir generated/jjp_dps2_subdag --output mc_dps_subdag.dag

# Legacy override (flat DAG even with block subdag flag)
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 10 --enable-lhe-block-subdags --keep-legacy-single-processing-path \
  --output-dir generated/legacy --output mc_legacy.dag

# Ntuple-only re-run from existing MiniAOD (XRootD remote)
python3 dag_generator.py generate-ntuple-only \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_SPS_CS --campaign JJP_DPS1 \
  --miniaod-base-url root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/output \
  --jobs 50 --dry-run

# Ntuple-only with subprocess-based output naming
python3 dag_generator.py generate-ntuple-only \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_SPS_CS --campaign JJP_SPS_G --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JJP_DPS1 \
  --miniaod-base-url root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/output \
  --jobs 50 --use-subprocess-naming \
  --output-dir generated/ntuple_from_v3_miniaod
```

### Running tests

`docs/testing.md` is the canonical procedure. In particular, record
`myschedd show` before a pilot submission, state the configured output event
count before submitting, verify stage-out with `xrdfs ...:1094`, download
products under `/tmp/chiw/`, and count the ROOT `Events` entries. `cmsenv` is
valid only from an actual CMSSW project `src` directory.

**Minimum validation before committing code changes:**

```bash
bash -n processing/run_chain.sh tests/run_all_tests.sh tests/submit_tests.sh
python3 -m py_compile dag_generator.py tools/coordinate_lhe_blocks.py
python3 tests/test_coordinate_lhe_blocks.py
```

Also run one `generate-test --dry-run` or generated-DAG inspection relevant to
the changed workflow. If touching:

- DAG staging/categories: verify emitted `CATEGORY` and `MAXJOBS` lines.
- Block SubDAGs: verify planner/coordinator configs and generated dependencies.
- MiniAOD merge: verify `processing → miniaod_merge → ntuple` ordering and
  provenance manifest content.
- Ntuple packaging: confirm `prepare-runtime --include-ntuple` uses the
  prebuilt CMSSW15 runtime or submodule fallback.

```bash
# Static validation + smoke DAG generation (no submit)
./tests/run_all_tests.sh

# Generate and submit to HTCondor
./tests/run_all_tests.sh --submit
./tests/run_all_tests.sh --submit --wait

# With ntuple
./tests/run_all_tests.sh --enable-ntuple \
  --cmssw15-runtime-tarball common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz

# LHE pool matrix test
./tests/submit_lhe_matrix.sh --submit --wait

# PDG encoding self-check
./tests/test_octet_pdg_tool.sh

# Local HTCondor test
./run_local_test.sh --submit --wait
./run_local_test.sh --campaign JJP_DPS1 --jobs 2 --max-events 10 --submit --enable-ntuple

# Syntax-check all shell scripts
bash -n processing/run_chain.sh lhe_generation/run_helac.sh \
  tests/run_all_tests.sh tests/submit_tests.sh tests/submit_lhe_matrix.sh
```

### Local shower rebuild

```bash
cd processing/pythia_shower && make -B all
```

### Submitting and monitoring

```bash
condor_submit_dag tests/generated/smoke/smoke.dag
condor_q
```

## Coding Conventions

- **Python**: PEP 8, 4-space indent, `snake_case`. Uppercase constants for site paths and fixed workflow settings.
- **Bash**: `set -euo pipefail` where practical; prefer the long-form flags already used by the repo (`--campaign`, `--enable-ntuple`, `--miniaod-input`). No inline bash in submit templates — use wrapper scripts.
- **Vocabulary**: Use canonical names throughout:
  - Pool names like `pool_jpsi_CSCO_g`
  - Shower modes like `phi_mpi_off`
  - Analysis types `JJP` and `JUP`
  - DAG categories: `lhe`, `processing`, `ntuple`, `lhe_planning`, `lhe_coordination`, `block_processing`, `miniaod_merge`
- **Product extensions**: Use `.lhe.gz` and `.hepmc.gz` for compressed products. Discovery code must handle both compressed and uncompressed LHE extensions (try `.lhe.gz` first, fall back to `.lhe`). HepMC intermediates remain plain text for CMSSW compatibility.
- **Block identifiers**: Block processing inputs use `BLOCK:<pool>:<group_id>:<idx>`. Block output IDs must include both source-job and block indices (e.g. `JOB000123_BLOCK000045`) to avoid collisions.
- **Commit messages**: Gitmoji-style with each line starting with a `:emoji_name:` token, or `feat:`/`fix:` prefixes. Keep messages imperative and specific to the workflow stage changed.
- **Security**: Never commit proxies, tokens, Kerberos artifacts, CRAB work areas, or generated ROOT outputs. Storage paths are centralized in `common/node_config_defaults.json`; physics constants and campaign definitions in `dag_generator.py`.
- **Temporary files**: Put downloads, extracted artifacts, and scratch output under `/tmp/chiw/`. Keep submit-time bundles on AFS or another submit-visible persistent filesystem.
