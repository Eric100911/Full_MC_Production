# MC Production unified workflow

This repository generates HTCondor DAGMan production workflows for heavy-flavor MC, covering the full chain:

`LHE(HELAC-Onia) → Pythia8 shower → HepMC mixing → CMSSW GEN-SIM → RAW → RECO → MiniAOD → Ntuple`

The `lxplus_t2_ihep` profile submits HTCondor/DAGMan jobs from CERN lxplus and writes LHE/output to IHEP T2 via XRootD. Other machine environments support hepthu, local Condor, and IHEP/lxlogin HepJob submission.

Two analysis types are supported: **JJP** (`J/psi + J/psi + phi`) and **JUP** (`J/psi + Upsilon + phi`). TPS campaigns (three-J/psi) are also supported for ntuple-only re-processing.

## Current acceptance criteria

- Code interface retains full chain capability including the Ntuple step.
- Small-batch HTCondor testing defaults to completing through MiniAOD with remote stage-out.
- The Ntuple step is retained in the interface; to run it, ensure the `external/TPS-Onia2MuMu` submodule is initialized and its gitlink points to the desired analysis code state.
- Ntuple input files, output files, and `maxEvents` are switched through `cmsRun` CLI. Analysis behavior (`analysisMode`, MC truth tree, acceptance gating) is fixed in `common/cmssw_configs/ntuple_*.py`.
- All programs, files, and certificates are bundled and uploaded to workers — worker nodes never read from AFS business directories at runtime.
- Worker startup copies the bundled proxy to `/tmp/x509up_u$UID`; subsequent programs do not reference local files in the unpack directory.
- Use `dag_generator.py --machine-env ...` to select the submit/storage profile. There are no longer separate branches for `VtxSmeared`, `ihep`, or `hepthu`.
- `pool_jpsi_CSCO_g` and `pool_upsilon_CSCO_g` use HELAC-Onia's CrystalBall pT model.

## Directory structure

- **`dag_generator.py`** — Main CLI entry point. Defines `LHEPool`, `Campaign`, and `MachineEnv` dataclasses plus all subcommands (`list`, `validate`, `generate`, `generate-test`, `generate-helac-matrix`, `generate-ntuple-only`, `prepare-runtime`). Campaign/pool definitions are Python literals in this file.
- **`hepjob_workflow.py`** — IHEP/lxlogin HepJob backend adapter. Generates bash job scripts instead of HTCondor submit files.
- **`common/node_config_defaults.json`** — Centralized storage and processing configuration, including EOS/XRootD roots, `LHE_pool` mappings, and premix/runtime defaults.
- **`common/octet_pdg.py`** — HELAC octet PDG encoding converter/scan tool.
- **`common/compression_util.py`** / **`common/compression_helpers.sh`** — Python and bash gzip helpers for transparent `.lhe.gz` handling.
- **`common/cmssw_configs/`** — Python CMSSW configuration fragments for GEN-SIM and per-analysis-type ntuples.
- **`common/paths.sh`** — Workspace-relative path definitions; no hardcoded usernames.
- **`common/packages/`** — Pre-built tarballs: `helac_package.tar.gz` (required), `cmssw15_tpsonia2mumu_runtime.tar.gz` (optional).
- **`external/TPS-Onia2MuMu`** — Git submodule for the ntuple analyzer (v2.0_patch2). Used as fallback when the prebuilt CMSSW15 runtime tarball is unavailable.
- **`lhe_generation/run_helac.sh`** — Worker-side HELAC-Onia execution script. Supports compressed output, shuffle-split, block staging, and `TARGET_EOS_BASE` override.
- **`lhe_generation/lhe_shuffle_split.cc`** — C++14 stratified LHE shuffle and fixed-size block splitter. Pre-compiled inside `cmssw/el7` container.
- **`lhe_generation/condor_wrappers/run_lhe_gen.sh`** — LHE job wrapper using JSON config (3 positional args) instead of legacy positional args.
- **`tools/plan_lhe_blocks.py`** — Per-pool LHE block planner: compresses, shuffle-splits, stages blocks, writes `plan_manifest_<pool>_<seed>.json`. Runs as a Condor job after HELAC generation.
- **`tools/coordinate_lhe_blocks.py`** — Multi-source campaign coordinator: reads per-pool plan manifests, applies strict-min block matching, generates `blocks_processing.dag` SubDAG.
- **`tools/compress_existing_lhe.py`** — Backfill utility to compress existing uncompressed LHE pools.
- **`tools/transfer_compress_lhe.py`** — Condor worker script for batch LHE compression with XRootD transfer.
- **`processing/run_chain.sh`** — Worker-side processing chain: shower → mix → CMSSW steps → optional ntuple → stage-out. Recompiles Pythia shower tools on the worker.
- **`processing/condor_wrappers/`** — Lightweight bash wrappers invoked by submit templates (`run_processing.sh`, `run_ntuple_only.sh`, `run_plan_lhe_blocks.sh`, `run_coordinate_lhe_blocks.sh`). Wrappers take only bootstrap bundle/config arguments; node settings are read from JSON.
- **`processing/pythia_shower/`** — C++ Pythia8+HepMC3 shower tools (`shower_normal.cc`, `shower_phi.cc`, `shower_sps.cc`, `event_mixer_multisource.cc`) with a Makefile.
- **`processing/templates/`** — HTCondor submit description files (`.sub`) per machine environment and DAG node type (including `plan_lhe_blocks.sub`, `coordinate_lhe_blocks.sub`, `compress.sub`, `transfer_compress.sub`).
- **`tests/`** — Shell-based test harness: `run_all_tests.sh` (main entry), `mock_test_worker.sh` (local worker-bundle/config mock), `submit_tests.sh` (per-campaign smoke DAGs), `submit_lhe_matrix.sh` (LHE pool matrix), `test_lhe_shuffle_split.sh` (shuffle-split unit tests), `test_octet_pdg_tool.sh` (PDG mapping self-check), plus `generate_synthetic_lhe.py` and `check_y_symmetry.py`.
- **`docs/`** — Design notes, investigation reports, and review documents.

## Environment setup

### 1. Proxy

```bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
./check_proxy.sh --init
./check_proxy.sh --status
```

DAGMan on lxplus uses a persistent proxy copy on AFS so that `condor_dagman` direct-submit works without access to the submit host's `/tmp`:

```bash
export X509_USER_PROXY=/afs/cern.ch/user/c/chiw/x509up_u$(id -u)
```

### 2. Required packages

Hard dependency:

- `common/packages/helac_package.tar.gz` — HELAC-Onia and HepMC sources for LHE generation.

Optional but recommended:

- `external/TPS-Onia2MuMu` git submodule — provides the ntuple analyzer source. Missing submodule produces a warning during validation; only required when ntuple is enabled.

```bash
git submodule update --init --recursive
```

- `common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz` — prebuilt CMSSW15 runtime (preferred over source-package fallback for ntuple).

## Main CLI usage

All commands share `--machine-env` to select the submit/storage profile.

### Machine environments

| Name | Backend | Storage |
|------|---------|---------|
| `lxplus_t2_ihep` (alias: `t2_cn_beijing`) | HTCondor DAGMan on CERN lxplus | IHEP T2 via XRootD |
| `hepthu` | HTCondor DAGMan on hepthu | Local filesystem |
| `local_condor` | Local HTCondor | Local filesystem |
| `ihep` | HepJob on IHEP/lxlogin | IHEP T2 via XRootD |

`lxplus_t2_ihep` splits MiniAOD and ntuple into separate DAG nodes; `hepthu` keeps ntuple inline to avoid cross-node local file access.

### Listing available configurations

```bash
python3 dag_generator.py list --kind all
python3 dag_generator.py list --kind campaigns
python3 dag_generator.py list --kind pools
```

### Validating the environment

```bash
python3 dag_generator.py validate --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS --scan-existing
python3 dag_generator.py validate --machine-env hepthu --campaign JUP_DPS1 --scan-existing
```

With strict analysis package check:

```bash
python3 dag_generator.py validate --campaign JJP_DPS1 --strict-analysis-packages
```

### Generating a production DAG

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS1 \
  --jobs 20 \
  --output-dir generated/jjp_dps1 \
  --output jjp_dps1.dag \
  --max-events -1
```

### Generating a smoke test DAG

```bash
# Minimal smoke test (1 job, 5 events, ntuple disabled)
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS2_CS \
  --output-dir tests/generated/smoke \
  --output smoke.dag

# With ntuple and efficiency manifest
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS1 \
  --jobs 1 --max-events 5 --enable-ntuple --efficiency-ntuple \
  --output-dir tests/generated/jjp_efficiency_smoke --output mc_test.dag

# Multi-campaign smoke test
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JUP_DPS1 \
  --jobs 1 --max-events 5 \
  --output-dir tests/generated/manual_test \
  --output mc_test.dag
```

### Generating a HELAC Fock-state matrix DAG

```bash
python3 dag_generator.py generate-helac-matrix \
  --output-dir generated/helac_matrix \
  --output helac_matrix.dag \
  --stageout-dir helac_matrix/jpsi_upsilon_fock_scan \
  --seed-base 92000 \
  --maxjobs-lhe 20
```

This runs only HELAC-Onia (no downstream shower/CMSSW). It generates 162 jobs covering 9 `cc~` Fock states, 9 `bb~` Fock states, and both born / `+ g` processes. Output tarballs are staged under the target EOS base.

### Preparing worker runtime bundles

```bash
python3 dag_generator.py prepare-runtime \
  --machine-env lxplus_t2_ihep \
  --output-dir tests/generated/runtime_bundle_check
```

Generates: `lhe_runtime_bundle.tar.gz`, `processing_runtime_bundle.tar.gz`, `summary_runtime_bundle.tar.gz`, `proxy_bundle.tar.gz`. Submit-mode bundles must be written to AFS workspace, not `/tmp`.

---

## Condor node configuration

Generated HTCondor nodes use JSON config files instead of long positional shell argument lists. For LHE generation, planning, coordination, processing, and ntuple nodes, the submit template passes only:

```text
$(proxy_bundle_name) $(runtime_bundle_name) $(config_name)
```

The matching `$(config_path)` is transferred with the runtime and proxy bundles. DAG `VARS` lines keep only stable names/paths used by the submit templates, such as bundle names, log roots, resource requests, wrapper path, and `config_path`/`config_name`.

Generated configs are written under the DAG output tree:

| Node type | Config directory |
|-----------|------------------|
| LHE generation | `node_configs/lhe_generation/LHE_*.json` |
| LHE planning | `node_configs/planning/PLAN_*.json` |
| LHE coordination | `node_configs/coordination/COORD_*.json` |
| Direct processing | `node_configs/processing/PROC_*.json` |
| Direct ntuple | `node_configs/ntuple/NTUPLE_*.json` |
| Block processing SubDAG | `plan_subdags/<campaign>/job_<N>/node_configs/processing/MIX_*.json` |
| Block ntuple SubDAG | `plan_subdags/<campaign>/job_<N>/node_configs/ntuple/NTUPLE_*.json` |

Storage defaults come from `common/node_config_defaults.json`. Raw/generated LHE pool files use the configured `xrootd_store_user_base` plus `LHE_pool/<mapped-subdir>`, for example:

```text
root://cceos.ihep.ac.cn///store/user/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO/sample_pool_2jpsi_cs_60100.lhe.gz
```

Processing output still uses `target_eos_base/output/<campaign>/<job_id>/...` unless overridden by config.

---

### LHE compression

LHE files may be stored compressed (`.lhe.gz`) or uncompressed (`.lhe`). Pool scanning tries `.lhe.gz` first then falls back to `.lhe`. HepMC intermediates always remain plain text for CMSSW compatibility.

```bash
# Generate with compressed LHE output (default gzip level 1)
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag \
  --compress-lhe --lhe-compression-level 3

# Backfill compress an existing LHE pool
python3 tools/compress_existing_lhe.py --pool-dir /path/to/lhe_pool --dry-run
python3 tools/compress_existing_lhe.py --pool-dir /path/to/lhe_pool --keep --level 3
```

### LHE shuffle-split

The `--lhe-shuffle-split` flag enables stratified shuffle-and-split of LHE output into fixed-size blocks. The original single LHE is always preserved for backward-compatible processing.

```bash
# Generate with 1000-event blocks
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --jobs 20 --output-dir generated/jjp_dps1 --output jjp_dps1.dag \
  --lhe-shuffle-split --lhe-events-per-block 1000

# Smoke test with shuffle-split
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --output-dir tests/generated/shuffle_smoke --output smoke.dag \
  --lhe-shuffle-split --lhe-events-per-block 100
```

### Block SubDAG workflow

The `--enable-lhe-block-subdags` flag enables a two-stage block-level workflow:

1. **Planner** (`tools/plan_lhe_blocks.py`) — runs after each HELAC job, compresses and shuffle-splits LHE into blocks, stages them, writes a plan manifest.
2. **Coordinator** (`tools/coordinate_lhe_blocks.py`) — for multi-source campaigns, reads all per-pool plan manifests, matches blocks with strict-min policy, generates a `blocks_processing.dag` SubDAG with `MIX_BLOCK` processing nodes.

Block files are named `block_<seed>_<NNNNNN>.lhe.gz` for cross-seed uniqueness.

```bash
# Block SubDAG — single-source SPS
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 10 --enable-lhe-block-subdags --no-scan-existing \
  --output-dir generated/jjp_sps_cs_subdag --output mc_sps_subdag.dag

# Block SubDAG — multi-source DPS with coordinator
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS \
  --jobs 10 --enable-lhe-block-subdags --no-scan-existing \
  --output-dir generated/jjp_dps2_subdag --output mc_dps_subdag.dag

# Legacy override — flat DAG even with block SubDAG flag
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 10 --enable-lhe-block-subdags --keep-legacy-single-processing-path \
  --output-dir generated/legacy --output mc_legacy.dag
```

### Ntuple-only re-processing from existing MiniAOD

The `generate-ntuple-only` subcommand produces a DAG that runs only the ntuple step, reading MiniAOD files from an existing production output area via XRootD.

```bash
# Discover MiniAOD files remotely, generate ntuple DAG
python3 dag_generator.py generate-ntuple-only \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_SPS_CS --campaign JJP_DPS1 \
  --miniaod-base-url root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/xcheng/MC_Production_v3/output \
  --jobs 50 --dry-run

# With subprocess-based output naming
python3 dag_generator.py generate-ntuple-only \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_SPS_CS --campaign JJP_SPS_G --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JJP_DPS1 \
  --miniaod-base-url root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/xcheng/MC_Production_v3/output \
  --jobs 50 --use-subprocess-naming \
  --output-dir generated/ntuple_from_v3_miniaod
```

### Reprocessing existing LHE

Use `--skip-lhe-generation` with `--existing-lhe-base` to redirect LHE pool scanning to a different storage area:

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --skip-lhe-generation \
  --existing-lhe-base root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/chiw/MC_Production_v3 \
  --jobs 10 --output-dir generated/reprocess --output reprocess.dag
```

### Overriding the target EOS base

The `--target-base-url` flag overrides the default EOS output base for all worker scripts:

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_DPS1 \
  --target-base-url root://cceos.ihep.ac.cn//eos/ihep/cms/store/user/chiw/MyTestArea \
  --jobs 20 --output-dir generated/custom_eos --output custom.dag
```

---

### Common options reference

| Flag | Applies to | Description |
|------|-----------|-------------|
| `--disable-ntuple` | `generate`, `generate-test` | Stop at MiniAOD |
| `--enable-ntuple` | `generate-test` | Enable ntuple in smoke test |
| `--efficiency-ntuple` | `generate`, `generate-test` | Write `ntuple_manifest.json` for efficiency tool (JJP only) |
| `--force-generate-lhe` | `generate`, `generate-test` | Skip remote LHE reuse |
| `--no-scan-existing` | `generate`, `generate-test` | Skip remote pool scan |
| `--test-mode` | `generate`, `generate-test` | Fast-test mode for HELAC |
| `--compress-lhe` | `generate`, `generate-test` | Write compressed `.lhe.gz` output |
| `--lhe-compression-level` | `generate`, `generate-test` | Gzip compression level (default: 1) |
| `--lhe-shuffle-split` | `generate`, `generate-test` | Stratified shuffle-split LHE into blocks |
| `--lhe-events-per-block` | `generate`, `generate-test` | Events per shuffle-split block (default: 1000) |
| `--enable-lhe-block-subdags` | `generate`, `generate-test` | Block SubDAG workflow with planner/coordinator |
| `--keep-legacy-single-processing-path` | `generate` | Flat DAG override even with `--enable-lhe-block-subdags` |
| `--skip-lhe-generation` | `generate`, `generate-test` | Reuse existing LHE without generation |
| `--existing-lhe-base` | `generate`, `generate-test` | Base URL for existing LHE pool scanning |
| `--target-base-url` | `generate`, `generate-test` | Override EOS output base for all workers |
| `--local-output-base` | `generate`, `generate-test` | Local LHE/output root (hepthu) |
| `--local-log-dir` | `generate`, `generate-test` | HTCondor stdout/stderr/log directory (hepthu) |
| `--use-subprocess-naming` | `generate-ntuple-only` | Subprocess-based ntuple output directory structure |

## Shower modes

Three canonical shower modes are supported:

| Mode | Description |
|------|-------------|
| `normal` | Standard Pythia8 shower, no phi enrichment |
| `phi_mpi_off` | Phi-enriched mode, MPI off, retry hadronization until target phi appears |
| `phi_mpi_on_gluon` | Phi-enriched mode, MPI on, gluon-origin phi classification |

Compatibility aliases: `phi`, `phi_mode1`, `sps` → `phi_mpi_off`; `phi_mode2` → `phi_mpi_on_gluon`. Normalization is handled by `canonical_mode()` in `dag_generator.py`.

## JJP double J/psi splitting

- `JJP_SPS_CS` and `JJP_SPS_G` produce `gg → J/psi + J/psi` born/color-singlet and `gg → J/psi + J/psi + g` sources respectively. They do not mix sources on the worker.
- `JJP_DPS2_CS` and `JJP_DPS2_G` combine `pool_2jpsi_cs`/`pool_2jpsi_g` with `pool_gg`. Output paths are separated by campaign name.
- `pool_gg` uses `minptq = 4.0`; all other real pools use `minptq = 0.0`.

## Ntuple configuration

The JJP ntuple config (`common/cmssw_configs/ntuple_jjp_cfg.py`) is a thin adaptation over the upstream TPS-Onia2MuMu reference (`external/TPS-Onia2MuMu` submodule). The former separate efficiency config has been merged — efficiency mode is now controlled by the `analysisMode` VarParsing parameter in the unified config.

The `--efficiency-ntuple` flag in `run_chain.sh` controls only whether an `ntuple_manifest.json` is written for the external `run-multileppat-efficiency` tool, not which cmsRun config is used.

When updating the submodule, diff `external/TPS-Onia2MuMu/test/ConfFile_cfg.py` against `common/cmssw_configs/ntuple_jjp_cfg.py` and re-apply campaign adjustments (`keepAllSingleObjectCandsInMC=True`, correct MC GlobalTag, relevant VarParsing defaults).

### Troubleshooting

| Symptom | Check |
|---------|-------|
| `MC_GenPart_*` arrays all empty | `inputGEN` must be `prunedGenParticles`, not `genParticles`. MiniAOD drops the `genParticles` collection. |
| Zero HLT-matched muons in efficiency | `FiltersForJpsi` must be `["hltJpsiMuonL3Filtered3p5", "hltDoubleMu43LowMassL3Filtered"]`. Old labels match no trigger objects. |

## Running tests

```bash
# Static validation + smoke DAG generation (no submit)
./tests/run_all_tests.sh

# Generate and submit to HTCondor
./tests/run_all_tests.sh --submit
./tests/run_all_tests.sh --submit --wait

# With ntuple
./tests/run_all_tests.sh \
  --enable-ntuple \
  --cmssw15-runtime-tarball common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz

# LHE pool matrix test
./tests/submit_lhe_matrix.sh --submit --wait

# LHE shuffle-split unit tests
./tests/test_lhe_shuffle_split.sh

# PDG encoding self-check
./tests/test_octet_pdg_tool.sh

# Worker-bundle/config mock through the Pythia shower stop point
./tests/mock_test_worker.sh

# Local HTCondor test
./run_local_test.sh --submit --wait
./run_local_test.sh --campaign JJP_DPS1 --jobs 2 --max-events 10 --submit --enable-ntuple
```

Default test coverage: `JJP_DPS2_CS`, `JJP_DPS2_G`, `JUP_DPS1`, plus `test_octet_pdg_tool.sh`. `mock_test_worker.sh` builds a production processing bundle, transfers a dummy proxy and JSON config into a temporary worker directory, runs `run_processing.sh`, and stops after the shower step. It validates config parsing, bundle extraction, local `.lhe.gz` decompression, CMSSW setup, Pythia startup, and non-empty `shower_*.hepmc` outputs without requiring HTCondor submission.

The LHE matrix test covers: `pool_jpsi_CSCO_g`, `pool_upsilon_CSCO_g`, `pool_gg`, `pool_2jpsi_cs`, `pool_2jpsi_g`, `pool_jpsi_upsilon_CSCO`, and auto-scans for legacy `9900xxxx` PDG encoding.

## Current known limitations

- Even with the ntuple submodule initialized, small-batch Condor validation defaults to `--disable-ntuple` to focus acceptance on MiniAOD and remote stage-out.
- `phi_mpi_on_gluon` determines phi origin from hardest-process gluon ancestry (status 21-29) in the Pythia event record. This is closer to workbook requirements than the old placeholder but should still get dedicated physics spot-checks before large-scale production.
- `condor_submit` warns that `MaxRetries` in submit templates is "unused" — this is cosmetic; retry control lives in DAGMan `RETRY` directives.
- LHE files with `<event>` or `<init>` substrings inside `<header>` blocks (e.g. `<event_info>`) are handled correctly by `lhe_shuffle_split.cc` v2.1+ but may confuse naive parsers.
- Ntuple-only DAGs (`generate-ntuple-only`) discover MiniAOD files via XRootD listing. If the remote directory structure doesn't match the expected `<campaign>/<job_id>/` pattern, discovery may miss files.
- `mock_test_worker.sh` intentionally uses `normal,normal` shower modes and `stop_at: shower`; phi-enriched `shower_sps` / `shower_phi` behavior is still covered by production/container or component tests because bare lxplus/el9 hosts may not match the bundled binary ABI.

## Typical workflow

```bash
# 1. Check proxy and environment
python3 dag_generator.py validate --machine-env lxplus_t2_ihep --campaign JJP_DPS2_CS --scan-existing

# 2. Generate smoke test DAG
python3 dag_generator.py generate-test \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS2_CS --campaign JJP_DPS2_G --campaign JUP_DPS1 \
  --output-dir tests/generated/smoke --output smoke.dag

# 3. Submit
condor_submit_dag tests/generated/smoke/smoke.dag

# 4. Monitor
condor_q

# 5. Production run with block SubDAG workflow
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep --campaign JJP_SPS_CS \
  --jobs 20 --enable-lhe-block-subdags --compress-lhe \
  --output-dir generated/production --output mc_production.dag
```

## Legacy test scripts

`tests/test_lhe_generation.sh`, `tests/test_shower_chain.sh`, `tests/test_cmssw_chain.sh`, and `tests/test_pipeline.sh` are retained for component-level debugging. The recommended submission workflow uses `dag_generator.py` with `tests/run_all_tests.sh` or `tests/submit_tests.sh`.

## Developer reference

See `CLAUDE.md` for the full architecture reference, coding conventions, and detailed invariants used by Claude Code when working in this repository. The `docs/` directory contains design notes and investigation reports.
