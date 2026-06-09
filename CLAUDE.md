# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
- **`common/setup.sh`** — Environment setup for local debugging (CMSSW 12/15, HELAC, Pythia, XRootD). Not used by worker nodes at runtime; they self-configure via bundled tarballs.
- **`common/octet_pdg.py`** — HELAC octet PDG encoding converter: translates between old `9900xxxx` codes and Pythia8 `99nqnsnrnLnJ` encoding. Also provides a `scan` subcommand for auditing LHE files.
- **`lhe_generation/run_helac.sh`** — Worker-side HELAC-Onia execution script. Unpacks `helac_package.tar.gz`, builds HepMC/HELAC, generates LHE, optionally shuffle-splits into blocks, stages out to XRootD.
- **`lhe_generation/lhe_shuffle_split.cc`** — C++17 tool for stratified LHE shuffle and 1000-event block splitting. Built inline by `run_helac.sh`; no external dependencies.
- **`processing/run_chain.sh`** — Worker-side processing chain: shower → mix → CMSSW steps → optional ntuple → stage-out. Recompiles Pythia shower tools on the worker to avoid glibc/ABI mismatches.
- **`processing/pythia_shower/`** — C++ Pythia8+HepMC3 shower tools (`shower_normal.cc`, `shower_phi.cc`, `event_mixer_multisource.cc`) with a Makefile.
- **`processing/templates/`** — HTCondor submit description files (`.sub`) per machine environment and DAG node type. Templates use wrapper scripts rather than inline bash.
- **`processing/condor_wrappers/`** — Lightweight bash wrappers (`run_processing.sh`, `run_ntuple_only.sh`) invoked by submit templates.
- **`common/compression_util.py`** — Python gzip helpers: `accepts_lhe_ext()`, `gzip_file_atomic()`, `gunzip_file_atomic()`.
- **`common/compression_helpers.sh`** — Bash equivalents: `is_gz_file()`, `decompress_if_needed()`, `accepts_lhe_ext()`. Source from worker scripts.
- **`common/cmssw_configs/`** — Python CMSSW configuration fragments for GEN-SIM and per-analysis-type ntuple configs (JJP, JUP, JJP-efficiency).
- **`external/TPS-Onia2MuMu`** — Git submodule: the ntuple analyzer source. Used as fallback when a prebuilt CMSSW15 runtime tarball is not available.
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
- Proxy handling: worker startup copies the bundled proxy to `/tmp/x509up_u$UID`; DAGMan on lxplus uses a persistent proxy copy on AFS.
- LHE files may be stored compressed (`.lhe.gz`) or uncompressed (`.lhe`). Pool scanning, listing, and resolution try `.lhe.gz` first then fall back to `.lhe`. New LHE output defaults to `.lhe.gz` when `--compress-lhe` is set. HepMC intermediates always remain plain text for CMSSW compatibility.
- LHE shuffle-split (`--lhe-shuffle-split`) produces `block_NNNNNN.lhe` files and a `shuffle_split_manifest.json` in a `lhe_blocks/` subdirectory. The original single LHE is always preserved for backward-compatible processing.

## Runtime Environments

### Worker container (lxplus_t2_ihep)
- **Image**: `cmssw/el7:x86_64` (CentOS 7, glibc 2.17)
- **Compiler toolchain**: LCG_88b (`/cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh`) — GCC 6.2 for gfortran and C++14 builds
- **C++ standard**: C++14 (`-std=c++14`) — GCC 6.2 predates the `-std=c++17` flag
- Bundled C++ binaries (`lhe_shuffle_split`) are compiled inside this container at bundle-prep time and run with LCG_88b sourced
- Python tools run on the host system (EL9, Python 3)

### Test environment
- Local tests (`tests/test_lhe_shuffle_split.sh`) compile and run inside the same `cmssw/el7` + LCG_88b container via `singularity exec`
- Synthetic LHE generation (`tests/generate_synthetic_lhe.py`) runs on the host Python

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
```

### Running tests

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
bash -n common/setup.sh processing/run_chain.sh tests/run_all_tests.sh tests/submit_tests.sh
```

### Local shower rebuild

```bash
source common/setup.sh --cmssw12
cd processing/pythia_shower && make -B all
```

### Submitting and monitoring

```bash
condor_submit_dag tests/generated/smoke/smoke.dag
condor_q
```

## Coding Conventions

- **Python**: PEP 8, 4-space indent, `snake_case`. Uppercase constants for site paths and fixed workflow settings.
- **Bash**: `set -e`, long-form flags (`--campaign`, `--enable-ntuple`, `--miniaod-input`). No inline bash in submit templates — use wrapper scripts.
- **Vocabulary**: Use the canonical names: pool names like `pool_jpsi_CSCO_g`, shower modes like `phi_mpi_off`, DAG categories `lhe`/`processing`/`ntuple`, analysis types `JJP`/`JUP`.
- **Commit messages**: Gitmoji-style with each line starting with a `:emoji_name:` token, or `feat:`/`fix:` prefixes. Keep messages imperative and specific to the workflow stage changed.
- **Security**: Never commit proxies, tokens, Kerberos artifacts, CRAB work areas, or generated ROOT outputs. Site-specific paths should be centralized in `common/setup.sh` or `dag_generator.py` constants.
