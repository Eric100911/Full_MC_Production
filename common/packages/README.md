# Package Preparation Guide
# =========================

This directory should contain the tarballs transferred to worker nodes.

## 1. Seeded HELAC package (required)

The tracked source artifact is `helac_package_cbseed_v1.tar.gz`; its published
SHA-256 is recorded in `helac_package_cbseed_v1.manifest.json`. Runtime
preparation verifies that artifact once and installs a copy under the stable
worker-facing name `helac_package.tar.gz`. Workers and `run_helac.sh` should
continue to use only that stable runtime basename.

`helac_package.tar.gz` (required at runtime)

`run_helac.sh` accepts either source tarballs or a prebuilt HELAC-Onia runtime.
When a compiled `HELAC-Onia-2.7.6/ho_cluster` is present, the worker reuses it
and normalizes known generated absolute symlinks before running.
The same package is used by `dag_generator.py generate-helac-matrix` for the
162-job J/psi+Upsilon Fock-state HELAC-only scan.

Contents:
- `HELAC-Onia-2.7.6.tar.gz`
- `hepmc2.06.11.tgz`

The versioned package is the reproducibility anchor. Create a replacement only
through `tools/repack_helac_crystalball_seed.py`, then update the versioned
artifact and manifest together; never overwrite the input package in place.

Verify the checked-in pair with:

```bash
python3 tools/repack_helac_crystalball_seed.py --check \
  --package common/packages/helac_package_cbseed_v1.tar.gz \
  --manifest common/packages/helac_package_cbseed_v1.manifest.json
```

For an unseeded source package, the historical packaging shape was:

```bash
cd <directory-containing-the-HELAC-source-tarballs>
cp sources/HELAC-Onia-2.7.6.tar.gz .
cp sources/hepmc2.06.11.tgz .
tar -czf helac_package.tar.gz HELAC-Onia-2.7.6.tar.gz hepmc2.06.11.tgz
cp helac_package.tar.gz <Full_MC_Production>/common/packages/
```

## 2. TPS-Onia2MuMu source

The ntuple stage now uses one shared CMSSW 15 package for both `JJP` and `JUP`:
- package path inside CMSSW: `src/HeavyFlavorAnalysis/TPS-Onia2MuMu`
- runtime configs are repo-owned under `common/cmssw_configs/`
- `JJP -> common/cmssw_configs/ntuple_jjp_cfg.py`
- `JUP -> common/cmssw_configs/ntuple_jup_cfg.py`
- JJP efficiency/acceptance uses the unified
  `common/cmssw_configs/ntuple_jjp_cfg.py`; `analysisMode` controls analyzer
  behavior and `--efficiency-ntuple` controls manifest creation.

The wrapper passes only changing runtime values to `cmsRun`
(`inputFiles`, `outputFile`, `runOnMC`, `maxEvents`). Persistent analyzer choices
such as `analysisMode`, `DoMonteCarloTree`, and
`RequireAcceptedCandidatesForMonteCarloTree` belong in the CMSSW config files.
The current general-purpose default is
`RequireAcceptedCandidatesForMonteCarloTree=False`.

The maintained source now lives in this repo as a git submodule:
- submodule path: `external/TPS-Onia2MuMu`
- upstream URL: `git@github.com:Eric100911/TPS-Onia2MuMu.git`
- current pinned gitlink should be treated as the package baseline

Initialize or refresh it with:

```bash
git submodule update --init --recursive
```

For the legacy generator, when no prebuilt CMSSW15 runtime is available, `dag_generator.py generate
--enable-ntuple`, `generate-test --enable-ntuple`, and `prepare-runtime
--include-ntuple` build `tpsonia2mumu_code.tar.gz` from the submodule
automatically and insert it into the ntuple runtime bundle. No manual copy into
`common/packages/` is needed.

For `hepthu` local-storage DAGs the ntuple payload may be inserted into the
processing runtime bundle so the ntuple can run inline with the local MiniAOD.
For lxplus/T2 DAGs it is kept in a separate ntuple runtime bundle.

## 3. `cmssw15_tpsonia2mumu_runtime.tar.gz`

For new IHEP batches, `mcprod runtime prepare --site ihep` reuses a validated
prebuilt CMSSW 15 tarball from this directory when available. Otherwise it
creates a fresh `CMSSW_15_0_15` project in EL9, installs the pinned
TPS-Onia2MuMu source, runs the full `scram b -j 4` build from `src/`, and
packages the project as part of the release. The batch worker unpacks the
compiled project and runs `scram build ProjectRename` before loading its
environment and running `cmsRun`.

The mcprod release validator checks the source tree, the compiled
TPS-Onia2MuMu library, and the corresponding `MultiLepPAT` entry in
`CMSSW_15_0_15/lib/el9_amd64_gcc12/.edmplugincache`.

The legacy generator accepts the same project tarball via
`--cmssw15-runtime-tarball`.

## Verification

```bash
cd /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/common/packages
tar -tzf helac_package.tar.gz | head -20
ls -lh *.tar.gz
```

Checks:
- the IHEP runtime release should expose `cmssw15_tpsonia2mumu_runtime.tar.gz` as the `cmssw15-ntuple-el9` bundle
- prebuilt CMSSW15 worker relocation target is `scram build ProjectRename`
- ntuple tests on this branch should be run in `CMSSW_15_0_15`

## Notes

1. `helac_package.tar.gz` remains the only hard dependency for LHE-only and MiniAOD-only smoke tests.
2. The submodule becomes required for `--enable-ntuple` only when no prebuilt CMSSW15 runtime is provided.
3. Keep the package small by excluding `.git`, CRAB work areas, ROOT outputs, and local caches.
