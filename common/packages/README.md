# Package Preparation Guide
# =========================

This directory should contain the tarballs transferred to worker nodes.

## 1. `helac_package.tar.gz` (required)

`run_helac.sh` accepts either source tarballs or a prebuilt HELAC-Onia runtime.
When a compiled `HELAC-Onia-2.7.6/ho_cluster` is present, the worker reuses it
and normalizes known generated absolute symlinks before running.
The same package is used by `dag_generator.py generate-helac-matrix` for the
162-job J/psi+Upsilon Fock-state HELAC-only scan.

Contents:
- `HELAC-Onia-2.7.6.tar.gz`
- `hepmc2.06.11.tgz`

Create it with:

```bash
cd <directory-containing-the-HELAC-source-tarballs>
cp sources/HELAC-Onia-2.7.6.tar.gz .
cp sources/hepmc2.06.11.tgz .
tar -czf helac_package.tar.gz HELAC-Onia-2.7.6.tar.gz hepmc2.06.11.tgz
cp helac_package.tar.gz <Full_MC_Production>/common/packages/
```

## 2. `tpsonia2mumu_code.tar.gz` (generated automatically)

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

When no prebuilt CMSSW15 runtime is available, `dag_generator.py generate
--enable-ntuple`, `generate-test --enable-ntuple`, and `prepare-runtime
--include-ntuple` build `tpsonia2mumu_code.tar.gz` from the submodule
automatically and insert it into the ntuple runtime bundle. No manual copy into
`common/packages/` is needed.

For `hepthu` local-storage DAGs the ntuple payload may be inserted into the
processing runtime bundle so the ntuple can run inline with the local MiniAOD.
For lxplus/T2 DAGs it is kept in a separate ntuple runtime bundle.

## 3. `cmssw15_tpsonia2mumu_runtime.tar.gz` (optional, preferred)

For ntuple production, place a prebuilt CMSSW 15 project tarball here or pass it
with `--cmssw15-runtime-tarball`. It should contain `CMSSW_15_0_15/` at archive
root. Worker jobs unpack it, run `scram build ProjectRename`, and skip the
per-job `scram b HeavyFlavorAnalysis/TPS-Onia2MuMu` rebuild.

The generator validates this contract before packaging. The archive must
contain:

- `CMSSW_15_0_15/src/`
- `CMSSW_15_0_15/src/HeavyFlavorAnalysis/TPS-Onia2MuMu/`
- `CMSSW_15_0_15/src/HeavyFlavorAnalysis/TPS-Onia2MuMu/test/ConfFile_cfg.py`
- `CMSSW_15_0_15/lib/el9_amd64_gcc12/pluginHeavyFlavorAnalysisTPS-Onia2MuMu.so`
- `CMSSW_15_0_15/lib/el9_amd64_gcc12/.edmplugincache`

Build the runtime with a clean full project rebuild (`scram b clean && scram b
-j 8`) so the plugin cache is regenerated before packaging.

## Verification

```bash
cd /afs/cern.ch/user/c/chiw/condor/Full_MC_Production/common/packages
tar -tzf helac_package.tar.gz | head -20
ls -lh *.tar.gz
```

Checks:
- runtime generation should produce `tpsonia2mumu_code.tar.gz` under the selected DAG output directory
- the archive must contain `HeavyFlavorAnalysis/TPS-Onia2MuMu/`
- source-package fallback worker build target is `scram b -j 4 HeavyFlavorAnalysis/TPS-Onia2MuMu`
- prebuilt CMSSW15 worker relocation target is `scram build ProjectRename`
- ntuple tests on this branch should be run in `CMSSW_15_0_15`

## Notes

1. `helac_package.tar.gz` remains the only hard dependency for LHE-only and MiniAOD-only smoke tests.
2. The submodule becomes required for `--enable-ntuple` only when no prebuilt CMSSW15 runtime is provided.
3. Keep the package small by excluding `.git`, CRAB work areas, ROOT outputs, and local caches.
