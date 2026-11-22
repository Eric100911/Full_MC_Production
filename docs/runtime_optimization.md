# Runtime Packaging Optimization Notes

> **Archived point-in-time notes (2026-05-09).** Several recommendations below,
> including the prebuilt CMSSW15 runtime, split ntuple nodes, configurable log
> roots, DAG categories, and native EL9 runtime-bundle preparation, have since
> been implemented. Treat the experiment details as historical evidence, not as
> current build or production instructions. Current procedures live in
> `README.md`, `docs/testing.md`, and `common/packages/README.md`.

This note tracks packaging changes worth making after the first T2_CN_Beijing
DAG/runtime review. The current workflow is correct but spends substantial
worker time rebuilding packages that can either be prebuilt once or reused from
stable AFS/CVMFS locations.

## Current Runtime Shape

- LHE jobs transfer `lhe_runtime_bundle.tar.gz`, unpack HELAC-Onia and HepMC2
  sources, build them in the worker sandbox, run HELAC-Onia, convert legacy
  octet PDG codes, and stage out LHE files.
- Processing jobs transfer `processing_runtime_bundle.tar.gz`, rebuild the
  Pythia shower/mixer tools, and run CMSSW 12 MiniAOD production. On the hepthu
  local-storage profile, ntuple can still run inline so the local MiniAOD does
  not need to move across worker nodes.
- On lxplus/T2 DAGs, ntuple-enabled workflows create separate ntuple nodes with
  `ntuple_runtime_bundle.tar.gz`. Those nodes create or unpack `CMSSW_15_0_15`,
  load TPS-Onia2MuMu, and run a repo-owned config from `common/cmssw_configs/`.

## Recommended Improvements

### Prebuild CMSSW 15 TPS-Onia2MuMu

Build the CMSSW 15 project area once in an EL9-compatible environment, package
the compiled project area, and transfer that runtime tarball for ntuple jobs.

Build-side sketch:

```bash
export SCRAM_ARCH=el9_amd64_gcc12
scramv1 project CMSSW CMSSW_15_0_15
cd CMSSW_15_0_15/src
tar -xzf /path/to/tpsonia2mumu_code.tar.gz
scram b -j 8 HeavyFlavorAnalysis/TPS-Onia2MuMu
cd ../..
tar -czf cmssw15_tpsonia2mumu_runtime.tar.gz CMSSW_15_0_15
```

Worker-side relocation sketch:

```bash
tar -xzf cmssw15_tpsonia2mumu_runtime.tar.gz -C "$WORKDIR"
cd "$WORKDIR/CMSSW_15_0_15/src"
scram build ProjectRename
eval "$(scram runtime -sh)"
cmsRun /path/to/common/cmssw_configs/ntuple_jjp_cfg.py inputFiles=... outputFile=... maxEvents=...
```

The relocation target is `ProjectRename`; use the full `scram build
ProjectRename` spelling in documentation and scripts.

### Transfer Ntuple Assets Only When Needed

Keep MiniAOD-only runs lightweight by including TPS-Onia2MuMu assets only when
`--enable-ntuple` is set. The strict package validation should remain tied to
that same flag.

### Split MiniAOD and Ntuple DAG Nodes

The lxplus/T2 workflow separates the processing node into:

- MiniAOD node: EL9-compatible CMSSW 12 / shower and reconstruction chain.
- Ntuple node: EL9 / prebuilt CMSSW 15 TPS-Onia2MuMu runtime.

The hepthu local-storage profile intentionally keeps ntuple inline for now:
there is no shared T2 MiniAOD URL for a separate ntuple node to consume. The
split remains useful on lxplus/T2 because ntuple retries can avoid rerunning
shower, GEN-SIM, RAW, RECO, and MiniAOD.

Ntuple config policy:

- input/output files and `maxEvents` remain `cmsRun` CLI arguments;
- analysis mode and MC-tree behavior are persistent config choices;
- `RequireAcceptedCandidatesForMonteCarloTree=False` is the default for both
  general ntuples and efficiency/acceptance ntuples.

### Reuse or Prebuild Shower Tools

The processing worker currently runs `make -B all` in
`processing/pythia_shower` for every job. If the ABI is fixed for the production
campaign, package prebuilt binaries and validate with `ldd`; fall back to a
worker rebuild only if validation fails.

The current shower programs use Pythia 8 with the HepMC3 plugin and the mixer
uses both HepMC3 and HepMC2. Any replacement Pythia tree must provide
`Pythia8Plugins/HepMC3.h`, not only the older HepMC2 plugin.

### Prebuild HELAC-Onia

HELAC-Onia and HepMC2 can be built once for the worker ABI and packaged as a
runtime tree. The worker should then unpack the compiled tree, write
`run_config.ho` and `input/user.inp`, run `ho_cluster`, convert the LHE file,
and stage out.

The HELAC runtime contract should include:

- `HELAC-Onia-2.7.6/ho_cluster`
- any generated build products needed by `ho_cluster`
- a stable HepMC2 runtime, either inside the tarball or via a documented AFS
  path
- the Fortran LHE converter or its source
- `py8_onia_user.inp` generation logic

### Use Stage Categories

Use DAGMan categories and per-stage throttles so LHE generation, MiniAOD
processing, and ntuple production can be controlled independently:

```dag
CATEGORY LHE_xxx lhe
CATEGORY PROC_xxx processing
CATEGORY NTUPLE_xxx ntuple
MAXJOBS lhe 20
MAXJOBS processing 50
MAXJOBS ntuple 30
```

### Make Log Roots Configurable

Hardcoded submit-template log paths should be replaced by a generated or
configured log root so the branch can move across user areas without editing
submit files.

## Local Experiment Notes

Experiments run on 2026-05-09 in `/tmp/chiw` from
`/afs/cern.ch/user/c/chiw/condor/Full_MC_Production`.

### AFS HepMC2 and Pythia 8.245

Available shared paths:

- HepMC2: `/afs/cern.ch/user/c/chiw/public/cms-utils/HepMC-2.06.11/install`
- Pythia8: `/afs/cern.ch/user/c/chiw/public/cms-utils/pythia8245`

The public Pythia tree reports version 8.245 at runtime and links successfully
with the public HepMC2 install. A small C++ test generated three Pythia events
and converted them to HepMC2 `GenEvent` objects.

Important limitation: this Pythia tree contains
`include/Pythia8Plugins/HepMC2.h` but not `include/Pythia8Plugins/HepMC3.h`.
The current `processing/pythia_shower/shower_*.cc` programs include
`Pythia8Plugins/HepMC3.h`, so this exact public Pythia tree cannot replace the
current CMSSW/CVMFS Pythia for the shower tools without either adding a HepMC3
plugin build or changing the shower output path to HepMC2.

### HELAC-Onia Relocation

Preparation reference: `Helac2Ntuple.pdf`, section 1, says to prepare
HELAC-Onia inside the EL7/LCG_88b environment:

1. enter the EL7 container (`cmssw-el7`; on this lxplus setup the noninteractive
   form is `cmssw-el7 --command-to-run <script>`);
2. source the LCG_88b CentOS7 GCC 6.2 view and add the matching Boost/GCC
   runtime paths;
3. build HepMC 2.06.11 with `--with-momentum=GEV --with-length=MM`;
4. add the HepMC install `bin`/`lib` paths to the HELAC runtime environment;
5. unpack `HELAC-Onia-2.7.6.tar.gz`;
6. set `hepmc_path = <HepMC install>` in
   `HELAC-Onia-2.7.6/input/ho_configuration.txt`;
7. comment `lhapdfobj =` in
   `HELAC-Onia-2.7.6/addon/pp_psiY_SPS/src/makefile`;
8. run `./config`;
9. smoke test with `./ho_cluster`, `generate addon 8`, `launch`.

For this branch's NPS workflow, also preserve the existing
`pp_NOnia_MPS` patching used by the historical HTCondor scripts:

- copy `src/RANDA_init.inc` into `addon/pp_NOnia_MPS/src/`;
- include `RANDA_init.inc` from `pp_NOnia_MPS.f90` when needed;
- comment `lhapdfobj` in `addon/pp_NOnia_MPS/makefile_pp_NOnia_MPS`;
- set `pythia8_path` when HELAC needs the Pythia interface.

Source tarballs found locally:

- `/afs/cern.ch/user/c/chiw/condor/HELAC-on-HTCondor/sources/HELAC-Onia-2.7.6.tar.gz`
- `/afs/cern.ch/user/c/chiw/condor/HelacOnHTCondor/packages/HELAC-Onia-2.7.6.tar.gz`
- `/afs/cern.ch/user/c/chiw/public/cms-utils/hepmc2.06.11.tgz`

The repository-local `common/packages/helac_package.tar.gz` was not present at
the time of testing, so the experiment used the source tarball directly.

Completed experiment:

1. unpack `HELAC-Onia-2.7.6.tar.gz` in one `/tmp/chiw` directory;
2. configure/build inside `cmssw-el7` against the shared AFS HepMC2 install;
3. tar the compiled `HELAC-Onia-2.7.6` tree;
4. unpack it into a second directory;
5. hide the original build tree;
6. verify the moved `ho_cluster` executable starts and can run a tiny
   `generate addon 11` job.

Result: relocation works after fixing HELAC's generated absolute symlinks. The
first tarball preserved absolute symlinks such as:

- `HELAC-Onia-2.7.6/ho_cluster -> /tmp/.../build/HELAC-Onia-2.7.6/cluster/bin/ho_cluster`
- `HELAC-Onia-2.7.6/bin/ho_cluster -> /tmp/.../build/HELAC-Onia-2.7.6/cluster/bin/ho_cluster`
- `HELAC-Onia-2.7.6/addon/pp_NOnia_MPS/bin/HO_pp_NOnia_MPS -> /tmp/.../build/HELAC-Onia-2.7.6/bin/HO_pp_NOnia_MPS`

That tarball only appeared to work while the original build directory still
existed. Replacing those with relative links made the moved copy usable:

- `ho_cluster -> cluster/bin/ho_cluster`
- `bin/ho_cluster -> ../cluster/bin/ho_cluster`
- `Helac-Onia -> bin/Helac-Onia`
- `addon/pp_NOnia_MPS/bin/HO_pp_NOnia_MPS -> ../../../bin/HO_pp_NOnia_MPS`

The fixed tarball is
`/tmp/chiw/runtime_opt_exp/helac_relocate_fixed/helac-onia-2.7.6-compiled-el7-fixed.tgz`
and is 58 MB. With the packaging tree hidden, the moved tree ran:

```text
moved_generation_status=0
/tmp/chiw/runtime_opt_exp/helac_relocate_fixed/moved/HELAC-Onia-2.7.6/PROC_HO_0/P0_addon_pp_NOnia_MPS/output/sample_pp_nonia_mps.lhe
```

Packaging implication: a production prebuilt HELAC tarball should normalize
generated absolute symlinks before archiving. Rewriting generated `HODIR` values
to the packaging path was not required for the precompiled `addon 11` smoke
test, but package validation should still scan for the build root and either
rewrite those paths at worker unpack time or prove the specific production path
does not invoke makefile rebuilds.
