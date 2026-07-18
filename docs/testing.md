# Test Procedures

This is the canonical test and pilot-run procedure for this branch. Historical
plans and investigation notes under `docs/` are not operational instructions.

## Test Levels

1. Static checks validate syntax, Python imports, configuration rendering, and
   DAG structure without submitting jobs.
2. Local mock checks exercise the production runtime bundle, wrapper, JSON
   config, compressed LHE normalization, and Pythia shower stop point.
3. Component checks cover PDG conversion and LHE shuffle-split behavior.
4. Pilot DAG checks run the real chain through MiniAOD and IHEP stage-out.
5. Ntuple pilots are separate and require the CMSSW15 runtime or initialized
   analyzer submodule.

All local scratch files, downloads, and extracted artifacts must be placed
under `/tmp/chiw/`. DAG bundles submitted to HTCondor must remain on AFS, not
under `/tmp`.

## Static and Local Checks

Run these before submitting a pilot:

```bash
mkdir -p /tmp/chiw
bash -n \
  processing/run_chain.sh \
  lhe_generation/run_helac.sh \
  processing/condor_wrappers/run_processing.sh \
  lhe_generation/condor_wrappers/run_lhe_gen.sh \
  processing/templates/summary.sh \
  tests/run_all_tests.sh \
  tests/submit_tests.sh \
  tests/submit_lhe_matrix.sh \
  tests/mock_test_worker.sh \
  tests/test_lhe_shuffle_split.sh
python3 -m py_compile dag_generator.py tools/compile_node_config.py
python3 tests/test_coordinate_lhe_blocks.py
python3 tests/test_lhe_planner_cap_generation.py
./tests/run_all_tests.sh
./tests/mock_test_worker.sh
./tests/test_lhe_shuffle_split.sh
```

`run_all_tests.sh` runs the octet-PDG self-check, GEN-SIM vertex-smearing
check, environment validation, and smoke DAG generation for
`JJP_DPS2_CS`, `JJP_DPS2_G`, and `JUP_DPS1`. It does not submit unless
`--submit` is supplied.

The MiniAOD merge worker has a separate CMSSW/XRootD smoke test because it
requires CVMFS, a valid proxy, IHEP EOS read access, and real MiniAOD inputs:

```bash
./tests/test_miniaod_merge_smoke.sh
# or from the common harness:
./tests/run_all_tests.sh --with-miniaod-merge-smoke
```

By default it uses two files discovered under:

```text
root://cceos.ihep.ac.cn:1094///store/user/xcheng/MC_Production_v3/output/JUP_DPS1/47[0-9]/output_MINIAOD.root
```

For these `xcheng` MiniAOD inputs, keep the triple slash after the explicit
IHEP endpoint (`:1094///store/...`). Do not normalize it to
`:1094//store/...` or `:1094/store/...` in the smoke test URL builder.

It writes merged output and the merge manifest under `/tmp/chiw/`.

Do not run XRootD/IHEP remote-access smoke tests from the restricted Codex
sandbox. In that environment `xrdfs`/`xrdcp` can fail immediately with
`[FATAL] Invalid address` or equivalent sandbox/network errors even when the
same command works on a normal CERN/IHEP login shell. Run these tests on an
interactive shell with the usual CMS environment and proxy, or explicitly allow
unsandboxed network execution when using an agent.

## Exact LHE Path Validation

Production nodes receive exact paths from
`common/node_config_defaults.json:lhe_pool_directories`. Runtime code must not
guess between `LHE_pool`, `lhe_pools`, capitalization variants, or legacy
layouts.

## Parallel LHE Inventory Counting

For a large existing-LHE inventory, prepare a plain HTCondor cluster with one
counting process per source file. This is not a DAGMan workflow:

```bash
python3 dag_generator.py scan-lhe-inventory \
  --campaign JJP_ALL \
  --count-events \
  --output inventories/jjp_counted.json \
  --run-on-condor /afs/cern.ch/user/c/chiw/condor/jjp_inventory_count \
  --machine-env lxplus_t2_ihep \
  --condor-max-materialize 50
```

The preparation command does not submit unless `--submit` is included. Submit
an existing prepared workspace through the same CLI:

```bash
python3 dag_generator.py scan-lhe-inventory \
  --run-on-condor /afs/cern.ch/user/c/chiw/condor/jjp_inventory_count \
  --submit
```

After the jobs finish, merge their result fragments:

```bash
python3 dag_generator.py scan-lhe-inventory \
  --summarize-from /afs/cern.ch/user/c/chiw/condor/jjp_inventory_count \
  --output inventories/jjp_counted.json
```

Summarization writes a diagnostic JSON even when results are missing or bad,
but returns nonzero and marks it `complete: false`. DAG generation rejects
such an inventory unless `--allow-incomplete-lhe-inventory` is explicitly
given. Paths below `/eos/user/` are recorded as exact
`root://eosuser.cern.ch///eos/user/...` URLs; other local paths must be visible
at the same path on the selected execute nodes.

Validate the committed configuration with:

```bash
python3 tools/compile_node_config.py \
  --pool-paths common/node_config_defaults.json \
  --pool pool_2jpsi_cs \
  --pool pool_gg \
  --output /tmp/chiw/node_config_defaults.verified.json
```

The compiler runs `xrdfs ls` for each selected remote pool and requires at
least one `.lhe` or `.lhe.gz` file. Select the pools required by the campaign;
do not block unrelated pilots on pools that have not been produced yet. IHEP
URLs must include the explicit `cceos.ihep.ac.cn:1094` endpoint.

## Local Pilot Generation

Generate one five-event MiniAOD pilot without submitting:

```bash
./tests/submit_tests.sh \
  --campaign JJP_DPS2_CS \
  --jobs 1 \
  --max-events 5 \
  --output-dir tests/generated/pilot_exact_paths \
  --output pilot.dag
```

Before submission, inspect:

```bash
python3 -m json.tool \
  tests/generated/pilot_exact_paths/node_configs/processing/PROC_JJP_DPS2_CS_0.json
rg -n '^(JOB|VARS|CATEGORY|MAXJOBS|FINAL)' \
  tests/generated/pilot_exact_paths/pilot.dag
```

For `JJP_DPS2_CS`, `max_events: 5` means each of the two LHE sources is
showered up to five events and then mixed into **five output events**. It does
not mean ten output events.

## Pilot Submission and Monitoring

Record the schedd before submission because cluster IDs are schedd-local:

```bash
myschedd show
condor_submit_dag tests/generated/pilot_exact_paths/pilot.dag
```

Record the DAG cluster ID printed by `condor_submit_dag`. Monitor the same
schedd explicitly if `myschedd` later changes:

```bash
condor_q -name bigbirdNN.cern.ch <dag-cluster> -nobatch
condor_q -name bigbirdNN.cern.ch \
  -constraint 'DAGManJobId == <dag-cluster>' -nobatch
```

A pilot is accepted only when:

- DAGMan exits with status 0.
- Every processing node and the final summary node exits with status 0.
- Processing logs show the configured exact LHE URLs.
- Shower, mixing, GEN-SIM, RAW, RECO, MiniAOD, and stage-out all complete.
- The remote MiniAOD exists at the path logged by the processing node.

The summary node is only a completion marker. It does not infer the output
path; use the processing log, `metadata.json`, and node JSON.

## Output Verification

Verify and download the exact product:

```bash
xrdfs root://cceos.ihep.ac.cn:1094/ ls -l \
  /store/user/chiw/MC_Production_v3/output/JJP_DPS2_CS/0

xrdcp -f \
  root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/output/JJP_DPS2_CS/0/output_MINIAOD.root \
  /tmp/chiw/pilot_JJP_DPS2_CS_0_MINIAOD.root

file /tmp/chiw/pilot_JJP_DPS2_CS_0_MINIAOD.root
root -l -b -q -e \
  'TFile f("/tmp/chiw/pilot_JJP_DPS2_CS_0_MINIAOD.root"); auto t=f.Get<TTree>("Events"); if (!t) gSystem->Exit(2); std::cout << "EVENT_COUNT=" << t->GetEntries() << std::endl;'
```

Standalone ROOT is sufficient for counting `Events` entries. Use `cmsenv` only
after changing into the `src` directory of an actual CMSSW project.

## Cleanup

Remove obsolete pilots from the schedd where they were submitted:

```bash
condor_rm -name bigbirdNN.cern.ch <child-cluster> <dag-cluster>
condor_q -name bigbirdNN.cern.ch <child-cluster> <dag-cluster> -nobatch
```

An empty final query confirms removal. Do not delete generated DAG directories
until their logs and metadata are no longer needed.

## Specialized Checks

```bash
# LHE generation matrix; submits six fast-test jobs and validates outputs.
./tests/submit_lhe_matrix.sh --submit --wait

# Ntuple-enabled smoke generation.
./tests/run_all_tests.sh \
  --enable-ntuple \
  --cmssw15-runtime-tarball \
  common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz
```

The scripts `tests/test_lhe_generation.sh`, `tests/test_shower_chain.sh`,
`tests/test_cmssw_chain.sh`, and `tests/test_pipeline.sh` are retained for
component debugging. They are not the primary acceptance workflow.

## JpsiJpsiPhi v4 Production

Full JJP production from the immutable v3 LHE pools must use block SubDAGs:

```bash
--campaign JJP_ALL
--jobs 1000
--enable-lhe-block-subdags
--skip-lhe-generation
--scan-existing
--lhe-events-per-block 1000
--lhe-shuffle-mode stratified
```

Here `--jobs 1000` means 1,000 source LHE files per required pool, not 1,000
output files. Planner nodes deterministically shuffle each file and split it
into non-overlapping blocks. Planner results may be reused by different
subprocesses. Within one subprocess, repeated inputs such as the two
`pool_jpsi_CSCO_g` occurrences in `JJP_DPS1` and `JJP_TPS` consume distinct
block indices. Strict-min determines the number of mixed output blocks.

Each output ID includes both the source-file index and block index:
`JOBxxxxxx_BLOCKxxxxxx`. This prevents different planner groups from
overwriting one another.

For small existing-LHE pilots, do not combine a full-size LHE file with tiny
`--lhe-events-per-block` values unless the planner is capped. In
`generate-test`, positive `--max-events` automatically becomes
`--lhe-max-events-per-plan` for existing-LHE block SubDAGs. For example,
`--max-events 20 --lhe-events-per-block 5` makes each PLAN node shuffle the
full input ordering but emit only four 5-event source blocks.

The v4 target base is:

```text
root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4
```

MiniAOD and ntuple files are written under:

```text
JpsiJpsiPhi_MC_Production_v4/output/<campaign>/JOBxxxxxx_BLOCKxxxxxx/
```

`JJP_ALL` includes `JJP_TPS`. TPS uses two distinct J/psi blocks plus one gg
block for every mixed output block.

Generate the full v4 DAG with:

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_ALL \
  --jobs 1000 \
  --max-events -1 \
  --enable-lhe-block-subdags \
  --skip-lhe-generation \
  --scan-existing \
  --lhe-events-per-block 1000 \
  --lhe-shuffle-mode stratified \
  --enable-ntuple \
  --cleanup \
  --cmssw15-runtime-tarball \
    common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz \
  --target-base-url \
    root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4 \
  --dagman-max-jobs-submitted 20000 \
  --dagman-max-jobs-idle 20000 \
  --maxjobs-lhe 20000 \
  --maxjobs-processing 20000 \
  --maxjobs-ntuple 20000 \
  --max-block-subdag-jobs 20000 \
  --output-dir generated/JpsiJpsiPhi_MC_Production_v4 \
  --output JpsiJpsiPhi_MC_Production_v4.dag
```

The validated top-level shape is 4,000 shared planner nodes, 6,000
coordinator nodes, 6,000 block SubDAGs, and zero HELAC-generation nodes. Record
`myschedd show` immediately before `condor_submit_dag`. The `20000` ceilings
are deliberately above the expected workflow width and serve as non-binding
safety bounds rather than production throttles.
