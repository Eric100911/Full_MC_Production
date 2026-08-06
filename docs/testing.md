# Test Procedures

This is the canonical test and pilot-run procedure for this branch. Historical
plans and investigation notes under `docs/` are not operational instructions.

## Test Levels

1. Static checks validate syntax, Python imports, configuration rendering, and
   DAG structure without submitting jobs.
2. The local worker mock exercises the production runtime bundle and wrapper
   through one-event MiniAOD production, containerized `edmFileUtil` counting,
   manifest validation, and local stage-out. It requires a valid CMS proxy and
   premix network access.
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
  tests/mock_test_edm_eventid.sh \
  tests/test_lhe_shuffle_split.sh
python3 -m py_compile \
  dag_generator.py \
  tools/compile_node_config.py \
  tools/coordinate_lhe_blocks.py \
  tools/dag_progress.py \
  tools/migrate_cap700_job_spec.py \
  tools/benchmark_phi_efficiency.py \
  tools/review_phase2_shower_efficiency.py
python3 -m unittest tests.test_dag_progress
python3 tests/test_coordinate_lhe_blocks.py
python3 tests/test_campaign_job_specs.py
python3 tests/test_lhe_planner_cap_generation.py
./tests/run_all_tests.sh
./tests/mock_test_worker.sh
./tests/test_lhe_shuffle_split.sh
```

The real GEN-SIM EventID smoke requires a usable HepMC fixture and CMSSW
environment. Run it explicitly with
`./tests/run_all_tests.sh --with-edm-eventid-smoke` or
`./tests/mock_test_edm_eventid.sh`.

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

For this flat-DAG `JJP_DPS2_CS` pilot, `max_events: 5` means each of the two LHE
sources is showered up to five events and then mixed into **five output
events**. It does not mean ten output events. Block SubDAG configs use separate
source budgets and should not infer LHE consumption from `--max-events` alone.

## MC_v4_1 Efficiency-balanced pilot

The v2 pilot is inventory-driven. Its job spec freezes the counted inventory
hash, selected nonconsecutive seeds, 1,000-event block layout, source
efficiencies, per-slot LHE budgets, capacity limits, and storage estimate.

Before submitting the pilot, record these production dimensions:

- campaigns: `JJP_DPS1_MC_v4_1` and `JJP_TPS_MC_v4_1`;
- pilot processing products: 10 per campaign (20 total);
- unique pilot source files: 10 `pool_jpsi_CSCO_g` plus 10 `pool_gg`;
- planner exposure: 2,000 events per selected J/psi file and 1,000 per gg file,
  split into 1,000-event blocks;
- DPS1 LHE budgets: normal 860, phi 1,000;
- TPS LHE budgets: normal 1,000 + normal 1,000 + phi 977;
- MiniAOD merge target: 5,000 events, packed statically with a 350-event
  exposure weight and the closest-boundary rule;
- current topology: one processing product per outer job, so retain one-input
  merge nodes and do not cross job boundaries;
- subprocess coverage: `DPS-Jpsi-JpsiPhi` and `TPS-JpsiJpsiPhi`.

Generate the 10+10 pilot on an AFS submit-visible directory with the existing
counted inventory and a new campaign output prefix:

```bash
python3 dag_generator.py generate \
  --machine-env lxplus_t2_ihep \
  --campaign JJP_DPS1_MC_v4_1 \
  --campaign JJP_TPS_MC_v4_1 \
  --campaign-job-spec common/campaign_job_specs/jjp_efficiency_balanced_pilot_v2.json \
  --skip-lhe-generation \
  --no-scan-existing \
  --existing-lhe-inventory generated/lhe_inventory_jjp_20260717_184812.json \
  --enable-lhe-block-subdags \
  --lhe-shuffle-split \
  --phi-consumption-mode exhaustive \
  --normal-shortfall-policy report-and-truncate \
  --miniaod-merge-events 5000 \
  --miniaod-merge-validation event-count \
  --enable-ntuple \
  --archive-subdag-logs \
  --proxy-path /afs/cern.ch/user/c/chiw/condor/x509up \
  --cmssw15-runtime-tarball common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz \
  --target-base-url root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4 \
  --output-dir generated/JpsiJpsiPhi_MC_Production_v4_1_exhaustive_pilot \
  --output JpsiJpsiPhi_MC_Production_v4_1_exhaustive_pilot.dag
```

The pilot IDs must be exactly:

```text
100 1095 1191 1287 1383 1479 1575 1670 1767 1862
```

Accept the pilot only if all 20 complete chains succeed. For every processing
sidecar require `status: ok`, `complete: true`, positive and equal
mixed/MiniAOD counts, and `miniaod_count_source: edmFileUtil`. Confirm the
consumed LHE exposure matches the slot budget recorded in its processing
config. Require every merge manifest to report
`expected_events_source: input_manifests`. A positive normal-source shortfall
is recorded and lowers the common mixed yield; zero normal output still fails.
Also inspect `source_event_balance`: an `unused_fraction` above the fixed 0.15
threshold emits a warning. It is always reported and never halts an otherwise
valid chain. The 15% value is a diagnostic threshold, not a configurable
production target.

The normal shower continues past recoverable `pythia.next()` failures until it
reaches its accepted-event target, its configured LHE budget, or EOF. Do not
increase the normal budget merely to compensate for an early ten-error abort.
After the pilot, recompute the normal:phi ratio from measured efficiencies and
freeze the accepted values and inventory/layout hash in the production v2
spec.

Generated MiniAOD merge nodes request one CPU and 3 GB memory. With
`--archive-subdag-logs`, the existing top-level `FINAL SUMMARY` becomes a
one-CPU EL9 worker that scans the configured `log_root`, groups the
`processing`, `miniaod_merge`, `ntuple`, and `final` logs by campaign/job, and
uploads one structured archive plus manifest for each group. The helper is
snapshotted in `summary_runtime_bundle.tar.gz`; no network or credential work
runs as a DAGMan POST script on the schedd.

The FINAL worker uses the proxy frozen into `proxy_bundle.tar.gz`. An expired
proxy makes log archival fail-soft and writes
`_shared/summary/workflow_log_archive_status_<workflow-id>.json`; it does not
invalidate successful physics work. The FINAL wrapper propagates
`DAG_STATUS`/`FAILED_COUNT`, so an upstream DAG failure is not hidden by a
successful summary or archive.

Remote archives use:

```text
<target>/output/<campaign>/<job_component>_logs/<workflow-id>/
<target>/output/_log_archives/<workflow-id>/archive_index.json
```

`tools/archive_subdag_logs.sh` remains available only for manual recovery of
older generated DAGs.

For a processing job whose CMSSW chain completed but whose stageout manifest
reports `transfer_failed`, set
`PROCESSING_STAGEOUT_RECOVERY=validate-existing-or-rerun` in a recovery submit
file. The worker first checks the remote ROOT and manifest sizes, opens the
ROOT with `edmFileUtil`, and compares its event count with the manifest. A
valid product keeps the original manifest under the job's `recovery/`
directory and replaces the canonical manifest with recovery provenance. Any
missing, empty, unreadable, or event-mismatched product automatically falls
back to the full processing chain. Acceptance requires a final
`size-verified` log line for both the ROOT product and canonical manifest.

The generated metadata contains `production_capacity_signals`: bottleneck
block counts, available and selected processing nodes, predicted event yields,
and retained-storage projection. Generation rejects a projection above 5 TB
and marks projections above 4 TB as warnings. Only after the pilot gate passes
should a full v2 spec be created with the measured efficiencies.

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

Use the progress helper to include nested SubDAG workers under the root
`JobBatchId` and combine live queue state with completed DAGMan node logs:

```bash
# Progress plus adaptive topology (full through 150 known nodes).
./tools/dag_progress.py <dag-cluster> \
  --schedd bigbirdNN.cern.ch \
  --structure

# One aggregated lane per campaign for a large production.
./tools/dag_progress.py <dag-cluster> \
  --schedd bigbirdNN.cern.ch \
  --structure=collapsed \
  --color=auto

# Exact layered topology with an explicit terminal width.
./tools/dag_progress.py <dag-cluster> \
  --schedd bigbirdNN.cern.ch \
  --structure=full \
  --width 160 \
  --details
```

Color defaults to `auto`; use `--color=always` when preserving ANSI colors
through a compatible pager or `--color=never` for plain logs. If the root
DAGMan job has already left the queue, also pass the persistent root DAG path
with `--dag-file`.

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
block indices. The coordinator consumes distinct planned blocks until every
source occurrence reaches its configured LHE-event budget. It stops when any
required source can no longer form another complete budgeted group; this
determines the number of mixed output blocks.

In inventory-driven v2 production, pre-generation block counts are capacity
estimates only. Stratified splitting can create several partial tail blocks
per source file, so exact cross-campaign and shard boundaries are calculated
after every planner finishes. `ALLOCATE_CAMPAIGN_SHARDS` scans the shared
planner-manifest index once and writes
`plan_subdags/campaign_shard_allocation.json`. Every coordinator then loads its
own shard cursor from that file. Check that coordinator configs do not contain
an embedded `source_manifests` array; they should reference the shared
campaign-level JSON file instead.

Each output ID includes both the source-file index and block index:
`JOBxxxxxx_BLOCKxxxxxx`. This prevents different planner groups from
overwriting one another.

For small existing-LHE pilots, do not combine a full-size LHE file with tiny
`--lhe-events-per-block` values unless the planner is capped. In
`generate-test`, positive `--max-events` automatically becomes
`--lhe-max-events-per-plan` for existing-LHE block SubDAGs. For example,
`--max-events 20 --lhe-events-per-block 5` makes each PLAN node shuffle the
full input ordering but emit only four 5-event source blocks. That statement is
only about planner output. For a repeated-pool campaign such as `JJP_DPS1`, a
two-block processing pilot must also set `--target-mixed-events 5`,
`--normal-max-lhe-events 5`, and `--phi-max-lhe-events 5`; otherwise the default
110/350-event source budgets can consume the small plan before all source
occurrences are filled.

Every block processing config records:

- `target_mixed_events` and `minimum_output_fraction`;
- authoritative per-source input groups and planned LHE-event counts;
- a deterministic, non-overlapping `edm_event_id` reservation;
- the URL of `processing_manifest_<campaign>_<job_id>.json`.

The processing sidecar records actual mixed and MiniAOD event counts plus
per-source retry/acceptance statistics. Merge validation uses these actual
component counts when all sidecars are available.

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
  --target-mixed-events 100 \
  --normal-max-lhe-events 110 \
  --phi-max-lhe-events 350 \
  --phi-max-hadronization-retries 5000 \
  --minimum-output-fraction 0.8 \
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

## Shower Efficiency Review

Compare standalone shower manifests from retry-limit benchmarks:

```bash
python3 tools/benchmark_phi_efficiency.py \
  /tmp/chiw/benchmarks/shower_*_manifest.json \
  --json-output /tmp/chiw/phi_benchmark.json \
  --csv-output /tmp/chiw/phi_benchmark.csv
```

Join coordinator manifests, generated processing configs, and staged processing
sidecars for a completed pilot:

```bash
python3 tools/review_phase2_shower_efficiency.py \
  --pilot-dir generated/<pilot> \
  --cache-dir /tmp/chiw/phase2_shower_review_manifests \
  --fetch-remote \
  --json-output /tmp/chiw/phase2_shower_review.json \
  --csv-output /tmp/chiw/phase2_shower_review.csv
```

Run `--fetch-remote` only from a normal CERN/IHEP shell or with approved
unsandboxed network access and a valid proxy.

## CERN MIX / IHEP MERGE+NTUPLE split

### Preconditions and invariants

- Use block SubDAG generation with MiniAOD merge planning enabled. In
  `mix-only`, coordinator manifests freeze merge groups even though the CERN
  SubDAG emits no MERGE, NTUPLE, or per-SubDAG FINAL nodes.
- Keep `target_eos_base` and storage settings centralized in
  `common/node_config_defaults.json`. IHEP full URLs must retain
  `root://cceos.ihep.ac.cn:1094///store/...`.
- Run IHEP preparation from an IHEP-visible repository checkout and place the
  generated workspace on persistent IHEP-visible storage.
- A valid CMS proxy is rebuilt into `bundles/proxy_bundle.tar.gz` immediately
  before each stage submission. Submission stops if less than ten minutes remain.
- Split audits intentionally validate sidecar manifests only. They do not
  checksum or download ROOT files. Existing worker-side EDM validation remains
  authoritative.

| Stage | Success evidence | Enables |
|---|---|---|
| MIX | Every processing sidecar is complete or explicitly merge-eligible | Frozen split manifest |
| MERGE | Every merge sidecar is merge-eligible and names the planned output | Per-campaign merge gate |
| NTUPLE | Every split ntuple sidecar names the planned output | Per-campaign ntuple gate and cleanup |

### Generate and run the CERN stage

Generate the CERN DAG with the normal campaign and block-planning options, plus
`--output-mode mix-only`.  Coordinator SubDAGs then contain only MIX nodes;
their manifests retain the frozen merge groups and ntuple destinations.

```bash
python3 dag_generator.py generate \
  --campaign <campaign> \
  --output-mode mix-only \
  <normal production options> \
  --output-dir generated/<cern-workspace> \
  --output mix-only.dag
```

After the CERN DAG has completed, export the handoff manifest.  This gate reads
processing sidecars only; it does not download or checksum ROOT files.  Worker
scripts retain their normal EDM validation.

```bash
python3 dag_generator.py audit-split \
  --stage mix \
  --workspace generated/<cern-workspace> \
  --output /path/on/ihep/split-manifest.json
```

### Prepare and run the IHEP stages

On an IHEP login node, use a repository checkout and an IHEP-visible persistent
output directory.  This creates two explicit HepJob submit scripts per campaign:
MERGE first, then NTUPLE after the merge audit gate exists.

```bash
python3 dag_generator.py generate \
  --output-mode merge-ntuple \
  --split-manifest /path/on/ihep/split-manifest.json \
  --output-dir /scratchfs/cms/<user>/split-workspace

bash /scratchfs/cms/<user>/split-workspace/submit_merge_<campaign>.sh
python3 dag_generator.py audit-split --stage merge \
  --workspace /scratchfs/cms/<user>/split-workspace
bash /scratchfs/cms/<user>/split-workspace/submit_ntuple_<campaign>.sh
python3 dag_generator.py audit-split --stage ntuple \
  --workspace /scratchfs/cms/<user>/split-workspace
```

An incomplete audit writes a compact `*_retry_tasks.json` and matching
`submit_*_retry.sh`. Inspect the failed count and worker logs, submit only
that retry script, and repeat the same audit. A successful audit removes stale
retry metadata and writes `gates/<stage>_<campaign>.json`.

The generated HepJob scripts use one array-style cluster per campaign and pass
`%{ProcId}` as the task index. Default resource classes are `short` for
MERGE and `mid` for NTUPLE. Override them during workspace generation with
`--hepjob-merge-walltime`, `--hepjob-ntuple-walltime`,
`--hepjob-merge-memory-mb`, and `--hepjob-ntuple-memory-mb`.

### DPS1 split architecture validation (2026-08-06)

The split workflow was exercised end to end with HepJob on IHEP using an
isolated CERN pilot and production groups 71--74. The production canary merged
group 71 while accepting the existing successful group 74 merge sidecar, then
produced both ntuples. Groups 72 and 73 were submitted only after the canary
ntuple gate passed. The CERN pilot produced two five-event component MiniAODs,
which IHEP merged into one approximately ten-event MiniAOD before running the
ntuple stage.

| Scope | MERGE cluster | NTUPLE cluster | Result |
|---|---:|---:|---|
| Production canary, groups 71/74 | `80201258`, retry `80201270` | `80201303` | Both gates complete |
| Production batch, groups 72/73 | `80201381` | `80201413` | Both gates complete |
| Isolated CERN pilot | `80201291` | `80201305`, retry `80201359` | Both gates complete |

Each production merge contained 4,300 events, matching the five processing
sidecars at 860 valid events each. The resulting ntuple sizes were 43,832,169,
43,736,625, 43,832,653, and 43,790,526 bytes for groups 71--74 respectively;
the pilot ntuple was 229,098 bytes. The files were checked with `xrdfs stat`,
and every split ntuple sidecar named its planned triple-slash IHEP URL.

The test exposed four operational compatibility requirements:

- IHEP login-node Python 3.6 requires `universal_newlines=True` rather than the
  newer `subprocess.run(..., text=True)` spelling in the merge wrapper.
- The `test` HepJob walltime is only five minutes and is too short for this
  ntuple workload; use `short` or longer even for the small pilot.
- The outer split wrapper must extract and export the bundled proxy again after
  the ntuple container exits so that post-container sidecar stage-out is
  authenticated independently of host and container UID differences.
- Create the configured HepJob log directory before submission. If a cluster
  is held only for that missing directory, create the exact path and release
  the affected processes with `hep_release`.

All recorded clusters had left the queue at completion. Cleanup was previewed
for the canary, batch, and pilot workspaces with `finalize-split` in dry-run
mode only; no component MiniAOD was deleted.

### Finalize component cleanup

Cleanup is explicit and dry-run by default:

```bash
python3 dag_generator.py finalize-split --workspace /scratchfs/cms/<user>/split-workspace
python3 dag_generator.py finalize-split --workspace /scratchfs/cms/<user>/split-workspace --apply
```

Finalize requires a successful ntuple audit gate and removes component MiniAODs
only; merged MiniAODs, ntuples, and sidecars are retained. Each invocation
writes a timestamped cleanup report under `logs/cleanup/`. Review the dry-run
report before using `--apply`.

The legacy polling workflow remains available for existing deployments, but new
split production should use these explicit stage commands. Workspace generation
does not submit jobs: record the IHEP campaign, task count, HepJob submission
output, and stage gate before proceeding.
