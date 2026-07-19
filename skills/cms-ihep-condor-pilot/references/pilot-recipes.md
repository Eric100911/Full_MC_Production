# Pilot recipes

## Small existing-LHE MiniAOD merge pilot

This exercises:

```text
existing LHE -> planner -> coordinator -> block processing -> MiniAOD merge -> ntuple -> final inventory
```

For `JJP_DPS1` with one source file:

- `--jobs 1` means one source LHE file from `pool_jpsi_CSCO_g`.
- `JJP_DPS1` uses `pool_jpsi_CSCO_g` twice, so mixed blocks consume distinct
  block indices from the same planned source.
- `generate-test` with existing LHEs auto-caps each PLAN node from positive
  `--max-events`, so `--max-events 20 --lhe-events-per-block 5` gives four
  source blocks without splitting the whole remote file into tiny blocks.
- Set the processing target and both source budgets to 5 for this deliberately
  tiny pilot. Each mixed block then consumes one normal and one phi source
  block, giving two mixed processing blocks from the four-block plan. Without
  these overrides, the default 110/350-event budgets exhaust the small plan
  before both repeated source occurrences can be filled.
- `--miniaod-merge-events 10` should produce one merged MiniAOD and one ntuple
  for a 2-block pilot.

Generate:

```bash
ts=$(date +%Y%m%d_%H%M%S)
out="generated/miniaod_merge_existing_pilot_${ts}"
log="/tmp/chiw/miniaod_merge_existing_pilot_logs_${ts}"
mkdir -p "$out" "$log"

python3 dag_generator.py generate-test \
  --campaign JJP_DPS1 \
  --jobs 1 \
  --max-events 20 \
  --target-mixed-events 5 \
  --normal-max-lhe-events 5 \
  --phi-max-lhe-events 5 \
  --enable-lhe-block-subdags \
  --skip-lhe-generation \
  --no-scan-existing \
  --lhe-events-per-block 5 \
  --lhe-shuffle-mode stratified \
  --enable-ntuple \
  --miniaod-merge-events 10 \
  --miniaod-merge-validation event-count \
  --log-root "$log" \
  --output-dir "$out" \
  --output miniaod_merge_existing_pilot.dag
```

Expected top-level DAG shape before submission:

```text
PLAN_* -> COORD_* -> SUBDAG EXTERNAL MIX_*
FINAL SUMMARY
```

There should be no `LHE_*` jobs. The merge and ntuple jobs are generated later
inside `plan_subdags/<campaign>/job_0/blocks_processing.dag` by the coordinator.
Inspect the planner JSON before submission; it should contain:

```json
"events_per_block": 5,
"max_events_per_plan": 20
```

After coordination, each processing config should also contain:

```json
"target_mixed_events": 5,
"event_id_span": 5,
"minimum_output_fraction": 0.8
```

Verify that the two repeated source entries use distinct block indices and that
each mixed block has a distinct `edm_event_id.first_luminosity_block`.

Submit:

```bash
condor_submit_dag "$out/miniaod_merge_existing_pilot.dag"
```

Monitor:

```bash
condor_q <dag-cluster> -nobatch
tail -80 "$out/miniaod_merge_existing_pilot.dag.dagman.out"
tail -80 "$out/miniaod_merge_existing_pilot.dag.nodes.log"
```

After coordinator completion:

```bash
rg -n '^(JOB|PARENT|CATEGORY|MAXJOBS|FINAL)' \
  "$out/plan_subdags/JJP_DPS1/job_0/blocks_processing.dag"
python3 -m json.tool \
  "$out/plan_subdags/JJP_DPS1/job_0/coord_manifest_JJP_DPS1_0.json" |
  sed -n '1,220p'
```

## XRootD checks

List configured pool directories with endpoint + plain path:

```bash
xrdfs root://cceos.ihep.ac.cn:1094/ ls /store/user/chiw/MC_Production_v3/LHE_pool
xrdfs root://cceos.ihep.ac.cn:1094/ ls /store/user/chiw/MC_Production_v3/LHE_pool/SPS-Jpsi
```

Full file URLs should use triple slash:

```text
root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-Jpsi/sample_pool_jpsi_CSCO_g_100.lhe.gz
```

## Useful live-log locations

- Top-level DAGMan: `<output-dir>/<dag>.dagman.out`
- Top-level node log: `<output-dir>/<dag>.nodes.log`
- Live Condor logs: the `--log-root` path, normally under `/tmp/chiw/...`
- Generated block SubDAG: `<output-dir>/plan_subdags/<campaign>/job_0/blocks_processing.dag`
- Coordinator manifest: `<output-dir>/plan_subdags/<campaign>/job_0/coord_manifest_<campaign>_0.json`
