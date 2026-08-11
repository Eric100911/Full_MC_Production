# Continuous Production Operations Reference

## IHEP directory conventions

```
/scratchfs2/cms/wangchi/hepjobs/Full_MC_Production/
  inbox/<timestamp>/<batch>/         # split-manifest.json
  handoffs/<batch>/                  # filtered coordinator manifest
    plan_subdags/<campaign>/job_<n>/ # coord_manifest_*.json
  runs/<timestamp>/<batch>/          # merge-ntuple workspace
    tasks/<campaign>/                # merge_tasks.json, ntuple_tasks.json
    scripts/                         # run_{merge|ntuple}_*.sh wrappers
    gates/                           # {merge|ntuple}_<campaign>.json
    logs/{merge|ntuple}/<campaign>/  # ProcId .out/.err
    bundles/                         # proxy_bundle.tar.gz, ntuple_runtime_bundle.tar.gz
  credentials/<timestamp>/credentials/ # x509_user_proxy
  repo/                             # Full_MC_Production clone
```

## Key commands

### Generate merge-ntuple workspace

```bash
python3 dag_generator.py generate --output-mode merge-ntuple \
  --split-manifest inbox/<ts>/<batch>/split-manifest.json \
  --output-dir runs/<ts>/<batch> \
  --cmssw15-runtime-tarball common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz \
  --hepjob-group cms --hepjob-merge-walltime short --hepjob-ntuple-walltime mid \
  --proxy credentials/<ts>/credentials/x509_user_proxy
```

### Audit stages

```bash
# MIX gate
python3 dag_generator.py audit-split --stage mix \
  --workspace handoffs/<batch> --output inbox/<ts>/<batch>/split-manifest.json

# MERGE gate (run from IHEP; needs X509_USER_PROXY)
python3 dag_generator.py audit-split --stage merge \
  --workspace runs/<ts>/<batch>

# NTUPLE gate (run from IHEP; needs X509_USER_PROXY)
python3 dag_generator.py audit-split --stage ntuple \
  --workspace runs/<ts>/<batch>
```

### Submit retry cluster

The audit writes snapshot-isolated retry artifacts:
- `tasks/<campaign>/<stage>_retry_tasks_<stamp>.json`
- `scripts/run_<stage>_<campaign>_retry_<stamp>.sh`
- `submit_<stage>_<campaign>_retry.sh` (stable entry point, always latest)

Submit only the generated retry script:
```bash
bash submit_<stage>_<campaign>_retry.sh
```

### HepJob queue check

```bash
export PATH=/cvmfs/common.ihep.ac.cn/software/hepjob/bin:$PATH
export HepJob_SiteName=ihep
hep_q -u wangchi          # all jobs
hep_q <cluster_id>        # specific cluster
```

## Inventory classification (tps_cutover_inventory.py)

The inventory compares each component block and merge group against its
planned EOS URL. Classification:

| Category | Condition |
|----------|-----------|
| cat1 | `split_ntuple_manifest` present with `status: ok` + `ntuple_url` match |
| cat2 | `merge_manifest` present with valid `merge_eligible` + `output_url` match |
| cat3 | All 5 `processing_manifest` sidecars present, `complete: true`, `merge_eligible` |
| cat4 | One or more component blocks missing valid processing manifest |

Output: `<work>/inventory/tps_job6_inventory.json` with `summary` (cat counts, blocks valid/missing) and `groups` (per-group detail including per-component `actual_miniaod_events`).

## Sidecar schemas

### processing_manifest (per block)
```json
{"status": "ok", "complete": true, "job_id": "JOB..._BLOCK...", "merge_eligible": true}
```

### merge_manifest (per merge group)
```json
{"campaign": "...", "job_id": "JOB..._MERGE...", "status": "ok", "merge_eligible": true,
 "output_url": "root://.../output_MINIAOD.root", "actual_events": 4024,
 "expected_events": 4024, "size_bytes": 271969670,
 "components": [{"job_id": "...", "actual_miniaod_events": 789, ...}, ...]}
```

### split_ntuple_manifest (per ntuple)
```json
{"status": "ok", "campaign": "...", "job_id": "JOB..._MERGE...",
 "ntuple_url": "root://.../output_ntuple.root"}
```

## Counting events via uproot

```python
import uproot
f = uproot.open("root://cceos.ihep.ac.cn:1094///store/user/chiw/.../output_ntuple.root")
n = f["Events"].num_entries
```

## XRootD quick reference

```bash
# Listing (needs proxy or authentication)
xrdfs root://cceos.ihep.ac.cn:1094/ ls /store/user/chiw/...

# Stat (works anonymously for public dirs, needs proxy for user dirs)
xrdfs root://cceos.ihep.ac.cn:1094/ stat /store/user/chiw/...

# Copy (needs proxy)
xrdcp --nopbar -f root://cceos.ihep.ac.cn:1094///store/... /tmp/local

# EOS endpoint: cceos.ihep.ac.cn:1094
# Path pattern: ///store/user/chiw/...  (triple slash after endpoint)
```
