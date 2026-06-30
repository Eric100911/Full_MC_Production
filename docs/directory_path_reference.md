# Directory & Path Reference

How intermediate and final product files are named and placed. Existing LHE
pool paths are exact configuration values; there is no runtime layout fallback.

---

## Central storage config

All default paths flow from `common/node_config_defaults.json`:

```json
{
  "storage": {
    "eos_redirector": "cceos.ihep.ac.cn:1094",
    "eos_lfn_base": "/store/user/chiw/MC_Production_v3",
    "output_subdir": "output"
  },
  "lhe_pool_directories": {
    "pool_2jpsi_cs": {
      "storage_name": "pool_2jpsi_cs",
      "path": "root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO"
    }
  }
}
```

`dag_generator.py` reads this on import and deep-copies the exact
`lhe_pool_directories` mapping into generated node configs. `run_chain.sh`
resolves `EOS:<pool>:...` only from that mapping and fails if the path is
missing, cannot be listed, or contains no LHE files.

IHEP XRootD full URLs use the explicit endpoint and a triple slash before the
LFN: `root://cceos.ihep.ac.cn:1094///store/...`. Do not normalize these URLs to
`:1094//store/...` or `:1094/store/...`.

`tools/compile_node_config.py` can compile explicit mappings and validates each
configured directory with `xrdfs ls` or local directory inspection. This
validation belongs before submission, not in worker-side layout inference.

---

## The master override: `TARGET_EOS_BASE`

Every worker script that writes to remote storage follows the same pattern:

```bash
EOS_BASE="${TARGET_EOS_BASE:-root://${EOS_REDIRECTOR}//${EOS_LFN_BASE}}"
EOS_GENERATED_LHE_BASE="${EOS_BASE}/lhe_pools"
EOS_OUTPUT="${EOS_BASE}/output"
```

| Variable | Derivation | Example |
|----------|-----------|---------|
| `EOS_BASE` | Configured target base or `$TARGET_EOS_BASE` | `root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3` |
| `EOS_GENERATED_LHE_BASE` | `storage.generated_lhe_base` | `$EOS_BASE/lhe_pools` |
| `EOS_OUTPUT` | `$EOS_BASE/output` | `root://…/MC_Production_v3/output` |

**How `TARGET_EOS_BASE` is set**: `dag_generator.py --target-base-url <url>` → generated node JSON `target_eos_base` / `storage.target_eos_base` → JSON wrapper sets `TARGET_EOS_BASE` in the worker environment → `run_chain.sh` derives processing output and generated-block roots.

LHE generation is separate: `run_lhe_gen.sh` reads
`node_configs/lhe_generation/LHE_*.json`, passes `output_dir` directly to
`run_helac.sh --output-dir`, and also passes the node config with `--config`.
Generated data uses `storage.generated_lhe_base`. Existing immutable pools use
their exact `lhe_pool_directories.<pool>.path`.

---

## Product directory map

```
{target_base}                           ← EOS_BASE or TARGET_EOS_BASE
│
├── lhe_pools/
│   └── {pool_storage_name}/           ← standard LHE layout
│       ├── sample_{pool}_{seed}.lhe.gz
│       └── lhe_blocks/
│           ├── block_{seed}_{idx}.lhe.gz    ← legacy (run_helac.sh direct)
│           └── {group_id}/                  ← grouped (plan_lhe_blocks.py)
│               └── block_{group_id}_{idx}.lhe.gz
│
├── LHE_pool/
│   └── {configured_subdir}/           ← exact existing path from node config
│       └── sample_{storage_name}_{seed}.lhe[.gz]
│
├── output/
│   └── {campaign_name}/
│       └── {job_id}/                  ← or BLOCKxxxxxx for block SubDAGs
│           ├── output_GENSIM.root
│           ├── output_RAW.root
│           ├── output_RECO.root
│           ├── output_MINIAOD.root
│           └── output_ntuple.root     ← (or {subprocess_id}-Ntuple-{version}-{job_id}.root)
│
└── JpsiJpsiPhi/                       ← only with --use-subprocess-naming
    └── Ntuple-{version}/
        └── {subprocess_id}/
            └── {subprocess_id}-Ntuple-{version}-{job_id}.root
```

---

## LHE generation (`run_helac.sh`)

### Where HELAC writes LHE

The LHE generation node JSON determines the remote directory:

```
node_configs/lhe_generation/LHE_*.json:
  output_dir = existing_lhe_pool_dir(pool_name, existing_lhe_base)
```

**Resolution** (`dag_generator.py` `existing_lhe_pool_dir`):
1. If `--existing-lhe-base` is supplied, append the pool's `storage_name`.
2. Otherwise return the exact configured `lhe_pool_directories.<pool>.path`.
3. If no exact path is configured, fail during DAG generation.

**Example for `pool_jpsi_CSCO_g`**:
`root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-Jpsi`

### Remote filename

```
sample_{pool_name}_{seed}.lhe.gz        ← with --compress-lhe
sample_{pool_name}_{seed}.lhe           ← uncompressed
```

The full stage-out URL:
```
{OUTPUT_DIR}/sample_{pool_name}_{seed}.lhe.gz
```

### Local stage-out (when `LOCAL_OUTPUT_BASE` is set)

```
{LOCAL_OUTPUT_BASE}/{OUTPUT_DIR}/sample_{pool_name}_{seed}.lhe.gz
```

### Block files (shuffle-split)

Local temp directory during the HELAC job:
```
{WORKDIR}/lhe_blocks/block_{seed}_{idx}.lhe
```

Remote stage-out:
```
{OUTPUT_DIR}/lhe_blocks/block_{seed}_{idx}.lhe.gz
```

---

## LHE block planning (`plan_lhe_blocks.py`)

Called by the planner DAG node, NOT directly by `run_helac.sh`. Runs `lhe_shuffle_split`, compresses blocks, and stages them.

### Block output directory

Derived from `dag_generator.py` `_resolve_block_output_dir()`:
```
{storage.generated_lhe_base}/{storage_name}/lhe_blocks
```

When grouped (`group_id != str(primary_seed)`), blocks go into a subdirectory:
```
{block_output_dir}/{group_id}/block_{group_id}_{idx:06d}.lhe.gz
```

### Plan manifest

Written locally under the DAG generation output directory:
```
{output_dir}/plan_subdags/{pool_label}/seed_{seed}/plan_manifest_{pool}_{seed}.json
```
or for grouped:
```
{output_dir}/plan_subdags/{pool_label}/{group_id}/plan_manifest_{pool}_{group_id}.json
```

---

## LHE consumption (`run_chain.sh`)

### `GEN:` input specs

Format: `GEN:pool_name:job_index[:seed]`

```
{storage.generated_lhe_base}/{storage_name}/sample_{storage_name}_{seed}.lhe.gz
```
Falls back to `.lhe`, then to listing the exact configured existing pool path.

### `BLOCK:` input specs

Format: `BLOCK:pool_name:namespace:block_idx`

Resolution tries four paths in order:
1. `{pool_base}/{pool}/lhe_blocks/{ns}/block_{ns}_{idx}.lhe.gz`  (grouped+compressed)
2. `{pool_base}/{pool}/lhe_blocks/{ns}/block_{ns}_{idx}.lhe`     (grouped+plain)
3. `{pool_base}/{pool}/lhe_blocks/block_{ns}_{idx}.lhe.gz`       (flat+compressed)
4. `{pool_base}/{pool}/lhe_blocks/block_{ns}_{idx}.lhe`          (flat+plain)

Where `pool_base = storage.generated_lhe_base`.

### `EOS:` input specs

Format: `EOS:pool_name:job_id:usage_idx`

Calls `get_lhe_file()`, which reads
`storage.lhe_pool_directories.<pool>.path`, lists only that directory, filters
`.lhe` and `.lhe.gz`, and wraps by modulo. No other directory is tried.

### `file:` input specs

Used in local chain tests — literal file path on the worker node.

Compressed local inputs are decompressed to `{WORKDIR}/input_<i>.lhe` before the C++ shower binaries run. Remote `.lhe.gz` files follow the same normalization after XRootD download.

---

## Processing output (`run_chain.sh`)

### Default output subpath

```bash
output_subpath = ${CUSTOM_OUTPUT_SUBPATH:-output/${CAMPAIGN_NAME}/${JOB_ID}}
```

For block SubDAG jobs, `JOB_ID` is
`JOB<source-file-index>_BLOCK<block-index>`, with both indices zero-padded to
six digits. Including the source-file index prevents output collisions between
different per-file SubDAGs.

### `stage_out()` function

Constructs the remote URL by prepending `EOS_BASE`:
```
{EOS_BASE}/{subpath}/{filename}
```

Full example for MiniAOD:
```
root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/output/JJP_DPS2_CS/BLOCK000042/output_MINIAOD.root
```

### Files produced

| Step | Filename |
|------|----------|
| GEN-SIM | `output_GENSIM.root` |
| RAW | `output_RAW.root` |
| RECO | `output_RECO.root` |
| MiniAOD | `output_MINIAOD.root` |
| Ntuple | `output_ntuple.root` |

---

## Ntuple output

### Standard naming

```
{EOS_BASE}/output/{campaign}/{job_id}/output_ntuple.root
```

### Subprocess naming (`--use-subprocess-naming`)

Controlled by `CUSTOM_OUTPUT_SUBPATH` and `CUSTOM_NTUPLE_BASENAME`:

```
subprocess_id = SUBPROCESS_MAP[campaign_name]   ← e.g. "SPS-JpsiJpsiPhi-LO"
version       = --ntuple-version or "v01_06"

target_base      = --target-base-url or CHIW_EOS_OUTPUT_BASE
output_subpath   = JpsiJpsiPhi/Ntuple-{version}/{subprocess_id}
ntuple_basename  = {subprocess_id}-Ntuple-{version}-{job_index}.root
```

Full URL:
```
root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/JpsiJpsiPhi/Ntuple-v01_06/SPS-JpsiJpsiPhi-LO/SPS-JpsiJpsiPhi-LO-Ntuple-v01_06-0.root
```

### Ntuple-only DAG mode

When using `generate-ntuple-only`, MiniAOD is read from an existing location. The ntuple output follows the same rules as above. The MiniAOD input is discovered by scanning:
```
{miniaod_base_url}/{campaign_name}/{job_index}/output_MINIAOD.root
```

---

## Coordinator and SubDAG paths

All local (not on EOS):

### Top-level node configs

```
{output_dir}/node_configs/lhe_generation/LHE_{pool_label}_{index}.json
{output_dir}/node_configs/planning/PLAN_{pool_label}_{index}.json
{output_dir}/node_configs/coordination/COORD_{campaign}_{job_index}.json
{output_dir}/node_configs/processing/PROC_{campaign}_{job_index}.json
{output_dir}/node_configs/ntuple/NTUPLE_{campaign}_{job_index}.json
```

Submit templates transfer these files with the runtime/proxy bundles. The worker wrapper arguments are only:

```
$(proxy_bundle_name) $(runtime_bundle_name) $(config_name)
```

### Coordinator SubDAG

```
{output_dir}/plan_subdags/{campaign_name}/job_{job_index}/blocks_processing.dag
```

### Coordinator manifest

```
{output_dir}/plan_subdags/{campaign_name}/job_{job_index}/coord_manifest_{campaign}_{job_index}.json
```

### Block processing configs

```
{output_dir}/plan_subdags/{campaign_name}/job_{job_index}/node_configs/processing/MIX_{campaign}_{job_index}_BLOCK{idx:06d}.json
```

### Ntuple configs (inside SubDAG)

```
{output_dir}/plan_subdags/{campaign_name}/job_{job_index}/node_configs/ntuple/NTUPLE_{campaign}_{job_index}_BLOCK{idx:06d}.json
```

---

## Key functions reference

| Function/script | Purpose |
|-----------------|---------|
| `existing_lhe_pool_dir()` | Return an explicit override path or the exact configured pool path |
| `existing_lhe_base_url()` | Normalize an optional explicit existing-LHE override |
| `node_storage_config()` | Build storage dict embedded in generated JSON configs |
| `DAGBuilder.write_node_config()` | Write per-node JSON configs under the DAG output tree |
| `_resolve_lhe_path()` | Path to HELAC LHE output for a seed |
| `_resolve_block_output_dir()` | Where block files should be staged |
| `_resolve_existing_lhe_path()` | Discover existing LHE by job index |
| `pool_remote_path()` | Pool base path for scan/display |
| `run_lhe_gen.sh` | Load LHE JSON config and invoke `run_helac.sh` with named flags |
| `run_processing.sh` | Load processing JSON config, set runtime env, and invoke `run_chain.sh` |
| `resolve_remote_target()` | Convert spec string to XRootD URL |
| `remote_url_for_spec()` | Full URL from spec |
| `stage_out()` | Copy local file to EOS |
| `transfer_output()` | Stage all processing outputs |
| `ntuple_cfg_path()` | Select CMSSW config for ntuple |
| `stable_seed()` | Deterministic seed from campaign+job |
| `tools/compile_node_config.py` | Compile and verify exact pool paths before submission |

---

## Storage ownership summary

After the fixes applied in this branch:

| File | Default user | Override mechanism |
|------|-------------|-------------------|
| `dag_generator.py` | **chiw** | `--target-base-url`, `--existing-lhe-base` |
| `common/node_config_defaults.json` | **chiw** | Edit the JSON |
| `run_chain.sh` | **chiw** | `TARGET_EOS_BASE` env var from processing/ntuple JSON wrappers |
| `run_helac.sh` | **chiw** | `--output-dir` from LHE generation JSON wrapper |
| `check_proxy.sh` | — (proxy-only) | N/A |

Operational scripts and active documentation use the `chiw` storage area.
Use `common/node_config_defaults.json` for exact existing pool paths,
`--existing-lhe-base` only as an explicit alternate base, and
`--target-base-url` for generated blocks and processing/ntuple output.
