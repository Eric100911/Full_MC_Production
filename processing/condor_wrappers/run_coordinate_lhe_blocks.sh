#!/bin/bash
# ==============================================================================
# run_coordinate_lhe_blocks.sh - Wrapper for the multi-source LHE block
# coordinator.
# ==============================================================================
set -euo pipefail

PROXY_BUNDLE="${1:?missing proxy bundle}"
COORD_BUNDLE="${2:?missing coordinator bundle}"
CONFIG_NAME="${3:?missing coordinator config JSON}"
CONFIG_PATH="${PWD}/${CONFIG_NAME}"

if [[ ! -s "${CONFIG_PATH}" ]]; then
    echo "ERROR: Coordinator config JSON not found or empty: ${CONFIG_PATH}" >&2
    exit 1
fi

echo "=== LHE Block Coordinator Wrapper ==="
echo "Config: ${CONFIG_NAME}"

echo "Extracting proxy bundle..."
tar -xzf "${PROXY_BUNDLE}"
PROXY_TARGET="/tmp/x509up_u$(id -u)"
install -m 600 credentials/x509_user_proxy "${PROXY_TARGET}"
rm -rf credentials
export X509_USER_PROXY="${PROXY_TARGET}"

echo "Extracting coordinator bundle..."
tar -xzf "${COORD_BUNDLE}"

echo "Running coordinate_lhe_blocks.py..."
cd runtime/tools
if ! python3 - "${CONFIG_PATH}" <<'PY'
import json
import subprocess
import sys

config_path = sys.argv[1]
with open(config_path, "r", encoding="utf-8") as handle:
    cfg = json.load(handle)

required = [
    "campaign",
    "job_index",
    "shower_modes",
    "campaign_inputs",
    "analysis_type",
    "n_sources",
    "max_events",
    "log_root",
    "request_cpus",
    "request_memory",
    "request_disk",
    "output_dir",
    "processing_sub_template_path",
    "processing_bundle_path",
    "processing_bundle_name",
    "proxy_bundle_path",
    "proxy_bundle_name",
    "processing_wrapper_path",
    "subdag_output_path",
    "max_block_subdag_jobs",
    "miniaod_merge_events",
    "miniaod_merge_validation",
    "max_miniaod_merge_jobs",
    "miniaod_merge_sub_template_path",
    "miniaod_merge_wrapper_path",
    "final_sub_template_path",
    "final_wrapper_path",
]
missing = [key for key in required if key not in cfg or cfg[key] in (None, "")]
if missing:
    raise SystemExit(f"Missing coordinator config keys: {', '.join(missing)}")

if "source_manifests" in cfg:
    source_manifests = cfg["source_manifests"]
else:
    manifest_path = cfg.get("source_manifests_path")
    if not manifest_path:
        raise SystemExit("Missing coordinator config key: source_manifests_path")
    with open(manifest_path, "r", encoding="utf-8") as handle:
        source_manifests = json.load(handle)

if not isinstance(source_manifests, list) or not source_manifests:
    raise SystemExit("Coordinator source manifests must be a non-empty list")
if not isinstance(cfg["shower_modes"], list) or not cfg["shower_modes"]:
    raise SystemExit("Coordinator config key shower_modes must be a non-empty list")
if not isinstance(cfg["campaign_inputs"], list) or not cfg["campaign_inputs"]:
    raise SystemExit("Coordinator config key campaign_inputs must be a non-empty list")

cmd = [
    "python3",
    "coordinate_lhe_blocks.py",
    "--campaign", str(cfg["campaign"]),
    "--job-index", str(cfg["job_index"]),
    "--source-manifests", json.dumps(source_manifests, separators=(",", ":")),
    "--shower-modes", ",".join(str(item) for item in cfg["shower_modes"]),
    "--campaign-inputs", ",".join(str(item) for item in cfg["campaign_inputs"]),
    "--analysis-type", str(cfg["analysis_type"]),
    "--n-sources", str(cfg["n_sources"]),
    "--max-events", str(cfg["max_events"]),
    "--log-root", str(cfg["log_root"]),
    "--request-cpus", str(cfg["request_cpus"]),
    "--request-memory", str(cfg["request_memory"]),
    "--request-disk", str(cfg["request_disk"]),
    "--output-dir", str(cfg["output_dir"]),
    "--processing-sub-template-path", str(cfg["processing_sub_template_path"]),
    "--processing-bundle-path", str(cfg["processing_bundle_path"]),
    "--processing-bundle-name", str(cfg["processing_bundle_name"]),
    "--proxy-bundle-path", str(cfg["proxy_bundle_path"]),
    "--proxy-bundle-name", str(cfg["proxy_bundle_name"]),
    "--processing-wrapper-path", str(cfg["processing_wrapper_path"]),
    "--subdag-output-path", str(cfg["subdag_output_path"]),
    "--max-block-subdag-jobs", str(cfg["max_block_subdag_jobs"]),
    "--miniaod-merge-events", str(cfg["miniaod_merge_events"]),
    "--miniaod-merge-validation", str(cfg["miniaod_merge_validation"]),
    "--max-miniaod-merge-jobs", str(cfg["max_miniaod_merge_jobs"]),
    "--miniaod-merge-sub-template-path", str(cfg["miniaod_merge_sub_template_path"]),
    "--miniaod-merge-wrapper-path", str(cfg["miniaod_merge_wrapper_path"]),
    "--final-sub-template-path", str(cfg["final_sub_template_path"]),
    "--final-wrapper-path", str(cfg["final_wrapper_path"]),
]
for key, flag in [
    ("target_mixed_events", "--target-mixed-events"),
    ("normal_max_lhe_events", "--normal-max-lhe-events"),
    ("phi_max_lhe_events", "--phi-max-lhe-events"),
    ("phi_max_hadronization_retries", "--phi-max-hadronization-retries"),
    ("minimum_output_fraction", "--minimum-output-fraction"),
    ("phi_consumption_mode", "--phi-consumption-mode"),
    ("normal_shortfall_policy", "--normal-shortfall-policy"),
    ("unused_hepmc_warning_fraction", "--unused-hepmc-warning-fraction"),
    ("source_lhe_budgets", "--source-lhe-budgets"),
    ("processing_start_index", "--processing-start-index"),
    ("max_processing_nodes", "--max-processing-nodes"),
    ("physics_campaign", "--physics-campaign"),
    ("source_rng_seeds", "--source-rng-seeds"),
    ("mixing_rng_seed", "--mixing-rng-seed"),
]:
    if cfg.get(key) not in (None, ""):
        value = cfg[key]
        if isinstance(value, (list, dict)):
            value = json.dumps(value, separators=(",", ":"))
        cmd.extend([flag, str(value)])
if cfg.get("enable_ntuple", False):
    cmd.append("--enable-ntuple")
if cfg.get("efficiency_ntuple", False):
    cmd.append("--efficiency-ntuple")
if cfg.get("cleanup", False):
    cmd.append("--cleanup")
if cfg.get("shuffle_mixing", False):
    cmd.append("--shuffle-mixing")
if isinstance(cfg.get("storage"), dict):
    cmd.extend(["--storage-config", json.dumps(cfg["storage"], separators=(",", ":"))])
if isinstance(cfg.get("processing_environment"), dict):
    cmd.extend([
        "--processing-environment-config",
        json.dumps(cfg["processing_environment"], separators=(",", ":")),
    ])
for key, flag in [
    ("target_machine", "--target-machine"),
    ("target_eos_base", "--target-eos-base"),
    ("ntuple_sub_template_path", "--ntuple-sub-template-path"),
    ("ntuple_bundle_path", "--ntuple-bundle-path"),
    ("ntuple_bundle_name", "--ntuple-bundle-name"),
    ("ntuple_wrapper_path", "--ntuple-wrapper-path"),
    ("local_output_base", "--local-output-base"),
]:
    if cfg.get(key):
        cmd.extend([flag, str(cfg[key])])

raise SystemExit(subprocess.run(cmd, check=False).returncode)
PY
then
    echo "ERROR: LHE block coordination failed" >&2
    exit 1
fi

echo "=== LHE block coordination completed successfully ==="
