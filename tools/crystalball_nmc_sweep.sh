#!/usr/bin/env bash
#
# Build the LHE-only CrystalBall `nmc` / `unwevt` sweep legs.
#
# Source plan: docs/lhe_output_location_and_crystalball_nmc_2026-09-18.md, Item 2
# Phase A. The sweep measures the deterministic response of the CrystalBall addon
# to `nmc` while holding the compiled addon RNG stream (MC_VEGAS:DSEED =
# 1234567d0) fixed. `unwevt` is swept too as a control: it becomes `nwri_tot` in
# addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90 and is never tested as a stop
# condition, so those legs are expected to reproduce the `unwevt=2000` legs
# byte-for-byte.
#
# Why a filtered single-node DAG: dag_generator has no LHE-only mode, and
# `generate`/`generate-test` always chain LHE -> PROC. Phase A needs the LHE
# output only. Each leg therefore keeps just the LHE node's own JOB / CATEGORY /
# VARS / RETRY lines from the real generated DAG, so the node config, bundles and
# worker command are exactly what the generator produced.
#
# Why `generate` and not `generate-test`: generate-test sets `test_mode`, which
# the worker turns into FAST_TEST. With both --nmc and --unwevt given explicitly
# that only changes an informational echo today, but the pilot that produced the
# 5,316-event baseline ran with test_mode False, and a measurement whose config
# differs from its own baseline is not worth the argument. The harness asserts
# test_mode is False for every leg.
#
# Only ONE LHE node per leg is kept. JJP_DPS1 declares pool_jpsi_CSCO_g twice, so
# the generator emits seed 100 and seed 101. Because the addon is not seeded from
# MY_SEED (plan Item 2), those two would be byte-identical copies; running both
# would double the CPU for no extra physics.
#
# Every leg writes to its own v4_5_full subdirectory: the sample name
# (sample_pool_jpsi_CSCO_g_100.lhe.gz) is identical across legs, so a shared
# directory would let each leg overwrite the previous one.
#
# Usage:
#   tools/crystalball_nmc_sweep.sh [--dry-run] [--only <slug>]
#
# This script only builds DAGs. It never submits, and it never writes to
# MC_Production_v3.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${BASE_DIR}"

CAMPAIGN="JJP_DPS1"
POOL_NODE="LHE_JpsiG_CSCO_0"   # seed 100; see the header note on seed 101
TARGET_ROOT="root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_5_full/cb_nmc_sweep"
OUTPUT_ROOT="${BASE_DIR}/generated/cb_nmc_sweep"

DRY_RUN=0
ONLY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --only) ONLY="$2"; shift 2 ;;
        -h|--help) sed -n '2,32p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

# slug:nmc:unwevt
LEGS=(
    "nmc250k_unwevt2k:250000:2000"
    "nmc500k_unwevt2k:500000:2000"
    "nmc1000k_unwevt2k:1000000:2000"
    "nmc2000k_unwevt2k:2000000:2000"
    "nmc500k_unwevt10k:500000:10000"
    "nmc500k_unwevt100k:500000:100000"
    "nmc500k_unwevt2k_repeat:500000:2000"
)

build_leg() {
    local slug="$1" nmc="$2" unwevt="$3"
    local leg_dir="${OUTPUT_ROOT}/${slug}"
    local raw_dag="${leg_dir}/generated.dag"
    local sweep_dag="${leg_dir}/${slug}.dag"

    echo "[INFO] ${slug}: nmc=${nmc} unwevt=${unwevt}"
    if [[ "${DRY_RUN}" -eq 1 ]]; then
        echo "       would write ${sweep_dag}"
        return 0
    fi

    rm -rf "${leg_dir}"
    mkdir -p "${leg_dir}"
    python3 dag_generator.py generate \
        --campaign "${CAMPAIGN}" \
        --jobs 1 \
        --machine-env lxplus_t2_ihep \
        --no-scan-existing \
        --compress-lhe \
        --lhe-compression-level 1 \
        --lhe-nmc "${nmc}" \
        --lhe-unwevt "${unwevt}" \
        --target-base-url "${TARGET_ROOT}/${slug}" \
        --output-dir "${leg_dir}" \
        --output generated.dag > "${leg_dir}/generate.log" 2>&1

    if [[ ! -f "${raw_dag}" ]]; then
        echo "[FAIL] ${slug}: generator produced no DAG; see ${leg_dir}/generate.log" >&2
        return 1
    fi

    # Keep only this node's lines. The generator emits JOB directly above
    # CATEGORY/VARS and then RETRY, so a single pass preserves their order.
    grep -E "^(JOB|CATEGORY|VARS|RETRY) ${POOL_NODE}( |$)" "${raw_dag}" > "${sweep_dag}" \
        || { echo "[FAIL] ${slug}: no lines for ${POOL_NODE} in ${raw_dag}" >&2; return 1; }

    local kept
    kept=$(grep -c '^JOB ' "${sweep_dag}")
    if [[ "${kept}" != "1" ]]; then
        echo "[FAIL] ${slug}: expected exactly 1 JOB line in ${sweep_dag}, found ${kept}" >&2
        return 1
    fi
    for keyword in CATEGORY VARS RETRY; do
        if ! grep -q "^${keyword} ${POOL_NODE}" "${sweep_dag}"; then
            echo "[FAIL] ${slug}: missing ${keyword} line in ${sweep_dag}" >&2
            return 1
        fi
    done

    # Every check below must be written as an explicit test: this function is
    # invoked as `build_leg ... || rc=1`, and that context disables `set -e`
    # for the whole body, so a bare failing command would be silently ignored.
    local config="${leg_dir}/node_configs/lhe_generation/${POOL_NODE}.json"
    if [[ ! -f "${config}" ]]; then
        echo "[FAIL] ${slug}: missing LHE node config ${config}" >&2
        return 1
    fi

    # The emitted output_dir must be exactly this leg's own v4_5_full directory.
    # This is the guard that keeps a sweep leg from ever reaching the inventory.
    local out_dir nmc_seen unwevt_seen
    out_dir=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['output_dir'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read output_dir from ${config}" >&2; return 1; }
    nmc_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['nmc'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read nmc from ${config}" >&2; return 1; }
    unwevt_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['unwevt'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read unwevt from ${config}" >&2; return 1; }
    test_mode_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['test_mode'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read test_mode from ${config}" >&2; return 1; }

    local want="${TARGET_ROOT}/${slug}/lhe_pools/pool_jpsi_CSCO_g"
    if [[ "${out_dir}" != "${want}" ]]; then
        echo "[FAIL] ${slug}: output_dir mismatch" >&2
        echo "         got  ${out_dir}" >&2
        echo "         want ${want}" >&2
        return 1
    fi
    if [[ "${out_dir}" == *"MC_Production_v3"* ]]; then
        echo "[FAIL] ${slug}: output_dir resolves into v3: ${out_dir}" >&2
        return 1
    fi
    if [[ "${nmc_seen}" != "${nmc}" || "${unwevt_seen}" != "${unwevt}" ]]; then
        echo "[FAIL] ${slug}: config carries nmc=${nmc_seen} unwevt=${unwevt_seen}, wanted ${nmc}/${unwevt}" >&2
        return 1
    fi
    if [[ "${test_mode_seen}" != "False" ]]; then
        echo "[FAIL] ${slug}: test_mode=${test_mode_seen}; the 5,316-event baseline ran with test_mode False" >&2
        return 1
    fi

    echo "[OK]   ${slug} -> ${sweep_dag}"
    echo "       nmc=${nmc_seen} unwevt=${unwevt_seen} test_mode=${test_mode_seen}"
    echo "       output_dir=${out_dir}"
}

rc=0
for leg in "${LEGS[@]}"; do
    IFS=':' read -r slug nmc unwevt <<< "${leg}"
    if [[ -n "${ONLY}" && "${ONLY}" != "${slug}" ]]; then
        continue
    fi
    build_leg "${slug}" "${nmc}" "${unwevt}" || rc=1
done

if [[ "${DRY_RUN}" -eq 0 && "${rc}" -eq 0 ]]; then
    echo
    echo "[INFO] Submit each leg with:"
    for leg in "${LEGS[@]}"; do
        IFS=':' read -r slug _ _ <<< "${leg}"
        [[ -n "${ONLY}" && "${ONLY}" != "${slug}" ]] && continue
        echo "       condor_submit_dag ${OUTPUT_ROOT}/${slug}/${slug}.dag"
    done
fi
exit "${rc}"
