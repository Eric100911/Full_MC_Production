#!/usr/bin/env bash
#
# Build the LHE-only CrystalBall `seed` sweep legs (Phase C prerequisite).
#
# Question: does the job seed change the CrystalBall physics at all? The nmc
# sweep (tools/crystalball_nmc_sweep.sh) held the seed at 100 for every leg and
# measured the nmc response. This sweep holds `nmc` and `unwevt` fixed at the
# values the user selected (nmc=2000000 -> final NCALL rung 1,280,000; unwevt
# unchanged at 2000) and varies only the seed.
#
# PREDICTION, from code reading - stated up front so the run can falsify it:
# every leg produces byte-identical *uncompressed* LHE. The seed reaches the
# worker (we assert that) and changes only filenames, log paths and the
# shuffle-split seed. Route of the seed, end to end:
#
#   dag_generator.py:3474  seed_for_pool_index = 100 + pool.seed_offset + index
#     -> LHE node config JSON "seed" + DAG VARS seed=
#   run_lhe_gen.sh         --seed str(cfg["seed"])
#   run_helac.sh:1011      MY_SEED=$2
#     -> output name sample_<pool>_<seed>.lhe[.gz], log label, log_root,
#        shuffle seed (MY_SEED*1000+37, only with --lhe-shuffle-split)
#     -> NOT the CrystalBall branch. `prepare_crystalball_addon_input` calls
#        prepare_runtime_user_inp, which substitutes preunw/unwevt/nmc/nopt/
#        nopt_step/noptlim/gener/ranhel into user.inp and never `seed`.
#        `run_config.ho` - the only place that writes `set seed = ${MY_SEED}` -
#        is produced in the non-CrystalBall `else` branch of run_helac.sh.
#   addon binary           contains ZERO occurrences of the string "seed" in any
#                          source, makefile or .inp. Its only RNG is RANDA()
#                          from src/MC_VEGAS.f90, driven by the compiled
#                          constant DSEED=1234567d0 (:44), which no input ever
#                          assigns.
#
# If the prediction holds, the 12,926-event yield measured at nmc=2000000 is the
# only sample this addon can produce, and independent-seed budget sizing is
# blocked on the Phase B RNG-seed plumbing rather than on measurement.
#
# TWO TIERS, because they answer different questions:
#   native  - seeds 100/101, taken straight from the real generated JJP_DPS1 DAG.
#             These are the only seeds the generator emits for this pool (it
#             declares pool_jpsi_CSCO_g twice, so index 0/1). This is the
#             production-realistic comparison.
#   patched - seeds 500/5000 on the same node, with `seed` rewritten in the node
#             config JSON and the DAG VARS line. This proves the seed has no
#             route into the physics *at all*, rather than only that 100==101.
#             The patch touches runtime configuration only; no HELAC source is
#             modified. Every patched leg asserts that the patched seed actually
#             reached both the config and the DAG, so "the seed arrived and
#             changed nothing" is distinguishable from "the patch was a no-op".
#
# A repeat leg (same seed, fresh directory, fresh bundles) separates "no seed
# effect" from "two independent jobs always agree".
#
# Mechanism is the same filtered single-node DAG as the nmc sweep: dag_generator
# has no LHE-only mode, and `generate`/`generate-test` always chain LHE -> PROC.
# Each leg keeps only the LHE node's JOB / CATEGORY / VARS / RETRY lines from the
# real generated DAG, so the bundles and worker command are what the generator
# produced. `generate` (not `generate-test`) keeps test_mode False, matching the
# 5,316-event baseline pilot.
#
# Each leg writes to its own v4_5_full subdirectory: output names are
# sample_pool_jpsi_CSCO_g_<seed>.lhe.gz, so a shared directory would collide.
#
# Usage:
#   tools/crystalball_seed_sweep.sh [--dry-run] [--only <slug>]
#
# This script only builds DAGs. It never submits, and it never writes to
# MC_Production_v3.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${BASE_DIR}"

CAMPAIGN="JJP_DPS1"
TARGET_ROOT="root://cceos.ihep.ac.cn:1094///store/user/chiw/JpsiJpsiPhi_MC_Production_v4_5_full/cb_seed_sweep"
OUTPUT_ROOT="${BASE_DIR}/generated/cb_seed_sweep"

# Held constant across every leg - this sweep varies the seed only.
NMC="2000000"
UNWEVT="2000"

DRY_RUN=0
ONLY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --only) ONLY="$2"; shift 2 ;;
        -h|--help) sed -n '2,66p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

# slug:node:seed
LEGS=(
    "seed100:100"
    "seed101:101"
    "seed500:500"
    "seed5000:5000"
    "seed100_repeat:100"
)

# LHE_JpsiG_CSCO_<index> is emitted with seed 100 + seed_offset(0) + index.
native_seed_for_node() {
    local node="$1"
    local index="${node##*_}"
    if ! [[ "${index}" =~ ^[0-9]+$ ]]; then
        echo "[FAIL] cannot parse pool index from node name ${node}" >&2
        return 1
    fi
    echo $((100 + index))
}

# Rewrite `seed` in the node config JSON and in the DAG VARS line. Idempotent:
# when seed == native the replacement is a no-op, so the native legs take the
# same code path as the patched ones and that path is always exercised.
patch_leg_seed() {
    local config="$1" dag="$2" node="$3" native="$4" seed="$5"
    python3 - "${config}" "${dag}" "${node}" "${native}" "${seed}" <<'PY' || return 1
import json
import re
import sys

config, dag, node, native, seed = sys.argv[1:6]

with open(dag, "r", encoding="utf-8") as handle:
    lines = handle.readlines()

prefix = f"VARS {node} "
if sum(1 for line in lines if line.startswith(prefix)) != 1:
    raise SystemExit(f"expected exactly 1 VARS line for {node} in {dag}")

for i, line in enumerate(lines):
    if not line.startswith(prefix):
        continue
    if f'seed="{native}"' not in line and f'seed="{seed}"' not in line:
        raise SystemExit(f"VARS line for {node} carries neither seed {native} nor {seed}")
    line = re.sub(rf'seed="\d+"', f'seed="{seed}"', line, count=1)
    # log_root embeds the seed as a path component; keep it consistent so the
    # Condor log/out/err for a patched leg do not masquerade as the native seed.
    line = re.sub(rf"/seed_\d+", f"/seed_{seed}", line)
    lines[i] = line

with open(dag, "w", encoding="utf-8") as handle:
    handle.writelines(lines)

with open(config, "r", encoding="utf-8") as handle:
    cfg = json.load(handle)
cfg["seed"] = int(seed)
with open(config, "w", encoding="utf-8") as handle:
    json.dump(cfg, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

build_leg() {
    local slug="$1" seed="$2"
    local leg_dir="${OUTPUT_ROOT}/${slug}"
    local raw_dag="${leg_dir}/generated.dag"
    local sweep_dag="${leg_dir}/${slug}.dag"

    # Seeds 100 and 101 are the generator's own; anything else is patched.
    local node="LHE_JpsiG_CSCO_0"
    if [[ "${seed}" == "101" ]]; then
        node="LHE_JpsiG_CSCO_1"
    fi
    local native
    native=$(native_seed_for_node "${node}") || return 1

    echo "[INFO] ${slug}: node=${node} native_seed=${native} seed=${seed}"

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
        --lhe-nmc "${NMC}" \
        --lhe-unwevt "${UNWEVT}" \
        --target-base-url "${TARGET_ROOT}/${slug}" \
        --output-dir "${leg_dir}" \
        --output generated.dag > "${leg_dir}/generate.log" 2>&1

    if [[ ! -f "${raw_dag}" ]]; then
        echo "[FAIL] ${slug}: generator produced no DAG; see ${leg_dir}/generate.log" >&2
        return 1
    fi

    # Keep only this node's lines. The generator emits JOB directly above
    # CATEGORY/VARS and then RETRY, so a single pass preserves their order.
    grep -E "^(JOB|CATEGORY|VARS|RETRY) ${node}( |$)" "${raw_dag}" > "${sweep_dag}" \
        || { echo "[FAIL] ${slug}: no lines for ${node} in ${raw_dag}" >&2; return 1; }

    local kept
    kept=$(grep -c '^JOB ' "${sweep_dag}")
    if [[ "${kept}" != "1" ]]; then
        echo "[FAIL] ${slug}: expected exactly 1 JOB line in ${sweep_dag}, found ${kept}" >&2
        return 1
    fi
    for keyword in CATEGORY VARS RETRY; do
        if ! grep -q "^${keyword} ${node}" "${sweep_dag}"; then
            echo "[FAIL] ${slug}: missing ${keyword} line in ${sweep_dag}" >&2
            return 1
        fi
    done

    local config="${leg_dir}/node_configs/lhe_generation/${node}.json"
    if [[ ! -f "${config}" ]]; then
        echo "[FAIL] ${slug}: missing LHE node config ${config}" >&2
        return 1
    fi

    if ! patch_leg_seed "${config}" "${sweep_dag}" "${node}" "${native}" "${seed}"; then
        echo "[FAIL] ${slug}: could not patch seed" >&2
        return 1
    fi

    # Every check below must be written as an explicit test: this function is
    # invoked as `build_leg ... || rc=1`, and that context disables `set -e`
    # for the whole body, so a bare failing command would be silently ignored.
    local out_dir nmc_seen unwevt_seen test_mode_seen seed_seen vars_seed
    out_dir=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['output_dir'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read output_dir from ${config}" >&2; return 1; }
    nmc_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['nmc'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read nmc from ${config}" >&2; return 1; }
    unwevt_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['unwevt'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read unwevt from ${config}" >&2; return 1; }
    test_mode_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['test_mode'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read test_mode from ${config}" >&2; return 1; }
    seed_seen=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['seed'])" "${config}") || {
        echo "[FAIL] ${slug}: could not read seed from ${config}" >&2; return 1; }

    # The seed must have reached BOTH carriers. A patched config with a stale DAG
    # would mean the worker ran with the native seed and the comparison is void.
    vars_seed=$(grep -o 'seed="[0-9]*"' "${sweep_dag}" | head -1 | grep -o '[0-9]*') || {
        echo "[FAIL] ${slug}: no seed= in the DAG VARS line" >&2; return 1; }

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
    if [[ "${nmc_seen}" != "${NMC}" || "${unwevt_seen}" != "${UNWEVT}" ]]; then
        echo "[FAIL] ${slug}: config carries nmc=${nmc_seen} unwevt=${unwevt_seen}, wanted ${NMC}/${UNWEVT}" >&2
        return 1
    fi
    if [[ "${test_mode_seen}" != "False" ]]; then
        echo "[FAIL] ${slug}: test_mode=${test_mode_seen}; the baseline pilot ran with test_mode False" >&2
        return 1
    fi
    if [[ "${seed_seen}" != "${seed}" || "${vars_seed}" != "${seed}" ]]; then
        echo "[FAIL] ${slug}: seed did not reach both carriers" >&2
        echo "         config seed=${seed_seen}  dag VARS seed=${vars_seed}  wanted ${seed}" >&2
        return 1
    fi

    echo "[OK]   ${slug} -> ${sweep_dag}"
    echo "       seed=${seed_seen} (config and DAG) nmc=${nmc_seen} unwevt=${unwevt_seen} test_mode=${test_mode_seen}"
    echo "       output_dir=${out_dir}"
}

rc=0
for leg in "${LEGS[@]}"; do
    IFS=':' read -r slug seed <<< "${leg}"
    if [[ -n "${ONLY}" && "${ONLY}" != "${slug}" ]]; then
        continue
    fi
    build_leg "${slug}" "${seed}" || rc=1
done

if [[ "${DRY_RUN}" -eq 0 && "${rc}" -eq 0 ]]; then
    echo
    echo "[INFO] Submit each leg with:"
    for leg in "${LEGS[@]}"; do
        IFS=':' read -r slug _ <<< "${leg}"
        [[ -n "${ONLY}" && "${ONLY}" != "${slug}" ]] && continue
        echo "       condor_submit_dag ${OUTPUT_ROOT}/${slug}/${slug}.dag"
    done
fi
exit "${rc}"
