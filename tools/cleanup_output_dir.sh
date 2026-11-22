#!/bin/bash
# Cleanup: remove all files under MC_Production_v3/output/
# Run somewhere with good EOS connectivity (lxplus, worker node).
#
# Usage:
#   bash tools/cleanup_output_dir.sh
#
# Also cleans the stale ntuple rebuild output if it exists.

set -euo pipefail

EOS_HOST="root://cceos.ihep.ac.cn"
OUTPUT_DIR="/store/user/chiw/MC_Production_v3/output"
NTUPLE_REBUILD_DIR="/store/user/chiw/MC_Production_v3/JpsiJpsiPhi/Ntuple-v02_00_patch2-prod_v02_00_pre2"
CAMPAIGNS=(JJP_DPS1 JJP_DPS2_CS JJP_DPS2_G JJP_SPS_CS JJP_SPS_G JJP_TPS)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

status_line() {
    # Clear line and print status
    printf '\r\033[K%s' "$1"
}

count_remaining() {
    local base="$1"
    xrdfs "${EOS_HOST}" ls -R "${base}" 2>/dev/null | grep -cE '\.(root|gz)$|/stages$' 2>/dev/null || echo 0
}

delete_dir_tree() {
    local base="$1"
    local label="$2"
    local campaign_list=("${@:3}")

    echo -e "${CYAN}=== [${label}] Scanning ${base} ...${NC}"
    local file_list
    file_list=$(mktemp /tmp/cleanup_files_XXXXXX.txt)

    xrdfs "${EOS_HOST}" ls -R "${base}" 2>/dev/null | grep -E '\.(root|gz)$|/stages$' > "${file_list}" || true

    local total
    total=$(wc -l < "${file_list}")
    if [[ "${total}" -eq 0 ]]; then
        echo -e "  ${GREEN}No files to delete.${NC}"
        rm -f "${file_list}"
        return
    fi
    echo -e "  ${total} files to delete."

    # Start deletion in background
    local delete_log
    delete_log=$(mktemp /tmp/cleanup_delete_log_XXXXXX.txt)
    xargs -P50 -I{} sh -c 'xrdfs '"${EOS_HOST}"' rm "$1" 2>/dev/null && echo "ok" || echo "fail"' _ "{}" < "${file_list}" > "${delete_log}" 2>/dev/null &
    local delete_pid=$!

    # Monitor progress
    echo ""
    local start_time
    start_time=$(date +%s)
    local remaining prev_remaining elapsed rate eta
    prev_remaining="${total}"

    while kill -0 "${delete_pid}" 2>/dev/null; do
        sleep 5
        remaining=$(count_remaining "${base}")
        local deleted=$((total - remaining))
        elapsed=$(($(date +%s) - start_time))

        if [[ "${elapsed}" -gt 0 && "${deleted}" -gt 0 ]]; then
            rate=$(awk "BEGIN {printf \"%.0f\", ${deleted}/${elapsed}}")
            if [[ "${deleted}" -gt 0 ]]; then
                eta=$(awk "BEGIN {printf \"%.0f\", ${remaining}*${elapsed}/${deleted}}")
            else
                eta="?"
            fi
        else
            rate="?"; eta="?"
        fi

        local pct
        pct=$(awk "BEGIN {printf \"%.1f\", 100*${deleted}/${total}}")
        status_line "  ${YELLOW}Deleted: ${deleted}/${total} (${pct}%) | Rate: ${rate}/s | ETA: ${eta}s | Elapsed: ${elapsed}s${NC}"
        prev_remaining="${remaining}"
    done
    wait "${delete_pid}" || true
    echo ""

    # Final count
    remaining=$(count_remaining "${base}")
    local deleted=$((total - remaining))
    elapsed=$(($(date +%s) - start_time))
    echo -e "  ${GREEN}Files deleted: ${deleted}/${total} in ${elapsed}s${NC}"

    if [[ "${remaining}" -gt 0 ]]; then
        echo -e "  ${YELLOW}Warning: ${remaining} files remain — retrying once...${NC}"
        xargs -P100 -I{} sh -c 'xrdfs '"${EOS_HOST}"' rm "$1" 2>/dev/null' _ "{}" < <(xrdfs "${EOS_HOST}" ls -R "${base}" 2>/dev/null | grep -E '\.(root|gz)$|/stages$') 2>/dev/null || true
        remaining=$(count_remaining "${base}")
        echo -e "  Remaining after retry: ${remaining}"
    fi

    # Remove empty job subdirectories
    echo ""
    echo -e "  Removing empty directories..."
    for campaign in "${campaign_list[@]}"; do
        local campaign_dir="${base}/${campaign}"
        xrdfs "${EOS_HOST}" ls "${campaign_dir}" 2>/dev/null | while read -r job_dir; do
            xrdfs "${EOS_HOST}" rmdir "${job_dir}" 2>/dev/null || true
        done || true
        xrdfs "${EOS_HOST}" rmdir "${campaign_dir}" 2>/dev/null || true
    done
    xrdfs "${EOS_HOST}" rmdir "${base}" 2>/dev/null || true
    echo -e "  ${GREEN}Directories removed.${NC}"

    rm -f "${file_list}" "${delete_log}"
}

# --- Main ---
echo "============================================"
echo -e "  ${CYAN}EOS Cleanup Script${NC}"
echo "  Started: $(date)"
echo "  Target:  ${OUTPUT_DIR}"
echo "============================================"
echo ""

# 1. Delete output/ (stale MiniAODs + old ntuples)
delete_dir_tree "${OUTPUT_DIR}" "output/" "${CAMPAIGNS[@]}"

# 2. Delete stale ntuple rebuild output if it exists
if xrdfs "${EOS_HOST}" stat "${NTUPLE_REBUILD_DIR}" &>/dev/null; then
    echo ""
    echo -e "${CYAN}=== [ntuple rebuild] Cleaning ${NTUPLE_REBUILD_DIR} ...${NC}"

    # Find subprocess directories dynamically
    sp_list=$(mktemp /tmp/cleanup_sp_XXXXXX.txt)
    xrdfs "${EOS_HOST}" ls "${NTUPLE_REBUILD_DIR}" 2>/dev/null > "${sp_list}" || true

    if [[ -s "${sp_list}" ]]; then
        # Delete files
        file_list2=$(mktemp /tmp/cleanup_files2_XXXXXX.txt)
        xrdfs "${EOS_HOST}" ls -R "${NTUPLE_REBUILD_DIR}" 2>/dev/null | grep '\.root$' > "${file_list2}" || true
        n2=$(wc -l < "${file_list2}")
        if [[ "${n2}" -gt 0 ]]; then
            echo "  ${n2} ntuple files to delete."

            start2=$(date +%s)
            xargs -P50 -I{} sh -c 'xrdfs '"${EOS_HOST}"' rm "$1" 2>/dev/null' _ "{}" < "${file_list2}" 2>/dev/null &
            pid2=$!

            while kill -0 "${pid2}" 2>/dev/null; do
                sleep 5
                rem2=$(xrdfs "${EOS_HOST}" ls -R "${NTUPLE_REBUILD_DIR}" 2>/dev/null | grep -c '\.root$' || echo 0)
                del2=$((n2 - rem2))
                el2=$(($(date +%s) - start2))
                pct2=$(awk "BEGIN {printf \"%.1f\", 100*${del2}/${n2}}")
                if [[ "${el2}" -gt 0 ]]; then
                    rate2=$(awk "BEGIN {printf \"%.0f\", ${del2}/${el2}}")
                else
                    rate2="?"
                fi
                status_line "  Deleted: ${del2}/${n2} (${pct2}%) | Rate: ${rate2}/s | Elapsed: ${el2}s"
            done
            wait "${pid2}" || true
            echo ""
        fi

        # Remove subprocess dirs
        while read -r sp_dir; do
            xrdfs "${EOS_HOST}" rmdir "${sp_dir}" 2>/dev/null || true
        done < "${sp_list}"
        xrdfs "${EOS_HOST}" rmdir "${NTUPLE_REBUILD_DIR}" 2>/dev/null || true
        echo -e "  ${GREEN}Rebuild directory removed.${NC}"

        rm -f "${file_list2}"
    fi
    rm -f "${sp_list}"
fi

# --- Verify ---
echo ""
echo "============================================"
echo -e "  ${CYAN}Verification${NC}"
echo "============================================"

if xrdfs "${EOS_HOST}" stat "${OUTPUT_DIR}" &>/dev/null; then
    echo -e "  ${RED}FAIL${NC} — ${OUTPUT_DIR} still exists"
else
    echo -e "  ${GREEN}OK${NC}   — ${OUTPUT_DIR} removed"
fi

if xrdfs "${EOS_HOST}" stat "${NTUPLE_REBUILD_DIR}" &>/dev/null; then
    echo -e "  ${YELLOW}NOTE${NC} — ${NTUPLE_REBUILD_DIR} still exists"
else
    echo -e "  ${GREEN}OK${NC}   — ${NTUPLE_REBUILD_DIR} removed"
fi

echo ""
echo "Cleanup finished at $(date)"
