#!/usr/bin/env bash

# Periodic, read-only monitor for explicitly named HELAC-Onia Condor jobs.
# Use exact schedd:cluster.proc identities; cluster IDs are schedd-local.

set -euo pipefail

INTERVAL_SECONDS=3600
QUERY_TIMEOUT_SECONDS=120
# -limit/-match bound only the displayed jobs; without -scanlimit the schedd
# still walks its whole history.  Keep the scan bounded so a missing job cannot
# occupy a shared schedd for minutes.
HISTORY_SCAN_LIMIT=10000
LOG_FILE="${HELAC_TRIAL_MONITOR_LOG:-/afs/cern.ch/user/c/chiw/condor/Full_MC_Production/generated/helac_trial_monitor.log}"
RUN_ONCE=0
declare -a JOB_SPECS=()
declare -A JOBS_BY_SCHEDD=()

usage() {
    cat <<'EOF'
Usage: monitor_helac_trials.sh [--once] [--interval SECONDS] [--log-file PATH] \
  --job SCHEDD:CLUSTER.PROC [--job SCHEDD:CLUSTER.PROC ...]

Writes read-only queue snapshots. When a specified job is no longer live, it
queries the recorded schedd's history once in that snapshot.
EOF
}

timestamp() {
    date '+%Y-%m-%dT%H:%M:%S%z'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --once) RUN_ONCE=1; shift ;;
        --interval) INTERVAL_SECONDS="$2"; shift 2 ;;
        --log-file) LOG_FILE="$2"; shift 2 ;;
        --job) JOB_SPECS+=("$2"); shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 64 ;;
    esac
done

(( ${#JOB_SPECS[@]} > 0 )) || {
    echo "At least one --job SCHEDD:CLUSTER.PROC is required" >&2
    usage >&2
    exit 64
}
[[ "$INTERVAL_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
    echo "--interval must be a positive integer" >&2
    exit 64
}

for spec in "${JOB_SPECS[@]}"; do
    [[ "$spec" == *:* ]] || {
        echo "Invalid --job (expected SCHEDD:CLUSTER.PROC): $spec" >&2
        exit 64
    }
    schedd="${spec%%:*}"
    job="${spec#*:}"
    [[ -n "$schedd" && "$job" == *.* ]] || {
        echo "Invalid --job (expected SCHEDD:CLUSTER.PROC): $spec" >&2
        exit 64
    }
    JOBS_BY_SCHEDD["$schedd"]+=" $job"
done

mkdir -p "$(dirname "$LOG_FILE")"
exec >>"$LOG_FILE" 2>&1

history_for_missing_job() {
    local schedd="$1"
    local job="$2"
    local history=""

    history=$(timeout --signal=TERM "${QUERY_TIMEOUT_SECONDS}s" \
        condor_history -name "$schedd" "$job" \
        -scanlimit "$HISTORY_SCAN_LIMIT" -af \
        JobStatus ExitCode ExitBySignal CompletionDate RemoveReason HoldReason 2>&1) || true
    if [[ -n "$history" ]]; then
        printf '[%s] history schedd=%s job=%s %s\n' \
            "$(timestamp)" "$schedd" "$job" "$history"
    else
        printf '[%s] no live or history record within %s scanned records schedd=%s job=%s\n' \
            "$(timestamp)" "$HISTORY_SCAN_LIMIT" "$schedd" "$job"
    fi
}

snapshot_schedd() {
    local schedd="$1"
    shift
    local -a jobs=("$@")
    local output=""

    printf '[%s] schedd=%s jobs=%s\n' "$(timestamp)" "$schedd" "${jobs[*]}"
    if ! output=$(timeout --signal=TERM "${QUERY_TIMEOUT_SECONDS}s" \
        condor_q -name "$schedd" "${jobs[@]}" -nobatch -autoformat \
        ClusterId ProcId JobStatus NumJobStarts LastRemoteHost \
        RemoteWallClockTime MemoryUsage HoldReason JobFlavour MaxRuntime 2>&1); then
        printf '[%s] condor_q failed schedd=%s; history is not queried\n' \
            "$(timestamp)" "$schedd"
        [[ -n "$output" ]] && printf '%s\n' "$output"
        return
    fi
    [[ -n "$output" ]] && printf '%s\n' "$output"

    local job cluster proc
    for job in "${jobs[@]}"; do
        cluster="${job%%.*}"
        proc="${job#*.}"
        if ! awk -v cluster="$cluster" -v proc="$proc" \
            '$1 == cluster && $2 == proc { found = 1 } END { exit !found }' \
            <<<"$output"; then
            history_for_missing_job "$schedd" "$job"
        fi
    done
    printf '[%s] completed schedd=%s snapshot\n' "$(timestamp)" "$schedd"
}

monitor_once() {
    printf '\n===== HELAC trial snapshot =====\n'
    local schedd
    local -a jobs=()
    for schedd in "${!JOBS_BY_SCHEDD[@]}"; do
        read -r -a jobs <<<"${JOBS_BY_SCHEDD[$schedd]}"
        snapshot_schedd "$schedd" "${jobs[@]}"
    done
    printf '[%s] completed trial snapshot\n' "$(timestamp)"
}

trap 'printf "[%s] monitor stopped\n" "$(timestamp)"' EXIT
printf '[%s] monitor started; interval=%ss\n' "$(timestamp)" "$INTERVAL_SECONDS"

while true; do
    monitor_once
    (( RUN_ONCE == 1 )) && break
    sleep "$INTERVAL_SECONDS"
done
