# ==============================================================================
# common/paths.sh — centralized workspace-relative path definitions
# ==============================================================================
# Source this file in any shell script that needs project-local paths.
# All paths derive from the workspace root (one level above common/), so
# no hardcoded usernames or AFS home directories appear anywhere.
#
# Usage:
#   source "${SCRIPT_DIR:-$(dirname "${BASH_SOURCE[0]}")}/common/paths.sh"

# --- workspace root ------------------------------------------------
if [[ -z "${_PATHS_SH_SOURCED:-}" ]]; then
    _PATHS_SH_SOURCED=1
    _paths_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    WORKSPACE_ROOT="$(cd "${_paths_self}/.." && pwd)"
    export WORKSPACE_ROOT
fi

# --- proxy resolution ----------------------------------------------
# Resolution order: $X509_USER_PROXY → voms-proxy-info --path
# Warns when the resolved proxy lives under /tmp (Condor-inaccessible).
resolve_proxy_path() {
    local proxy=""

    if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
        proxy="${X509_USER_PROXY}"
    fi

    if [[ -z "${proxy}" ]] && command -v voms-proxy-info &>/dev/null; then
        local voms_path
        voms_path="$(voms-proxy-info --path 2>/dev/null)" || true
        if [[ -n "${voms_path}" && -f "${voms_path}" ]]; then
            proxy="${voms_path}"
        fi
    fi

    if [[ -z "${proxy}" ]]; then
        printf '[ERROR] 未找到 X509 代理文件。请设置 X509_USER_PROXY 或运行 voms-proxy-init。\n' >&2
        return 1
    fi

    if [[ "${proxy}" == /tmp/* ]]; then
        printf '[WARN] 代理 %s 位于 /tmp 下；HTCondor worker 节点可能无法访问。请将 X509_USER_PROXY 设置为持久路径（例如 ~/x509up_u$(id -u)）。\n' "${proxy}" >&2
    fi

    printf '%s\n' "${proxy}"
    return 0
}

# --- workspace-local directories -----------------------------------
DEFAULT_LOG_DIR="${WORKSPACE_ROOT}/generated/log"
DEFAULT_TEMP_DIR="/tmp/${USER:-$(id -un)}"
