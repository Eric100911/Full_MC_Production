#!/bin/bash
# ==============================================================================
# compression_helpers.sh - Compression-aware file handling for worker scripts
# ==============================================================================
# Source this file in worker scripts that need transparent .lhe.gz / .hepmc.gz
# support.  Provides suffix detection, atomic decompression, and pool listing
# helpers that accept both compressed and uncompressed extensions.
# ==============================================================================

set -e

# ── Suffix helpers ───────────────────────────────────────────────────────────

is_gz_file() {
    [[ "$1" == *.gz ]]
}

strip_gz_suffix() {
    local path="$1"
    echo "${path%.gz}"
}

# ── Extension acceptance ─────────────────────────────────────────────────────

# True (0) if the filename ends with .lhe or .lhe.gz
accepts_lhe_ext() {
    local name="$1"
    [[ "$name" == *.lhe || "$name" == *.lhe.gz ]]
}

# True (0) if the filename ends with .hepmc or .hepmc.gz
accepts_hepmc_ext() {
    local name="$1"
    [[ "$name" == *.hepmc || "$name" == *.hepmc.gz ]]
}

# ── Decompression ────────────────────────────────────────────────────────────

# If $src is a .gz file, atomically decompress to $dst (write .tmp then rename).
# If $src is uncompressed and different from $dst, copy it.
# If $dst already exists, skip.
decompress_if_needed() {
    local src="$1"
    local dst="$2"

    if [[ -f "$dst" ]]; then
        return 0
    fi

    if is_gz_file "$src"; then
        gunzip -c "$src" > "${dst}.tmp" && mv "${dst}.tmp" "$dst"
    else
        if [[ "$src" != "$dst" ]]; then
            cp "$src" "$dst"
        fi
    fi
}

# ── Pool listing ─────────────────────────────────────────────────────────────

# List LHE files (compressed or not) in a local directory, sorted.
# Usage:  list_lhe_pool_files_local <dir>
list_lhe_pool_files_local() {
    local dir="$1"
    find "$dir" -type f \( -name "*.lhe" -o -name "*.lhe.gz" \) 2>/dev/null | sort
}

# ── Event counting ───────────────────────────────────────────────────────────

# Count <event> lines in an LHE file, transparently handling .gz input.
count_lhe_events() {
    local file="$1"
    if is_gz_file "$file"; then
        gunzip -c "$file" | grep -c "<event>" || echo "0"
    else
        grep -c "<event>" "$file" || echo "0"
    fi
}
