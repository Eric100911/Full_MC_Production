#!/usr/bin/env python3
"""
HepJob (hep_sub) 工作流生成器 — ihep/lxlogin 集群适配版。

将 dag_generator.py 中的 campaign/pool 定义转化为 hep_sub 可提交的
bash 作业脚本，并提供自动化编排器（提交 LHE → 等待 → 提交 processing）。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tarfile
from collections import OrderedDict
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

from common.compression_util import accepts_lhe_ext
from dag_generator import (
    BASE_DIR,
    BUNDLE_NAMES,
    CAMPAIGNS,
    EOS_BASE,
    EOS_HOST,
    EOS_PATH_BASE,
    LHE_POOLS,
    POOL_DAG_LABELS,
    build_bundle,
    build_proxy_bundle,
    build_ntuple_manifest,
    canonical_mode,
    compute_pool_requirements,
    detect_proxy_path,
    discover_ntuple_jobs,
    ensure_dir,
    expand_campaign_selection,
    pool_storage_name,
    prepare_ntuple_only_assets,
    prepare_runtime_assets,
    real_pool_names,
    scan_existing_pools,
    validate_efficiency_campaigns,
    write_ntuple_manifest,
)

WORKFS2_BASE = "/workfs2/cms/chengxing/Full_MC_Production"
SCRATCHFS_BASE = "/scratchfs/cms/chengxing"
WORKFLOW_BASE = os.path.join(SCRATCHFS_BASE, "hepjob")
LOG_BASE = os.path.join(WORKFLOW_BASE, "logs")

CONTAINER_IMAGES = {
    "el7": "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el7:x86_64",
    "el8": "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el8:x86_64",
    "el9": "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el9:x86_64",
}

CMSSW_PATHS = {
    "12": "/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw/CMSSW_12_4_14",
    "15": "/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_15_0_15",
}


def bool_str(value: bool) -> str:
    return "true" if value else "false"


def seed_for_pool_index(pool_name: str, index: int) -> int:
    pool = LHE_POOLS[pool_name]
    seed = 100 + pool.seed_offset + index
    if seed >= 100000:
        seed = 110 + (seed % 80000)
    return seed


def generate_lhe_job_script(
    pool_name: str,
    index: int,
    seed: int,
    unwevt: int,
    test_mode: bool,
    bundle_dir: str,
    lhe_bundle_name: str,
    proxy_bundle_name: str,
    compress_lhe: bool = False,
    lhe_compression_level: int = 1,
) -> str:
    """生成单个 LHE 生成作业的 bash 脚本内容。
    使用 cmssw-el7 容器运行 HELAC-Onia（提供 gfortran + LCG 环境）。
    """

    pool = LHE_POOLS[pool_name]
    test_str = bool_str(test_mode)
    work_dir = f"{bundle_dir}/lhe_{pool_name}_{index}"
    proxy_path = f"{work_dir}/credentials/x509_user_proxy"
    compress_args = ""
    if compress_lhe:
        compress_args = f" --compress-lhe --lhe-compression-level {lhe_compression_level}"

    return _LHE_SCRIPT_TEMPLATE.format(
        pool_name=pool_name,
        index=index,
        seed=seed,
        bundle_dir=bundle_dir,
        lhe_bundle_name=lhe_bundle_name,
        proxy_bundle_name=proxy_bundle_name,
        min_pt_conia=pool.min_pt_conia,
        min_pt_bonia=pool.min_pt_bonia,
        min_pt_q=pool.min_pt_q,
        unwevt=unwevt,
        test_str=test_str,
        work_dir=work_dir,
        proxy_path=proxy_path,
        compress_args=compress_args,
    )


_LHE_SCRIPT_TEMPLATE = """#!/bin/bash
set -euo pipefail
# HepJob LHE generation: {pool_name} index={index} seed={seed}

echo "=== LHE job start: {pool_name} seed={seed} ==="
echo "Host: $(hostname)"
echo "Date: $(date)"

BUNDLE_DIR="{bundle_dir}"
WORK_DIR="{work_dir}"
rm -rf "${{WORK_DIR}}" 2>/dev/null || true
mkdir -p "${{WORK_DIR}}"
cd "${{WORK_DIR}}"

echo "Extracting bundles..."
tar -xzf "${{BUNDLE_DIR}}/{proxy_bundle_name}"
tar -xzf "${{BUNDLE_DIR}}/{lhe_bundle_name}"

PROXY_FILE="{proxy_path}"
chmod 600 "${{PROXY_FILE}}"

# Write inner command as a script to avoid cmssw-el7 quoting issues
cat > "${{WORK_DIR}}/run_inside.sh" << INNER_EOF
#!/bin/bash
export X509_USER_PROXY={proxy_path}
cd {work_dir}/runtime/lhe_generation
if [ -f Makefile ]; then
    make clean 2>/dev/null || true
    make || echo 'Warning: make failed, using pre-built converter'
fi
bash run_helac.sh \\
    --pool {pool_name} \\
    --seed {seed} \\
    --min-pt-conia {min_pt_conia} \\
    --min-pt-bonia {min_pt_bonia} \\
    --min-pt-q {min_pt_q} \\
    --unwevt {unwevt} \\
    --test-mode {test_str}{compress_args}
INNER_EOF
chmod +x "${{WORK_DIR}}/run_inside.sh"

echo "Launching cmssw-el7 container..."
cmssw-el7 -B /workfs2 -B /scratchfs -- "${{WORK_DIR}}/run_inside.sh" \
    > "${{WORK_DIR}}/container_stdout.log" 2> "${{WORK_DIR}}/container_stderr.log"
CONTAINER_RC=$?

echo "Container exit code: ${{CONTAINER_RC}}"
echo "=== Container stdout (last 30 lines) ==="
tail -n 30 "${{WORK_DIR}}/container_stdout.log" 2>/dev/null || true
echo "=== Container stderr (last 30 lines) ==="
tail -n 30 "${{WORK_DIR}}/container_stderr.log" 2>/dev/null || true

if [ ${{CONTAINER_RC}} -ne 0 ]; then
    echo "ERROR: Container exited with code ${{CONTAINER_RC}}"
    exit ${{CONTAINER_RC}}
fi

echo "=== LHE job done: {pool_name} seed={seed} ==="
"""


def generate_processing_job_script(
    campaign_name: str,
    job_index: int,
    input_specs: str,
    modes: str,
    analysis: str,
    n_sources: int,
    max_events: int,
    enable_ntuple: bool,
    efficiency_ntuple: bool,
    shuffle_mixing: bool,
    cleanup: bool,
    bundle_dir: str,
    proc_bundle_name: str,
    proxy_bundle_name: str,
) -> str:
    """生成单个 processing 作业的 bash 脚本内容。
    使用 cmssw-el8 容器运行 processing chain（提供正确的 xrdfs 等工具）。
    """

    enable_ntuple_str = bool_str(enable_ntuple)
    efficiency_ntuple_str = bool_str(efficiency_ntuple)
    shuffle_mixing_str = bool_str(shuffle_mixing)
    cleanup_str = bool_str(cleanup)
    work_dir = f"{bundle_dir}/proc_{campaign_name}_{job_index}"
    proxy_path = f"{work_dir}/credentials/x509_user_proxy"

    return _PROC_SCRIPT_TEMPLATE.format(
        campaign_name=campaign_name,
        job_index=job_index,
        input_specs=input_specs,
        modes=modes,
        analysis=analysis,
        max_events=max_events,
        enable_ntuple_str=enable_ntuple_str,
        efficiency_ntuple_str=efficiency_ntuple_str,
        shuffle_mixing_str=shuffle_mixing_str,
        cleanup_str=cleanup_str,
        bundle_dir=bundle_dir,
        proc_bundle_name=proc_bundle_name,
        proxy_bundle_name=proxy_bundle_name,
        work_dir=work_dir,
        proxy_path=proxy_path,
    )


_PROC_SCRIPT_TEMPLATE = """#!/bin/bash
set -euo pipefail
# HepJob Processing: {campaign_name} job={job_index}

echo "=== Processing job start: {campaign_name} job={job_index} ==="
echo "Host: $(hostname)"
echo "Date: $(date)"

BUNDLE_DIR="{bundle_dir}"
WORK_DIR="{work_dir}"
rm -rf "${{WORK_DIR}}" 2>/dev/null || true
mkdir -p "${{WORK_DIR}}"
cd "${{WORK_DIR}}"

echo "Extracting bundles..."
tar -xzf "${{BUNDLE_DIR}}/{proxy_bundle_name}"
tar -xzf "${{BUNDLE_DIR}}/{proc_bundle_name}"

PROXY_FILE="{proxy_path}"
chmod 600 "${{PROXY_FILE}}"

# Write inner command as a script to avoid cmssw-el8 quoting issues
cat > "${{WORK_DIR}}/run_inside.sh" << INNER_EOF
#!/bin/bash
# Sanitize environment: remove host CMSSW PATH/LD_LIBRARY_PATH
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/cvmfs/cms.cern.ch/common
unset LD_LIBRARY_PATH
export X509_USER_PROXY={proxy_path}
cd {work_dir}/runtime/processing
export WORKDIR=\$(mktemp -d /tmp/hepjob_proc_XXXXXX)
bash run_chain.sh \\
    --workdir "\${{WORKDIR}}" \\
    --inputs '{input_specs}' \\
    --modes '{modes}' \\
    --analysis {analysis} \\
    --campaign {campaign_name} \\
    --job-id {job_index} \\
    --max-events {max_events} \\
    --enable-ntuple {enable_ntuple_str} \\
    --efficiency-ntuple {efficiency_ntuple_str} \\
    --shuffle-mixing {shuffle_mixing_str} \\
    --cleanup {cleanup_str}
EC=\$?
rm -rf "\${{WORKDIR}}" 2>/dev/null || true
exit \$EC
INNER_EOF
chmod +x "${{WORK_DIR}}/run_inside.sh"

echo "Launching cmssw-el8 container..."
cmssw-el8 -B /workfs2 -B /scratchfs -- "${{WORK_DIR}}/run_inside.sh" \
    > "${{WORK_DIR}}/container_stdout.log" 2> "${{WORK_DIR}}/container_stderr.log"
CONTAINER_RC=$?

echo "Container exit code: ${{CONTAINER_RC}}"
echo "=== Container stdout (last 30 lines) ==="
tail -n 30 "${{WORK_DIR}}/container_stdout.log" 2>/dev/null || true
echo "=== Container stderr (last 30 lines) ==="
tail -n 30 "${{WORK_DIR}}/container_stderr.log" 2>/dev/null || true

if [ ${{CONTAINER_RC}} -ne 0 ]; then
    echo "ERROR: Container exited with code ${{CONTAINER_RC}}"
    exit ${{CONTAINER_RC}}
fi

echo "=== Processing job done: {campaign_name} job={job_index} ==="
"""


def generate_ntuple_only_job_script(
    campaign_name: str,
    job_index: int,
    analysis: str,
    miniaod_input: str,
    max_events: int,
    efficiency_ntuple: bool,
    cleanup: bool,
    bundle_dir: str,
    ntuple_bundle_name: str,
    proxy_bundle_name: str,
) -> str:
    """生成单个 ntuple-only 作业的 bash 脚本内容。

    使用 cmssw-el9 容器单独运行 ntuple 步骤（CMSSW_15 需要 el9）。
    """
    efficiency_ntuple_str = bool_str(efficiency_ntuple)
    cleanup_str = bool_str(cleanup)
    work_dir = f"{bundle_dir}/ntuple_{campaign_name}_{job_index}"
    proxy_path = f"{work_dir}/credentials/x509_user_proxy"

    return _NTUPLE_ONLY_SCRIPT_TEMPLATE.format(
        campaign_name=campaign_name,
        job_index=job_index,
        analysis=analysis,
        miniaod_input=miniaod_input,
        max_events=max_events,
        efficiency_ntuple_str=efficiency_ntuple_str,
        cleanup_str=cleanup_str,
        bundle_dir=bundle_dir,
        ntuple_bundle_name=ntuple_bundle_name,
        proxy_bundle_name=proxy_bundle_name,
        work_dir=work_dir,
        proxy_path=proxy_path,
    )


_NTUPLE_ONLY_SCRIPT_TEMPLATE = """#!/bin/bash
set -euo pipefail
# HepJob Ntuple-only: {campaign_name} job={job_index}

echo "=== Ntuple-only job start: {campaign_name} job={job_index} ==="
echo "Host: $(hostname)"
echo "Date: $(date)"

BUNDLE_DIR="{bundle_dir}"
WORK_DIR="{work_dir}"
rm -rf "${{WORK_DIR}}" 2>/dev/null || true
mkdir -p "${{WORK_DIR}}"
cd "${{WORK_DIR}}"

echo "Extracting bundles..."
tar -xzf "${{BUNDLE_DIR}}/{proxy_bundle_name}"
tar -xzf "${{BUNDLE_DIR}}/{ntuple_bundle_name}"

PROXY_FILE="{proxy_path}"
chmod 600 "${{PROXY_FILE}}"

# Write inner command as a script to avoid cmssw-el9 quoting issues
cat > "${{WORK_DIR}}/run_inside.sh" << INNER_EOF
#!/bin/bash
# Sanitize environment: remove host CMSSW PATH/LD_LIBRARY_PATH
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/cvmfs/cms.cern.ch/common
unset LD_LIBRARY_PATH
export X509_USER_PROXY={proxy_path}
cd {work_dir}/runtime/processing
export WORKDIR=\$(mktemp -d /tmp/hepjob_ntuple_XXXXXX)
bash run_chain.sh \\
    --workdir "\${{WORKDIR}}" \\
    --inputs file:/dev/null \\
    --modes normal \\
    --analysis {analysis} \\
    --campaign {campaign_name} \\
    --job-id {job_index} \\
    --max-events {max_events} \\
    --enable-ntuple true \\
    --efficiency-ntuple {efficiency_ntuple_str} \\
    --cleanup {cleanup_str} \\
    --skip-to ntuple \\
    --miniaod-input {miniaod_input} \\
    --transfer-miniaod false
EC=\$?
rm -rf "\${{WORKDIR}}" 2>/dev/null || true
exit \$EC
INNER_EOF
chmod +x "${{WORK_DIR}}/run_inside.sh"

echo "Launching cmssw-el9 container..."
cmssw-el9 -B /workfs2 -B /scratchfs -- "${{WORK_DIR}}/run_inside.sh" \
    > "${{WORK_DIR}}/container_stdout.log" 2> "${{WORK_DIR}}/container_stderr.log"
CONTAINER_RC=$?

echo "Container exit code: ${{CONTAINER_RC}}"
echo "=== Container stdout (last 30 lines) ==="
tail -n 30 "${{WORK_DIR}}/container_stdout.log" 2>/dev/null || true
echo "=== Container stderr (last 30 lines) ==="
tail -n 30 "${{WORK_DIR}}/container_stderr.log" 2>/dev/null || true

if [ ${{CONTAINER_RC}} -ne 0 ]; then
    echo "ERROR: Container exited with code ${{CONTAINER_RC}}"
    exit ${{CONTAINER_RC}}
fi

echo "=== Ntuple-only job done: {campaign_name} job={job_index} ==="
"""


def write_job_script(path: str, content: str) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)
    os.chmod(path, 0o755)


def generate_submit_lhe_script(
    job_scripts: List[str],
    job_names: List[str],
    logs_dir: str,
    bundle_dir: str,
    lhe_bundle_name: str,
    proxy_bundle_name: str,
    hep_group: str = "cms",
    walltime: str = "test",
) -> str:
    """生成提交所有 LHE 作业的 bash 脚本。"""

    lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        f"# Auto-generated LHE job submitter for HepJob",
        f"# Generated: {datetime.now().isoformat()}",
        "",
        'source ~/.bashrc 2>/dev/null || true',
        f'LOG_DIR="{logs_dir}/lhe"',
        "mkdir -p \"${LOG_DIR}\"",
        "",
        "echo \"=== Submitting LHE jobs ===\"",
        "JOB_IDS=()",
        "",
    ]

    for script_path, job_name in zip(job_scripts, job_names):
        lines.append(f"# {job_name}")
        lines.append(
            f"OUTPUT=$(hep_sub -g {hep_group} -gwn CMS -wt {walltime} -n 1 "
            f'-o "${{LOG_DIR}}/{job_name}.out" '
            f'-e "${{LOG_DIR}}/{job_name}.err" '
            f'"{script_path}" 2>&1)'
        )
        lines.append(f'echo "  {job_name}: ${{OUTPUT}}"')
        lines.append('# Extract cluster ID')
        lines.append(
            'CLUSTER_ID=$(echo "${OUTPUT}" | grep -oP "cluster \K\d+")'
        )
        lines.append('JOB_IDS+=("${CLUSTER_ID:-unknown}")')
        lines.append("")

    lines.append('echo "=== All LHE jobs submitted: ${#JOB_IDS[@]} jobs ==="')
    lines.append('echo "Job IDs: ${JOB_IDS[*]}"')
    lines.append("")
    lines.append('# Save job IDs for orchestrator')
    lines.append(
        'echo "${JOB_IDS[@]}" > "${LOG_DIR}/lhe_job_ids.txt"'
    )

    return "\n".join(lines) + "\n"


def generate_submit_processing_script(
    job_scripts: List[str],
    job_names: List[str],
    logs_dir: str,
    bundle_dir: str,
    proc_bundle_name: str,
    proxy_bundle_name: str,
    hep_group: str = "cms",
    walltime: str = "test",
) -> str:
    """生成提交所有 processing 作业的 bash 脚本。"""

    lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        f"# Auto-generated processing job submitter for HepJob",
        f"# Generated: {datetime.now().isoformat()}",
        "",
        'source ~/.bashrc 2>/dev/null || true',
        f'LOG_DIR="{logs_dir}/processing"',
        "mkdir -p \"${LOG_DIR}\"",
        "",
        "echo \"=== Submitting processing jobs ===\"",
        "JOB_IDS=()",
        "",
    ]

    for script_path, job_name in zip(job_scripts, job_names):
        lines.append(f"# {job_name}")
        lines.append(
            f"OUTPUT=$(hep_sub -g {hep_group} -gwn CMS -wt {walltime} -n 1 "
            f'-o "${{LOG_DIR}}/{job_name}.out" '
            f'-e "${{LOG_DIR}}/{job_name}.err" '
            f'"{script_path}" 2>&1)'
        )
        lines.append(f'echo "  {job_name}: ${{OUTPUT}}"')
        lines.append('# Extract cluster ID')
        lines.append(
            'CLUSTER_ID=$(echo "${OUTPUT}" | grep -oP "cluster \K\d+")'
        )
        lines.append('JOB_IDS+=("${CLUSTER_ID:-unknown}")')
        lines.append("")

    lines.append('echo "=== All processing jobs submitted: ${#JOB_IDS[@]} jobs ==="')
    lines.append('echo "Job IDs: ${JOB_IDS[*]}"')
    lines.append("")
    lines.append(
        'echo "${JOB_IDS[@]}" > "${LOG_DIR}/processing_job_ids.txt"'
    )

    return "\n".join(lines) + "\n"


def generate_orchestrator_script(
    submit_lhe_path: str,
    submit_proc_path: str,
    logs_dir: str,
) -> str:
    """生成完整工作流编排器：提交 LHE → 等待 → 提交 processing。"""

    script = f'''#!/bin/bash
set -euo pipefail
# HepJob workflow orchestrator
# Generated: {datetime.now().isoformat()}

source ~/.bashrc 2>/dev/null || true

LOG_DIR="{logs_dir}"
mkdir -p "${{LOG_DIR}}"

echo "============================================"
echo "HepJob MC Production Workflow Orchestrator"
echo "============================================"
echo "Start: $(date)"

# Phase 1: Submit LHE jobs
echo ""
echo "=== Phase 1: Submitting LHE generation jobs ==="
bash "{submit_lhe_path}"

# Phase 2: Wait for LHE jobs
echo ""
echo "=== Phase 2: Waiting for LHE jobs to complete ==="
MAX_WAIT=7200  # 2 hour timeout
WAIT_INTERVAL=60
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    source ~/.bashrc 2>/dev/null || true
    ACTIVE=$(hep_q -u chengxing 2>/dev/null | tail -1 | grep -oP '\d+ idle' | grep -oP '\d+' || echo "0")

    if [ "${{ACTIVE:-0}}" -eq 0 ]; then
        echo "All LHE jobs completed (no active jobs)"
        break
    fi

    echo "  ${{ACTIVE}} job(s) still active, waiting... (elapsed ${{WAITED}}s)"
    sleep $WAIT_INTERVAL
    WAITED=$((WAITED + WAIT_INTERVAL))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "WARNING: LHE job wait timeout reached"
fi

echo "Current job status:"
source ~/.bashrc 2>/dev/null || true
hep_q -u chengxing 2>/dev/null || echo "  (no active jobs)"

# Phase 3: Submit processing jobs
echo ""
echo "=== Phase 3: Submitting processing chain jobs ==="
bash "{submit_proc_path}"

echo ""
echo "============================================"
echo "Workflow submitted successfully"
echo "============================================"
echo ""
echo "Monitor with: source ~/.bashrc && hep_q -u chengxing"
echo "Check logs: ${{LOG_DIR}}"
echo "End: $(date)"
'''
    return script


class HepJobBuilder:
    """生成 hep_sub 作业脚本和编排器的构建器。"""

    def __init__(
        self,
        output_dir: str,
        jobs_per_campaign: int,
        max_events: int,
        enable_ntuple: bool,
        efficiency_ntuple: bool,
        shuffle_mixing: bool,
        cleanup: bool,
        test_mode: bool,
        scan_existing: bool,
        force_generate_lhe: bool,
        proxy_path: str,
        lhe_unwevt: Optional[int],
        hep_group: str = "cms",
        walltime: str = "test",
    ):
        self.output_dir = os.path.abspath(output_dir)
        self.jobs_per_campaign = jobs_per_campaign
        self.max_events = max_events
        self.enable_ntuple = enable_ntuple
        self.efficiency_ntuple = efficiency_ntuple
        self.shuffle_mixing = shuffle_mixing
        self.cleanup = cleanup
        self.test_mode = test_mode
        self.scan_existing = scan_existing
        self.force_generate_lhe = force_generate_lhe
        self.proxy_path = proxy_path
        self.lhe_unwevt = lhe_unwevt
        self.hep_group = hep_group
        self.walltime = walltime

        self.bundle_dir = os.path.join(self.output_dir, "bundles")
        self.scripts_dir = os.path.join(self.output_dir, "scripts")
        self.logs_dir = os.path.join(self.output_dir, "logs")

        self.lhe_jobs_info: List[Dict] = []
        self.proc_jobs_info: List[Dict] = []
        self.metadata: Dict = OrderedDict()

    def resolved_lhe_unwevt(self) -> int:
        if self.lhe_unwevt is not None:
            return self.lhe_unwevt
        return 100 if self.test_mode else 100000

    def build(
        self,
        campaign_names: Sequence[str],
    ) -> Tuple[str, str, str]:
        """生成所有文件，返回 orchestrator, submit_lhe, submit_proc 路径。"""

        ensure_dir(self.bundle_dir)
        ensure_dir(self.scripts_dir)
        ensure_dir(self.logs_dir)

        pool_requirements = compute_pool_requirements(campaign_names, self.jobs_per_campaign)

        existing_pools: Dict[str, Dict] = OrderedDict()
        if self.force_generate_lhe:
            for pool_name, required_count in pool_requirements.items():
                existing_pools[pool_name] = {
                    "required_count": required_count,
                    "use_existing": False,
                    "error": "已禁用远端复用",
                }
        elif self.scan_existing:
            existing_pools = scan_existing_pools(pool_requirements, self.proxy_path)
        else:
            for pool_name, required_count in pool_requirements.items():
                existing_pools[pool_name] = {
                    "required_count": required_count,
                    "use_existing": False,
                }

        # 生成 runtime bundles
        runtime_assets = prepare_runtime_assets(self.bundle_dir)
        build_proxy_bundle(self.bundle_dir, self.proxy_path)

        lhe_bundle_name = BUNDLE_NAMES["lhe"]
        proc_bundle_name = BUNDLE_NAMES["processing"]
        proxy_bundle_name = BUNDLE_NAMES["proxy"]

        # 生成 LHE job scripts
        lhe_scripts = []
        lhe_names = []
        for pool_name, required_count in pool_requirements.items():
            info = existing_pools.get(pool_name, {})
            if info.get("use_existing"):
                print(f"  复用已有 LHE pool: {pool_name}")
                continue

            pool = LHE_POOLS[pool_name]
            for index in range(required_count):
                seed = seed_for_pool_index(pool_name, index)
                job_name = f"lhe_{pool_name}_{index}"
                script_path = os.path.join(self.scripts_dir, f"{job_name}.sh")
                content = generate_lhe_job_script(
                    pool_name, index, seed,
                    self.resolved_lhe_unwevt(), self.test_mode,
                    self.bundle_dir, lhe_bundle_name, proxy_bundle_name,
                )
                write_job_script(script_path, content)
                lhe_scripts.append(script_path)
                lhe_names.append(job_name)
                self.lhe_jobs_info.append({
                    "name": job_name,
                    "pool": pool_name,
                    "index": index,
                    "seed": seed,
                    "script": script_path,
                })

        # 生成 processing job scripts
        proc_scripts = []
        proc_names = []
        for campaign_name in campaign_names:
            campaign = CAMPAIGNS[campaign_name]
            input_specs_list: List[List[str]] = []
            for pool_name in campaign.inputs:
                specs = []
                for usage_idx in range(self.jobs_per_campaign):
                    spec = f"EOS:{pool_name}:{usage_idx}:0"
                    specs.append(spec)
                input_specs_list.append(specs)

            for job_index in range(self.jobs_per_campaign):
                job_name = f"proc_{campaign_name}_{job_index}"
                inputs = ",".join([spec_list[job_index] for spec_list in input_specs_list])
                modes = ",".join(campaign.shower_modes)

                script_path = os.path.join(self.scripts_dir, f"{job_name}.sh")
                content = generate_processing_job_script(
                    campaign_name, job_index,
                    inputs, modes, campaign.analysis_type,
                    campaign.n_sources, self.max_events,
                    self.enable_ntuple, self.efficiency_ntuple, self.shuffle_mixing, self.cleanup,
                    self.bundle_dir, proc_bundle_name, proxy_bundle_name,
                )
                write_job_script(script_path, content)
                proc_scripts.append(script_path)
                proc_names.append(job_name)
                self.proc_jobs_info.append({
                    "name": job_name,
                    "campaign": campaign_name,
                    "job_index": job_index,
                    "inputs": inputs,
                    "modes": modes,
                    "script": script_path,
                })

        # 生成 submit 脚本
        submit_lhe_path = os.path.join(self.output_dir, "submit_lhe.sh")
        submit_lhe_content = generate_submit_lhe_script(
            lhe_scripts, lhe_names, self.logs_dir, self.bundle_dir,
            lhe_bundle_name, proxy_bundle_name, self.hep_group, self.walltime,
        )
        write_job_script(submit_lhe_path, submit_lhe_content)

        submit_proc_path = os.path.join(self.output_dir, "submit_processing.sh")
        submit_proc_content = generate_submit_processing_script(
            proc_scripts, proc_names, self.logs_dir, self.bundle_dir,
            proc_bundle_name, proxy_bundle_name, self.hep_group, self.walltime,
        )
        write_job_script(submit_proc_path, submit_proc_content)

        orchestrator_path = os.path.join(self.output_dir, "workflow.sh")
        orchestrator_content = generate_orchestrator_script(
            submit_lhe_path, submit_proc_path, self.logs_dir,
        )
        write_job_script(orchestrator_path, orchestrator_content)

        # 元数据
        self.metadata = OrderedDict([
            ("created_at", datetime.now().isoformat()),
            ("output_dir", self.output_dir),
            ("orchestrator", orchestrator_path),
            ("submit_lhe", submit_lhe_path),
            ("submit_processing", submit_proc_path),
            ("campaigns", list(campaign_names)),
            ("jobs_per_campaign", self.jobs_per_campaign),
            ("max_events", self.max_events),
            ("enable_ntuple", self.enable_ntuple),
            ("efficiency_ntuple", self.efficiency_ntuple),
            ("shuffle_mixing", self.shuffle_mixing),
            ("test_mode", self.test_mode),
            (
                "ntuple_manifest",
                build_ntuple_manifest(campaign_names, self.jobs_per_campaign)
                if self.efficiency_ntuple
                else OrderedDict(),
            ),
            ("lhe_jobs", self.lhe_jobs_info),
            ("processing_jobs", self.proc_jobs_info),
            ("bundle_dir", self.bundle_dir),
            ("logs_dir", self.logs_dir),
        ])
        metadata_path = os.path.join(self.output_dir, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as handle:
            json.dump(self.metadata, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

        if self.efficiency_ntuple:
            write_ntuple_manifest(
                self.output_dir,
                build_ntuple_manifest(campaign_names, self.jobs_per_campaign),
            )

        return orchestrator_path, submit_lhe_path, submit_proc_path


def default_hepjob_output_dir() -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(WORKFLOW_BASE, f"batch_{timestamp}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="HepJob (hep_sub) MC 工作流生成器 — ihep/lxlogin 适配版",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    generate_parser = subparsers.add_parser("generate", help="生成正式生产工作流")
    generate_parser.add_argument(
        "--campaign", action="append", required=True,
        help="可重复指定，支持 ALL/JJP_ALL/JUP_ALL 或逗号分隔。",
    )
    generate_parser.add_argument("--jobs", type=int, default=10, help="每个 campaign 的 job 数。")
    generate_parser.add_argument(
        "--output-dir", default=None,
        help=f"输出目录（默认: {WORKFLOW_BASE}/hepjob_output/batch_<timestamp>）。",
    )
    generate_parser.add_argument("--max-events", type=int, default=-1, help="每个 job 的事件数，-1 表示全部。")
    generate_parser.add_argument("--lhe-unwevt", type=int, default=None, help="LHE 节点的 unwevt。")
    generate_parser.add_argument(
        "--enable-ntuple", dest="enable_ntuple", action="store_true", default=False,
        help="启用 Ntuple 步骤。",
    )
    generate_parser.add_argument(
        "--disable-ntuple", dest="enable_ntuple", action="store_false",
        help="跳过 Ntuple，仅到 MiniAOD。",
    )
    generate_parser.add_argument(
        "--efficiency-ntuple", action="store_true",
        help="生成 multileppat 效率/acceptance 可用的 JJP full-GEN truth ntuple，并写出 ntuple manifest。",
    )
    generate_parser.add_argument(
        "--shuffle-mixing", dest="shuffle_mixing", action="store_true", default=False,
        help="启用确定性的多输入源 shuffle mixing。",
    )
    generate_parser.add_argument(
        "--no-shuffle-mixing", dest="shuffle_mixing", action="store_false",
        help="禁用 shuffle mixing（默认顺序 mixing）。",
    )
    generate_parser.add_argument(
        "--cleanup", dest="cleanup", action="store_true", default=True,
        help="作业结束后清理中间文件。",
    )
    generate_parser.add_argument(
        "--no-cleanup", dest="cleanup", action="store_false",
        help="保留 worker 上的中间文件。",
    )
    generate_parser.add_argument(
        "--scan-existing", dest="scan_existing", action="store_true", default=True,
        help="扫描远端已有 LHE pool。",
    )
    generate_parser.add_argument(
        "--no-scan-existing", dest="scan_existing", action="store_false",
        help="不扫描远端，所有 pool 都生成。",
    )
    generate_parser.add_argument(
        "--force-generate-lhe", action="store_true",
        help="强制生成本次需要的全部 LHE。",
    )
    generate_parser.add_argument(
        "--proxy-path", default=detect_proxy_path(),
        help="X509 代理路径（默认自动探测）。",
    )
    generate_parser.add_argument("--group", default="cms", help="HepJob 组名。")
    generate_parser.add_argument(
        "--walltime", default="test", choices=("test", "short", "mid", "long", "special"),
        help="作业 walltime 等级（test/short/mid/long/special）。",
    )

    test_parser = subparsers.add_parser("generate-test", help="生成小批量测试工作流")
    test_parser.add_argument(
        "--campaign", action="append", required=True,
        help="可重复指定 campaign。",
    )
    test_parser.add_argument("--jobs", type=int, default=1, help="每个 campaign 的 job 数。")
    test_parser.add_argument(
        "--output-dir", default=None,
        help="输出目录（默认: 自动生成时间戳目录）。",
    )
    test_parser.add_argument("--max-events", type=int, default=5, help="每个 job 的事件数。")
    test_parser.add_argument(
        "--enable-ntuple", dest="enable_ntuple", action="store_true", default=False,
        help="测试 workflow 中启用 Ntuple 步骤。",
    )
    test_parser.add_argument(
        "--disable-ntuple", dest="enable_ntuple", action="store_false",
        help="测试 workflow 中跳过 Ntuple 步骤。",
    )
    test_parser.add_argument(
        "--efficiency-ntuple", action="store_true",
        help="生成 multileppat 效率/acceptance 可用的 JJP full-GEN truth ntuple，并写出 ntuple manifest。",
    )
    test_parser.add_argument(
        "--shuffle-mixing", dest="shuffle_mixing", action="store_true", default=False,
        help="启用确定性的多输入源 shuffle mixing。",
    )
    test_parser.add_argument(
        "--no-shuffle-mixing", dest="shuffle_mixing", action="store_false",
        help="禁用 shuffle mixing（默认顺序 mixing）。",
    )
    test_parser.add_argument("--proxy-path", default=detect_proxy_path(), help="代理路径。")
    test_parser.add_argument("--group", default="cms", help="HepJob 组名。")

    ntuple_only_parser = subparsers.add_parser(
        "generate-ntuple-only",
        help="从已有 MiniAOD 文件生成仅含 ntuple 重跑节点的 HepJob 工作流",
    )
    ntuple_only_parser.add_argument(
        "--campaign", action="append", required=True,
        help="可重复指定，支持 ALL/JJP_ALL/JUP_ALL 或逗号分隔。",
    )
    ntuple_only_parser.add_argument(
        "--miniaod-dir", default="",
        help="本地 MiniAOD 基础目录，包含 campaign_name/job_index/MINIAOD.root 结构。",
    )
    ntuple_only_parser.add_argument(
        "--miniaod-base-url", default="",
        help="远端 MiniAOD URL 基础 (e.g. root://cceos.ihep.ac.cn//eos/.../output)，与 --jobs 配合使用。",
    )
    ntuple_only_parser.add_argument(
        "--miniaod-filename", default="MINIAOD.root",
        help="MiniAOD 文件名（默认: MINIAOD.root）。",
    )
    ntuple_only_parser.add_argument("--jobs", type=int, default=0, help="每个 campaign 的 job 数。")
    ntuple_only_parser.add_argument(
        "--output-dir", default=None,
        help=f"输出目录（默认: {WORKFLOW_BASE}/hepjob_output/batch_<timestamp>）。",
    )
    ntuple_only_parser.add_argument("--max-events", type=int, default=-1, help="每个 job 的事件数，-1 表示全部。")
    ntuple_only_parser.add_argument(
        "--efficiency-ntuple", action="store_true",
        help="生成 multileppat 效率/acceptance 可用的 JJP full-GEN truth ntuple，并写出 ntuple manifest。",
    )
    ntuple_only_parser.add_argument(
        "--cleanup", dest="cleanup", action="store_true", default=True,
        help="作业结束后清理中间文件。",
    )
    ntuple_only_parser.add_argument(
        "--no-cleanup", dest="cleanup", action="store_false",
        help="保留 worker 上的中间文件。",
    )
    ntuple_only_parser.add_argument("--proxy-path", default=detect_proxy_path(), help="代理路径。")
    ntuple_only_parser.add_argument("--group", default="cms", help="HepJob 组名。")
    ntuple_only_parser.add_argument(
        "--walltime", default="test", choices=("test", "short", "mid", "long", "special"),
        help="作业 walltime 等级（test/short/mid/long/special）。",
    )
    ntuple_only_parser.add_argument(
        "--local-output-base", default="",
        help="本地输出基目录；会通过 LOCAL_OUTPUT_BASE 环境变量传递给 worker。",
    )

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    if args.command in ("generate", "generate-test"):
        campaign_names = expand_campaign_selection(args.campaign)

        output_dir = args.output_dir or default_hepjob_output_dir()
        jobs_per_campaign = args.jobs
        max_events = args.max_events
        enable_ntuple = args.enable_ntuple if hasattr(args, "enable_ntuple") else False
        efficiency_ntuple = args.efficiency_ntuple if hasattr(args, "efficiency_ntuple") else False
        shuffle_mixing = args.shuffle_mixing if hasattr(args, "shuffle_mixing") else False
        if efficiency_ntuple:
            try:
                validate_efficiency_campaigns(campaign_names)
            except ValueError as exc:
                parser.error(str(exc))
            enable_ntuple = True
        cleanup = args.cleanup if hasattr(args, "cleanup") else True
        test_mode = (args.command == "generate-test")
        scan_existing = args.scan_existing if hasattr(args, "scan_existing") else True
        force_generate_lhe = args.force_generate_lhe if hasattr(args, "force_generate_lhe") else False
        lhe_unwevt = args.lhe_unwevt if hasattr(args, "lhe_unwevt") else None
        hep_group = args.group
        walltime = args.walltime if hasattr(args, "walltime") else "test"

        print(f"=== HepJob 工作流生成 ===")
        print(f"Campaigns: {', '.join(campaign_names)}")
        print(f"每个 campaign job 数: {jobs_per_campaign}")
        print(f"每 job 事件数: {max_events}")
        print(f"Ntuple: {enable_ntuple}")
        print(f"Efficiency ntuple: {efficiency_ntuple}")
        print(f"Shuffle mixing: {shuffle_mixing}")
        print(f"测试模式: {test_mode}")
        print(f"输出目录: {output_dir}")
        print()

        builder = HepJobBuilder(
            output_dir=output_dir,
            jobs_per_campaign=jobs_per_campaign,
            max_events=max_events,
            enable_ntuple=enable_ntuple,
            efficiency_ntuple=efficiency_ntuple,
            shuffle_mixing=shuffle_mixing,
            cleanup=cleanup,
            test_mode=test_mode,
            scan_existing=scan_existing,
            force_generate_lhe=force_generate_lhe,
            proxy_path=args.proxy_path,
            lhe_unwevt=lhe_unwevt,
            hep_group=hep_group,
            walltime=walltime,
        )

        orch_path, lhe_path, proc_path = builder.build(campaign_names)

        print()
        print("=== 生成完成 ===")
        print(f"编排器: {orch_path}")
        print(f"LHE 提交脚本: {lhe_path}")
        print(f"Processing 提交脚本: {proc_path}")
        print(f"作业脚本: {builder.scripts_dir}/")
        print(f"日志目录: {builder.logs_dir}/")
        print(f"元数据: {os.path.join(output_dir, 'metadata.json')}")
        if efficiency_ntuple:
            print(f"Ntuple manifest: {os.path.join(output_dir, 'ntuple_manifest.json')}")
        print()
        print("使用方法：")
        print(f"  # 自动编排（提交 LHE → 等待 → 提交 processing）")
        print(f"  bash {orch_path}")
        print()
        print(f"  # 或分步执行")
        print(f"  bash {lhe_path}")
        print(f"  hep_q                                   # 检查状态")
        print(f"  bash {proc_path}")
        return 0

    if args.command == "generate-ntuple-only":
        campaign_names = expand_campaign_selection(args.campaign)

        if args.efficiency_ntuple:
            try:
                validate_efficiency_campaigns(campaign_names)
            except ValueError as exc:
                parser.error(str(exc))

        output_dir = args.output_dir or default_hepjob_output_dir()
        miniaod_dir = args.miniaod_dir or ""
        miniaod_base_url = args.miniaod_base_url or ""
        miniaod_filename = args.miniaod_filename or "MINIAOD.root"
        jobs = args.jobs
        max_events = args.max_events
        efficiency_ntuple = args.efficiency_ntuple
        cleanup = args.cleanup
        hep_group = args.group
        walltime = args.walltime
        local_output_base = args.local_output_base or ""

        use_miniaod_dir = bool(miniaod_dir)
        use_miniaod_base_url = bool(miniaod_base_url)

        if use_miniaod_dir and use_miniaod_base_url:
            parser.error("--miniaod-dir 和 --miniaod-base-url 不能同时使用")

        # Resolve job indices
        if use_miniaod_dir:
            campaign_jobs_map = discover_ntuple_jobs(
                miniaod_dir, campaign_names, miniaod_filename, max_jobs=jobs,
            )
        elif use_miniaod_base_url:
            if jobs <= 0:
                parser.error("使用 --miniaod-base-url 时必须提供 --jobs")
            campaign_jobs_map = OrderedDict(
                (name, list(range(jobs))) for name in campaign_names
            )
        else:
            parser.error("需要 --miniaod-dir 或 --miniaod-base-url")

        if not campaign_jobs_map:
            print("错误: 没有任何可用的 ntuple job（未找到 MiniAOD 文件）", file=sys.stderr)
            return 1

        total_jobs = sum(len(indices) for indices in campaign_jobs_map.values())

        print(f"=== HepJob Ntuple-only 工作流生成 ===")
        print(f"Campaigns: {', '.join(campaign_names)}")
        print(f"Total ntuple jobs: {total_jobs}")
        print(f"每 job 事件数: {max_events}")
        print(f"Efficiency ntuple: {efficiency_ntuple}")
        print(f"输出目录: {output_dir}")
        print()

        ensure_dir(output_dir)
        bundle_dir = os.path.join(output_dir, "bundles")
        scripts_dir = os.path.join(output_dir, "scripts")
        logs_dir = os.path.join(output_dir, "logs")
        ensure_dir(bundle_dir)
        ensure_dir(scripts_dir)
        ensure_dir(logs_dir)

        # Prepare ntuple-only runtime assets
        runtime_assets = prepare_ntuple_only_assets(bundle_dir)
        build_proxy_bundle(bundle_dir, args.proxy_path)
        ntuple_bundle_name = BUNDLE_NAMES["ntuple"]
        proxy_bundle_name = BUNDLE_NAMES["proxy"]

        # MiniAOD input path factory
        def miniaod_input_fn(campaign_name: str, job_index: int) -> str:
            if use_miniaod_dir:
                raw_path = os.path.join(miniaod_dir, campaign_name, str(job_index), miniaod_filename)
                return f"file:{raw_path}"
            else:
                base = miniaod_base_url.rstrip("/")
                return f"{base}/{campaign_name}/{job_index}/{miniaod_filename}"

        # Generate ntuple-only job scripts
        ntuple_scripts = []
        ntuple_names = []
        for campaign_name in campaign_names:
            campaign = CAMPAIGNS[campaign_name]
            job_indices = campaign_jobs_map.get(campaign_name, [])
            for job_index in job_indices:
                job_name = f"ntuple_{campaign_name}_{job_index}"
                miniaod_input = miniaod_input_fn(campaign_name, job_index)
                script_path = os.path.join(scripts_dir, f"{job_name}.sh")
                content = generate_ntuple_only_job_script(
                    campaign_name, job_index, campaign.analysis_type,
                    miniaod_input, max_events,
                    efficiency_ntuple, cleanup,
                    bundle_dir, ntuple_bundle_name, proxy_bundle_name,
                )
                write_job_script(script_path, content)
                ntuple_scripts.append(script_path)
                ntuple_names.append(job_name)

        # Generate submit script
        submit_ntuple_path = os.path.join(output_dir, "submit_ntuple.sh")
        submit_lines = ["#!/bin/bash", "set -euo pipefail", ""]
        for script_path, job_name in zip(ntuple_scripts, ntuple_names):
            log_prefix = os.path.join(logs_dir, job_name)
            submit_lines.append(
                f"hep_sub {script_path} -g {hep_group} -gwn CMS -wt {walltime} -n 1 "
                f"-o {log_prefix}.stdout -e {log_prefix}.stderr"
            )
        write_job_script(submit_ntuple_path, "\n".join(submit_lines) + "\n")

        # Metadata
        metadata = OrderedDict([
            ("created_at", datetime.now().isoformat()),
            ("output_dir", output_dir),
            ("submit_ntuple", submit_ntuple_path),
            ("campaigns", list(campaign_names)),
            ("campaign_jobs", {name: list(indices) for name, indices in campaign_jobs_map.items()}),
            ("ntuple_only", True),
            ("max_events", max_events),
            ("efficiency_ntuple", efficiency_ntuple),
            ("bundle_dir", bundle_dir),
            ("logs_dir", logs_dir),
            (
                "ntuple_manifest",
                build_ntuple_manifest(
                    campaign_names,
                    campaign_jobs_map=campaign_jobs_map,
                    local_output_base=local_output_base,
                )
                if efficiency_ntuple
                else OrderedDict(),
            ),
        ])
        metadata_path = os.path.join(output_dir, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

        if efficiency_ntuple:
            write_ntuple_manifest(
                output_dir,
                build_ntuple_manifest(
                    campaign_names,
                    campaign_jobs_map=campaign_jobs_map,
                    local_output_base=local_output_base,
                ),
            )

        print()
        print("=== 生成完成 ===")
        print(f"Submit 脚本: {submit_ntuple_path}")
        print(f"作业脚本: {scripts_dir}/")
        print(f"日志目录: {logs_dir}/")
        print(f"元数据: {metadata_path}")
        if efficiency_ntuple:
            print(f"Ntuple manifest: {os.path.join(output_dir, 'ntuple_manifest.json')}")
        print()
        print("使用方法：")
        print(f"  bash {submit_ntuple_path}")
        return 0

    parser.error(f"未知命令: {args.command}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
