#!/usr/bin/env python3
"""
基于 workbook_v2.md 的 HTCondor DAGMan 工作流生成器。

目标：
1. 用统一的数据模型描述 LHE pool、campaign 和测试配置。
2. 生成可直接提交到 HTCondor 的 DAG、DAGMan 配置和元数据摘要。
3. 提供环境校验、配置列表和小批量测试 DAG 生成入口。
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.compression_util import accepts_lhe_ext  # noqa: E402

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(BASE_DIR, "generated")
TEST_OUTPUT_DIR = os.path.join(BASE_DIR, "tests", "generated")
TPS_ONIA2MUMU_SUBMODULE = os.path.join(BASE_DIR, "external", "TPS-Onia2MuMu")

EOS_HOST = "cceos.ihep.ac.cn"
EOS_XRDFS_TARGET = EOS_HOST
EOS_PATH_BASE = "/eos/ihep/cms/store/user/xcheng/MC_Production_v3"
EOS_BASE = f"root://{EOS_HOST}/{EOS_PATH_BASE}"
EOS_OUTPUT = f"{EOS_BASE}/output"
STORAGE_SITE = "T2_CN_Beijing"

# Chi's output area for v3 reprocessing and ntuple-from-MiniAOD workflows.
CHIW_EOS_OUTPUT_BASE = "root://cceos.ihep.ac.cn//store/user/chiw/MC_Production_v3"
NTUPLE_VERSION = "v01_06"

SUBPROCESS_MAP = OrderedDict([
    ("JJP_SPS_CS",  "SPS-JpsiJpsiPhi-LO"),
    ("JJP_SPS_G",   "SPS-JpsiJpsiPhi-NLOstar"),
    ("JJP_DPS2_CS", "DPS-JpsiJpsi-Phi-LO"),
    ("JJP_DPS2_G",  "DPS-JpsiJpsi-Phi-NLOstar"),
    ("JJP_DPS1",    "DPS-Jpsi-JpsiPhi"),
])

def parse_jobs_arg(jobs_str: str):
    """Parse --jobs argument that accepts either a single integer (applied to all
    campaigns) or comma-separated campaign=count pairs.

    Returns:
        int if the value is a bare integer.
        Dict[str, int] if the value contains '=' signs (per-campaign mapping).
        None if the value is empty.
    """
    if not jobs_str or not jobs_str.strip():
        return None
    jobs_str = jobs_str.strip()
    if "=" in jobs_str:
        result = OrderedDict()
        for pair in jobs_str.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if "=" not in pair:
                raise ValueError(f"Mixed --jobs format: expected all campaign=count pairs, got '{pair}'")
            name, count = pair.split("=", 1)
            result[name.strip()] = int(count.strip())
        return result
    try:
        return int(jobs_str)
    except ValueError:
        raise ValueError(
            f"Invalid --jobs value: '{jobs_str}'. "
            "Use a bare integer (e.g. --jobs 100) or campaign=count pairs "
            "(e.g. --jobs JJP_SPS_CS=999,JJP_DPS1=996)."
        )

DEFAULT_TEST_CAMPAIGNS = ("JJP_DPS2_CS", "JJP_DPS2_G", "JUP_DPS1")
POOL_SCAN_CACHE_ENV = "DAG_GENERATOR_POOL_SCAN_CACHE"

REQUIRED_FILES = (
    "common/octet_pdg.py",
    "lhe_generation/run_helac.sh",
    "processing/run_chain.sh",
    "processing/templates/lhe_gen.sub",
    "processing/templates/helac_matrix.sub",
    "processing/templates/processing.sub",
    "processing/templates/ntuple.sub",
    "processing/templates/summary.sub",
    "processing/templates/summary.sh",
    "common/cmssw_configs/hepmc_to_GENSIM.py",
)

REQUIRED_COMMANDS = (
    "python3",
    "condor_submit",
    "condor_submit_dag",
    "condor_q",
    "xrdfs",
    "xrdcp",
    "apptainer",
)

LHE_SUBMIT_TEMPLATE_LXPLUS = "processing/templates/lhe_gen.sub"
PROCESSING_SUBMIT_TEMPLATE_LXPLUS = "processing/templates/processing.sub"
LHE_SUBMIT_TEMPLATE_HEPTHU = "processing/templates/lhe_gen_hepthu.sub"
PROCESSING_SUBMIT_TEMPLATE_HEPTHU = "processing/templates/processing_hepthu.sub"
LHE_SUBMIT_TEMPLATE_LOCAL = "processing/templates/lhe_gen_local.sub"
PROCESSING_SUBMIT_TEMPLATE_LOCAL = "processing/templates/processing_local.sub"
SUMMARY_SUBMIT_TEMPLATE = "processing/templates/summary.sub"
# Log and proxy paths are workspace-relative or user-provided; no hardcoded user paths.
DEFAULT_HEPTHU_OUTPUT_BASE = os.path.expanduser("~/MC_Production_result")
DEFAULT_LOCAL_CONDOR_OUTPUT_BASE = os.path.expanduser("~/MC_Production_result")


@dataclass(frozen=True)
class MachineEnv:
    """Submit-host and storage profile selected from the unified CLI."""

    name: str
    description: str
    backend: str
    submit_host: str
    storage_description: str
    storage_mode: str
    required_commands: Tuple[str, ...]
    log_dir: str = ""
    local_output_base: str = ""
    lhe_submit_template: str = LHE_SUBMIT_TEMPLATE_LXPLUS
    processing_submit_template: str = PROCESSING_SUBMIT_TEMPLATE_LXPLUS
    summary_submit_template: str = SUMMARY_SUBMIT_TEMPLATE
    target_machine: str = ""
    aliases: Tuple[str, ...] = ()

    @property
    def uses_local_storage(self) -> bool:
        return self.storage_mode == "local"

    @property
    def is_hepjob(self) -> bool:
        return self.backend == "hepjob"

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "aliases": list(self.aliases),
            "description": self.description,
            "backend": self.backend,
            "submit_host": self.submit_host,
            "storage_description": self.storage_description,
            "storage_mode": self.storage_mode,
            "log_dir": self.log_dir,
            "local_output_base": self.local_output_base,
            "lhe_submit_template": self.lhe_submit_template,
            "processing_submit_template": self.processing_submit_template,
            "summary_submit_template": self.summary_submit_template,
            "target_machine": self.target_machine,
        }


MACHINE_ENVS: "OrderedDict[str, MachineEnv]" = OrderedDict(
    [
        (
            "lxplus_t2_ihep",
            MachineEnv(
                name="lxplus_t2_ihep",
                aliases=("t2_cn_beijing",),
                description="CERN lxplus HTCondor submit; store LHE/output on IHEP T2_CN_Beijing via XRootD.",
                backend="condor_dagman",
                submit_host="CERN lxplus",
                storage_description="IHEP T2_CN_Beijing XRootD storage",
                storage_mode="t2_xrootd",
                required_commands=REQUIRED_COMMANDS,
                log_dir="",
            ),
        ),
        (
            "hepthu",
            MachineEnv(
                name="hepthu",
                description="Tsinghua hepthu HTCondor submit; store LHE/output on a local filesystem.",
                backend="condor_dagman",
                submit_host="hepthu HTCondor",
                storage_description="Local filesystem storage",
                storage_mode="local",
                required_commands=("python3", "condor_submit", "condor_submit_dag", "condor_q", "apptainer"),
                local_output_base=DEFAULT_HEPTHU_OUTPUT_BASE,
                lhe_submit_template=LHE_SUBMIT_TEMPLATE_HEPTHU,
                processing_submit_template=PROCESSING_SUBMIT_TEMPLATE_HEPTHU,
                target_machine="nd-16.hepthu.com",
            ),
        ),
        (
            "local_condor",
            MachineEnv(
                name="local_condor",
                description="Local HTCondor submit; store output on local filesystem.",
                backend="condor_dagman",
                submit_host="local HTCondor",
                storage_description="Local filesystem storage",
                storage_mode="local",
                required_commands=("python3", "condor_submit", "condor_submit_dag", "condor_q", "apptainer"),
                local_output_base=DEFAULT_LOCAL_CONDOR_OUTPUT_BASE,
                lhe_submit_template=LHE_SUBMIT_TEMPLATE_LOCAL,
                processing_submit_template=PROCESSING_SUBMIT_TEMPLATE_LOCAL,
            ),
        ),
        (
            "ihep",
            MachineEnv(
                name="ihep",
                description="IHEP/lxlogin HepJob submit; store LHE/output on IHEP T2_CN_Beijing via XRootD.",
                backend="hepjob",
                submit_host="IHEP lxlogin/HepJob",
                storage_description="IHEP T2_CN_Beijing XRootD storage",
                storage_mode="t2_xrootd",
                required_commands=("python3", "hep_sub", "hep_q", "hep_rm", "xrdfs", "xrdcp", "apptainer"),
            ),
        ),
    ]
)

MACHINE_ENV_ALIASES = {
    alias: name
    for name, machine_env in MACHINE_ENVS.items()
    for alias in (machine_env.aliases + (name,))
}

BUNDLE_NAMES = {
    "lhe": "lhe_runtime_bundle.tar.gz",
    "processing": "processing_runtime_bundle.tar.gz",
    "ntuple": "ntuple_runtime_bundle.tar.gz",
    "summary": "summary_runtime_bundle.tar.gz",
    "compression": "compression_runtime_bundle.tar.gz",
    "proxy": "proxy_bundle.tar.gz",
    "plan": "plan_runtime_bundle.tar.gz",
    "coordinate": "coordinate_runtime_bundle.tar.gz",
}

NTUPLE_WRAPPER_PATH = os.path.join(
    BASE_DIR, "processing", "condor_wrappers", "run_ntuple_only.sh"
)
NTUPLE_WRAPPER_NAME = "run_ntuple_only.sh"
COMPRESS_WRAPPER_PATH = os.path.join(
    BASE_DIR, "processing", "condor_wrappers", "run_compress.sh"
)
COMPRESS_WRAPPER_NAME = "run_compress.sh"
TRANSFER_COMPRESS_WRAPPER_PATH = os.path.join(
    BASE_DIR, "processing", "condor_wrappers", "run_transfer_compress.sh"
)
TRANSFER_COMPRESS_WRAPPER_NAME = "run_transfer_compress.sh"
PLAN_SUBMIT_TEMPLATE = "processing/templates/plan_lhe_blocks.sub"
PLAN_WRAPPER_PATH = os.path.join(
    BASE_DIR, "processing", "condor_wrappers", "run_plan_lhe_blocks.sh"
)
COORDINATE_SUBMIT_TEMPLATE = "processing/templates/coordinate_lhe_blocks.sub"
COORDINATE_WRAPPER_PATH = os.path.join(
    BASE_DIR, "processing", "condor_wrappers", "run_coordinate_lhe_blocks.sh"
)
DEFAULT_LOG_ROOT = os.path.join(BASE_DIR, "log")
CMSSW15_RUNTIME_TARBALL_NAME = "cmssw15_tpsonia2mumu_runtime.tar.gz"
DEFAULT_CMSSW15_RUNTIME_TARBALL = os.path.join(
    BASE_DIR,
    "common",
    "packages",
    CMSSW15_RUNTIME_TARBALL_NAME,
)
CMSSW15_RUNTIME_REQUIRED_MEMBERS = (
    "CMSSW_15_0_15",
    "CMSSW_15_0_15/src",
    "CMSSW_15_0_15/src/HeavyFlavorAnalysis/TPS-Onia2MuMu",
    "CMSSW_15_0_15/src/HeavyFlavorAnalysis/TPS-Onia2MuMu/test/ConfFile_cfg.py",
    "CMSSW_15_0_15/lib/el9_amd64_gcc12/pluginHeavyFlavorAnalysisTPS-Onia2MuMu.so",
    "CMSSW_15_0_15/lib/el9_amd64_gcc12/.edmplugincache",
)

POOL_DAG_LABELS = {
    "pool_jpsi_CSCO_g": "JpsiG_CSCO",
    "pool_upsilon_CSCO_g": "UpsilonG_CSCO",
    "pool_gg": "GG",
    "pool_2jpsi_cs": "DoubleJpsiCS",
    "pool_2jpsi_g": "DoubleJpsiG",
    "pool_jpsi_upsilon_CSCO": "JpsiUpsilon_CSCO",
}

HELAC_MATRIX_STATES = (
    "3S11",
    "3P01",
    "3P11",
    "3P21",
    "3S18",
    "1S08",
    "3P08",
    "3P18",
    "3P28",
)
HELAC_MATRIX_STAGEOUT_DIR = (
    "root://eosuser.cern.ch//eos/user/c/chiw/JpsiJpsiUps/tryHelac/"
    "psiY_fullcalc_14May2026"
)
HELAC_MATRIX_CHARM_BASE_MASS = 1.54845
HELAC_MATRIX_BOTTOM_BASE_MASS = 4.73020
HELAC_MATRIX_OCTET_MASS_SHIFT = 0.1


def canonical_mode(mode: str) -> str:
    """把历史别名统一到新的 shower 模式枚举。"""

    normalized = mode.strip().lower()
    alias_map = {
        "normal": "normal",
        "phi": "phi_mpi_off",
        "phi_default": "phi_mpi_off",
        "phi_mode1": "phi_mpi_off",
        "phi_mpi_off": "phi_mpi_off",
        "sps": "phi_mpi_off",
        "phi_mode2": "phi_mpi_on_gluon",
        "phi_mpi_on_gluon": "phi_mpi_on_gluon",
        "phi_gluon": "phi_mpi_on_gluon",
    }
    if normalized not in alias_map:
        raise ValueError(f"未知的 shower 模式: {mode}")
    return alias_map[normalized]


class LHEPool:
    """单个 LHE pool 的生成配置。"""

    def __init__(
        self,
        name: str,
        description: str,
        process_lines: Sequence[str] = (),
        min_pt_conia: float = 6.0,
        min_pt_bonia: float = 4.0,
        min_pt_q: float = 0.0,
        notes: str = "",
        seed_offset: int = 0,
        storage_name: Optional[str] = None,
        variants: Sequence[str] = (),
        public: bool = True,
    ):
        self.name = name
        self.description = description
        self.process_lines = list(process_lines)
        self.min_pt_conia = min_pt_conia
        self.min_pt_bonia = min_pt_bonia
        self.min_pt_q = min_pt_q
        self.notes = notes
        self.seed_offset = seed_offset
        self.storage_name = storage_name or name
        self.variants = list(variants)
        self.public = public

    @property
    def process_text(self) -> str:
        if self.is_composite:
            return " + ".join(self.variants)
        return "\n".join(self.process_lines)

    @property
    def is_composite(self) -> bool:
        return bool(self.variants)

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "process_lines": self.process_lines,
            "min_pt_conia": self.min_pt_conia,
            "min_pt_bonia": self.min_pt_bonia,
            "min_pt_q": self.min_pt_q,
            "notes": self.notes,
            "storage_name": self.storage_name,
            "variants": self.variants,
            "public": self.public,
        }


class Campaign:
    """单个 physics campaign 的定义。"""

    def __init__(
        self,
        name: str,
        analysis_type: str,
        inputs: Sequence[str],
        shower_modes: Sequence[str],
        description: str,
        notes: str = "",
    ):
        if len(inputs) != len(shower_modes):
            raise ValueError(f"{name}: inputs 与 shower_modes 数量不一致")
        self.name = name
        self.analysis_type = analysis_type
        self.inputs = list(inputs)
        self.shower_modes = [canonical_mode(mode) for mode in shower_modes]
        self.description = description
        self.notes = notes

    @property
    def n_sources(self) -> int:
        return len(self.inputs)

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "analysis_type": self.analysis_type,
            "inputs": self.inputs,
            "shower_modes": self.shower_modes,
            "description": self.description,
            "notes": self.notes,
        }


class WorkflowOptions:
    """一次 DAG 生成的公共控制选项。"""

    def __init__(
        self,
        jobs_per_campaign: int,
        max_events: int,
        enable_ntuple: bool,
        efficiency_ntuple: bool,
        cleanup: bool,
        test_mode: bool,
        scan_existing: bool,
        force_generate_lhe: bool,
        proxy_path: str,
        lhe_unwevt: Optional[int],
        dagman_max_jobs_submitted: int,
        dagman_max_jobs_idle: int,
        machine_env: Optional[MachineEnv] = None,
        local_log_dir: str = "",
        local_output_base: str = "",
        log_root: str = "",
        maxjobs_lhe: int = 20,
        maxjobs_processing: int = 50,
        maxjobs_ntuple: int = 30,
        cmssw15_runtime_tarball: Optional[str] = None,
        shuffle_mixing: bool = False,
        strict_vtx_smearing_check: bool = False,
        compress_lhe: bool = False,
        lhe_compression_level: int = 1,
        lhe_shuffle_split: bool = False,
        lhe_events_per_block: int = 1000,
        lhe_shuffle_mode: str = "stratified",
        lhe_n_strata: str = "auto",
        lhe_drop_incomplete_last_block: bool = False,
        use_subprocess_naming: bool = False,
        enable_lhe_block_subdags: bool = False,
        keep_legacy_single_processing_path: bool = False,
        lhe_shuffle_seed_base: Optional[int] = None,
        max_block_subdag_jobs: int = 0,
        target_base_url: str = "",
        ntuple_version: str = "",
        skip_lhe_generation: bool = False,
        existing_lhe_base: str = "",
    ):
        self.machine_env = machine_env or MACHINE_ENVS["lxplus_t2_ihep"]
        self.jobs_per_campaign = jobs_per_campaign
        self.max_events = max_events
        self.enable_ntuple = enable_ntuple
        self.efficiency_ntuple = efficiency_ntuple
        self.cleanup = cleanup
        self.test_mode = test_mode
        self.compress_lhe = compress_lhe
        self.lhe_compression_level = lhe_compression_level
        self.lhe_shuffle_split = lhe_shuffle_split
        self.lhe_events_per_block = lhe_events_per_block
        self.lhe_shuffle_mode = lhe_shuffle_mode
        self.lhe_n_strata = lhe_n_strata
        self.lhe_drop_incomplete_last_block = lhe_drop_incomplete_last_block
        self.use_subprocess_naming = use_subprocess_naming
        self.enable_lhe_block_subdags = enable_lhe_block_subdags
        self.keep_legacy_single_processing_path = keep_legacy_single_processing_path
        self.lhe_shuffle_seed_base = lhe_shuffle_seed_base
        self.max_block_subdag_jobs = max_block_subdag_jobs if max_block_subdag_jobs > 0 else maxjobs_processing
        self.target_base_url = target_base_url
        self.ntuple_version = ntuple_version
        self.scan_existing = scan_existing
        self.force_generate_lhe = force_generate_lhe
        self.skip_lhe_generation = skip_lhe_generation
        self.existing_lhe_base = existing_lhe_base or ""
        self.proxy_path = proxy_path
        self.lhe_unwevt = lhe_unwevt
        self.dagman_max_jobs_submitted = dagman_max_jobs_submitted
        self.dagman_max_jobs_idle = dagman_max_jobs_idle
        self.local_log_dir = local_log_dir or self.machine_env.log_dir
        self.log_root = log_root or self.local_log_dir
        self.local_output_base = local_output_base or self.machine_env.local_output_base
        self.maxjobs_lhe = maxjobs_lhe
        self.maxjobs_processing = maxjobs_processing
        self.maxjobs_ntuple = maxjobs_ntuple
        self.cmssw15_runtime_tarball = cmssw15_runtime_tarball
        self.shuffle_mixing = shuffle_mixing
        self.strict_vtx_smearing_check = strict_vtx_smearing_check

    def resolved_lhe_unwevt(self) -> int:
        if self.lhe_unwevt is not None:
            return self.lhe_unwevt
        return 100 if self.test_mode else 100000

    def to_dict(self) -> Dict[str, object]:
        return {
            "machine_env": self.machine_env.to_dict(),
            "jobs_per_campaign": self.jobs_per_campaign,
            "max_events": self.max_events,
            "enable_ntuple": self.enable_ntuple,
            "efficiency_ntuple": self.efficiency_ntuple,
            "cleanup": self.cleanup,
            "test_mode": self.test_mode,
            "scan_existing": self.scan_existing,
            "force_generate_lhe": self.force_generate_lhe,
            "proxy_path": self.proxy_path,
            "lhe_unwevt": self.resolved_lhe_unwevt(),
            "dagman_max_jobs_submitted": self.dagman_max_jobs_submitted,
            "dagman_max_jobs_idle": self.dagman_max_jobs_idle,
            "local_log_dir": self.local_log_dir,
            "local_output_base": self.local_output_base,
            "log_root": self.log_root,
            "maxjobs_lhe": self.maxjobs_lhe,
            "maxjobs_processing": self.maxjobs_processing,
            "maxjobs_ntuple": self.maxjobs_ntuple,
            "cmssw15_runtime_tarball": self.cmssw15_runtime_tarball,
            "shuffle_mixing": self.shuffle_mixing,
            "strict_vtx_smearing_check": self.strict_vtx_smearing_check,
            "use_subprocess_naming": self.use_subprocess_naming,
            "target_base_url": self.target_base_url,
            "ntuple_version": self.ntuple_version,
            "enable_lhe_block_subdags": self.enable_lhe_block_subdags,
            "keep_legacy_single_processing_path": self.keep_legacy_single_processing_path,
            "lhe_shuffle_seed_base": self.lhe_shuffle_seed_base,
            "max_block_subdag_jobs": self.max_block_subdag_jobs,
            "skip_lhe_generation": self.skip_lhe_generation,
            "existing_lhe_base": self.existing_lhe_base,
        }


LHE_POOLS: "OrderedDict[str, LHEPool]" = OrderedDict(
    [
        (
            "pool_jpsi_CSCO_g",
            LHEPool(
                name="pool_jpsi_CSCO_g",
                description="pp -> J/psi + X with CrystalBall pT model",
                process_lines=(
                    "addon/pp_psiX_CrystalBall",
                    "state.inp = 1  # J/psi",
                ),
                notes="J/psi 基础池使用 HELAC-Onia addon/pp_psiX_CrystalBall，CrystalBall 参数取 addon input 默认。",
                seed_offset=0,
            ),
        ),
        (
            "pool_upsilon_CSCO_g",
            LHEPool(
                name="pool_upsilon_CSCO_g",
                description="pp -> Upsilon(1S) + X with CrystalBall pT model",
                process_lines=(
                    "addon/pp_psiX_CrystalBall",
                    "state.inp = 3  # Upsilon(1S)",
                ),
                notes="Upsilon(1S) 基础池使用 HELAC-Onia addon/pp_psiX_CrystalBall，CrystalBall 参数取 addon input 默认。",
                seed_offset=20000,
            ),
        ),
        (
            "pool_gg",
            LHEPool(
                name="pool_gg",
                description="gg -> gg",
                process_lines=("generate g g > g g",),
                min_pt_conia=0.0,
                min_pt_bonia=0.0,
                min_pt_q=4.0,
                notes="QCD 背景胶子池。",
                seed_offset=40000,
            ),
        ),
        (
            "pool_2jpsi_cs",
            LHEPool(
                name="pool_2jpsi_cs",
                description="gg -> J/psi + J/psi (born 子过程)",
                process_lines=("generate g g > cc~(3S11) cc~(3S11)",),
                notes="double-J/psi born/color-singlet SPS 基础池。",
                seed_offset=60000,
                public=True,
            ),
        ),
        (
            "pool_2jpsi_g",
            LHEPool(
                name="pool_2jpsi_g",
                description="gg -> J/psi + J/psi + g",
                process_lines=("generate g g > cc~(3S11) cc~(3S11) g",),
                notes="double-J/psi + g SPS 基础池。",
                seed_offset=70000,
            ),
        ),
        (
            "pool_jpsi_upsilon_CSCO",
            LHEPool(
                name="pool_jpsi_upsilon_CSCO",
                description="gg -> J/psi + Upsilon",
                process_lines=("generate g g > jpsi y(1s)",),
                notes="J/psi + Upsilon SPS 基础池。",
                seed_offset=80000,
            ),
        ),
    ]
)


CAMPAIGNS: "OrderedDict[str, Campaign]" = OrderedDict(
    [
        (
            "JJP_SPS_CS",
            Campaign(
                name="JJP_SPS_CS",
                analysis_type="JJP",
                inputs=("pool_2jpsi_cs",),
                shower_modes=("phi_mpi_off",),
                description="double-J/psi born/color-singlet 单源做 phi-enriched shower，不与带额外 gluon 的样本混合。",
            ),
        ),
        (
            "JJP_SPS_G",
            Campaign(
                name="JJP_SPS_G",
                analysis_type="JJP",
                inputs=("pool_2jpsi_g",),
                shower_modes=("phi_mpi_off",),
                description="double-J/psi + g 单源做 phi-enriched shower，不与 born 样本混合。",
            ),
        ),
        (
            "JJP_DPS1",
            Campaign(
                name="JJP_DPS1",
                analysis_type="JJP",
                inputs=("pool_jpsi_CSCO_g", "pool_jpsi_CSCO_g"),
                shower_modes=("normal", "phi_mpi_off"),
                description="两个 J/psi(CS+CO)+g 源混合，覆盖 normal/phi 默认组合。",
            ),
        ),
        (
            "JJP_DPS2_CS",
            Campaign(
                name="JJP_DPS2_CS",
                analysis_type="JJP",
                inputs=("pool_2jpsi_cs", "pool_gg"),
                shower_modes=("normal", "phi_mpi_off"),
                description="double-J/psi born 源与 gg 池混合。",
            ),
        ),
        (
            "JJP_DPS2_G",
            Campaign(
                name="JJP_DPS2_G",
                analysis_type="JJP",
                inputs=("pool_2jpsi_g", "pool_gg"),
                shower_modes=("normal", "phi_mpi_off"),
                description="double-J/psi + g 源与 gg 池混合。",
            ),
        ),
        (
            "JJP_TPS",
            Campaign(
                name="JJP_TPS",
                analysis_type="JJP",
                inputs=("pool_jpsi_CSCO_g", "pool_jpsi_CSCO_g", "pool_gg"),
                shower_modes=("normal", "normal", "phi_mpi_off"),
                description="三源混合的 JJP TPS 方案。",
            ),
        ),
        (
            "JUP_SPS",
            Campaign(
                name="JUP_SPS",
                analysis_type="JUP",
                inputs=("pool_jpsi_upsilon_CSCO",),
                shower_modes=("phi_mpi_off",),
                description="J/psi + Upsilon 单源做 phi-enriched shower。",
            ),
        ),
        (
            "JUP_DPS1",
            Campaign(
                name="JUP_DPS1",
                analysis_type="JUP",
                inputs=("pool_jpsi_CSCO_g", "pool_upsilon_CSCO_g"),
                shower_modes=("phi_mpi_off", "normal"),
                description="J/psi 走 phi 默认模式，Upsilon 走 normal。",
            ),
        ),
        (
            "JUP_DPS2",
            Campaign(
                name="JUP_DPS2",
                analysis_type="JUP",
                inputs=("pool_jpsi_CSCO_g", "pool_upsilon_CSCO_g"),
                shower_modes=("normal", "phi_mpi_off"),
                description="J/psi 走 normal，Upsilon 走 phi 默认模式。",
            ),
        ),
        (
            "JUP_DPS3",
            Campaign(
                name="JUP_DPS3",
                analysis_type="JUP",
                inputs=("pool_jpsi_upsilon_CSCO", "pool_gg"),
                shower_modes=("normal", "phi_mpi_off"),
                description="J/psi+Upsilon 源与 gg 池混合。",
            ),
        ),
        (
            "JUP_TPS",
            Campaign(
                name="JUP_TPS",
                analysis_type="JUP",
                inputs=("pool_jpsi_CSCO_g", "pool_upsilon_CSCO_g", "pool_gg"),
                shower_modes=("normal", "normal", "phi_mpi_off"),
                description="三源混合的 JUP TPS 方案。",
            ),
        ),
    ]
)


MODE_LABELS = OrderedDict(
    [
        ("normal", "普通 shower"),
        ("phi_mpi_off", "phi-enriched 默认模式：关闭 MPI，循环 hadronize 直到找到 phi"),
        ("phi_mpi_on_gluon", "phi-enriched 扩展模式：开启 MPI，并要求 phi 与 LHE 胶子关联"),
    ]
)

_POOL_SCAN_CACHE: Optional[Dict[str, int]] = None


def real_pool_names(pool_name: str) -> List[str]:
    return [pool_name]


def pool_storage_name(pool_name: str) -> str:
    return LHE_POOLS[pool_name].storage_name


def machine_env_choices() -> Tuple[str, ...]:
    choices = ["auto"]
    for name, machine_env in MACHINE_ENVS.items():
        choices.append(name)
        choices.extend(machine_env.aliases)
    return tuple(choices)


def detect_machine_env_name() -> str:
    hostname = socket.gethostname().lower()
    cwd = os.path.abspath(os.getcwd()).lower()

    if "lxplus" in hostname:
        return "lxplus_t2_ihep"
    if "hepthu" in hostname or hostname.startswith("nd-") or "/home/storage29/" in cwd:
        return "hepthu"
    if "ihep" in hostname or "lxlogin" in hostname:
        return "ihep"
    if os.environ.get("LOCAL_OUTPUT_BASE"):
        return "local_condor"
    return "lxplus_t2_ihep"


def requested_machine_env_name(args: argparse.Namespace) -> str:
    machine_env_name = getattr(args, "machine_env", "auto")
    if getattr(args, "local_condor", False):
        if machine_env_name not in {"auto", "local_condor"}:
            raise ValueError(
                "--local-condor cannot be combined with --machine-env unless it is local_condor"
            )
        return "local_condor"
    return machine_env_name


def resolve_machine_env(name: str) -> MachineEnv:
    if name == "auto":
        name = detect_machine_env_name()
    canonical_name = MACHINE_ENV_ALIASES.get(name)
    if not canonical_name:
        raise ValueError(f"未知 machine env: {name}")
    return MACHINE_ENVS[canonical_name]


def required_files_for_env(machine_env: MachineEnv) -> Tuple[str, ...]:
    required = [
        "common/octet_pdg.py",
        "lhe_generation/run_helac.sh",
        "processing/run_chain.sh",
        machine_env.lhe_submit_template,
        machine_env.processing_submit_template,
        machine_env.summary_submit_template,
        "processing/templates/summary.sh",
        "common/cmssw_configs/hepmc_to_GENSIM.py",
    ]
    if machine_env.name == "hepthu":
        required.extend(
            [
                "lhe_generation/condor_wrappers/run_lhe_gen.sh",
                "processing/condor_wrappers/run_processing.sh",
            ]
        )
    deduped: List[str] = []
    for relative_path in required:
        if relative_path and relative_path not in deduped:
            deduped.append(relative_path)
    return tuple(deduped)


def proxy_candidates_for_env(machine_env: Optional[MachineEnv] = None) -> List[str]:
    """Return ordered proxy path candidates for the given machine environment.

    Resolution order:
      1. $X509_USER_PROXY  environment variable
      2. voms-proxy-info --path  (system tool)

    Hardcoded user paths are intentionally absent — every collaborator has
    a different AFS home and directory layout.

    Callers should warn when the resolved proxy lives under /tmp, since
    Condor worker nodes cannot access a /tmp proxy that belongs to a
    different host.
    """
    machine_env = machine_env or resolve_machine_env("auto")
    candidates: List[str] = []

    env_proxy = os.environ.get("X509_USER_PROXY")
    if env_proxy:
        candidates.append(env_proxy)

    if command_exists("voms-proxy-info"):
        try:
            result = subprocess.run(
                ["voms-proxy-info", "--path"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, timeout=10, check=False,
            )
            if result.returncode == 0:
                voms_path = result.stdout.strip()
                if voms_path and voms_path not in candidates:
                    candidates.append(voms_path)
        except Exception:
            pass

    deduped: List[str] = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def detect_proxy_path(machine_env_name: str = "auto") -> str:
    """Resolve an available X509 proxy path for the current environment.

    Candidates are ordered by ``proxy_candidates_for_env``.  Each candidate
    under ``/tmp`` triggers a warning (Condor worker nodes cannot access a
    submit-host ``/tmp`` directory).  A fatal error is emitted only when
    *no* usable non-/tmp proxy is found.
    """

    try:
        machine_env = resolve_machine_env(machine_env_name)
    except ValueError:
        machine_env = resolve_machine_env("auto")

    candidates = proxy_candidates_for_env(machine_env)

    found_any = False
    good: List[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        if os.path.exists(candidate):
            found_any = True
            if candidate.startswith("/tmp/"):
                print(
                    f"[WARN] 代理 {candidate} 位于 /tmp 下；"
                    f"HTCondor worker 节点无法访问。",
                    file=sys.stderr,
                )
            else:
                good.append(candidate)

    if good:
        return good[0]

    if found_any:
        print(
            "[WARN] 所有存在的代理文件都位于 /tmp 下；"
            "HTCondor worker 节点将无法访问。"
            "请将 X509_USER_PROXY 设置为持久路径（例如 ~/x509up_u$(id -u)）。",
            file=sys.stderr,
        )
        # Return the first /tmp candidate anyway so the caller can inspect it.
        for candidate in candidates:
            if candidate and os.path.exists(candidate):
                return candidate

    # No proxy found — return first candidate for a clear downstream message.
    if candidates:
        return candidates[0]
    return ""


def ensure_local_xrootd_proxy(proxy_path: str) -> str:
    """给本地 xrdfs/xrdcp 准备一个可直接使用的代理副本。"""

    tmp_proxy = f"/tmp/x509up_u{os.getuid()}"
    candidates: List[str] = []
    if tmp_proxy:
        candidates.append(tmp_proxy)
    if proxy_path and proxy_path not in candidates:
        candidates.append(proxy_path)

    for candidate in candidates:
        if not candidate or not os.path.exists(candidate):
            continue
        ok, _, _ = check_proxy_valid(candidate)
        if ok and candidate == tmp_proxy:
            return candidate

    if proxy_path and os.path.exists(proxy_path):
        try:
            shutil.copyfile(proxy_path, tmp_proxy)
            os.chmod(tmp_proxy, 0o600)
            ok, _, _ = check_proxy_valid(tmp_proxy)
            if ok:
                return tmp_proxy
        except OSError:
            pass

    return proxy_path


def command_exists(command: str) -> bool:
    return shutil.which(command) is not None


def load_pool_scan_cache() -> Dict[str, int]:
    """可选地从外部 JSON 读取已知 pool 计数，绕开本地 xrdfs 子进程兼容性问题。"""

    global _POOL_SCAN_CACHE
    if _POOL_SCAN_CACHE is not None:
        return _POOL_SCAN_CACHE

    cache_path = os.environ.get(POOL_SCAN_CACHE_ENV, "").strip()
    if not cache_path:
        _POOL_SCAN_CACHE = {}
        return _POOL_SCAN_CACHE

    try:
        with open(cache_path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, ValueError, TypeError):
        _POOL_SCAN_CACHE = {}
        return _POOL_SCAN_CACHE

    cache: Dict[str, int] = {}
    if isinstance(raw, dict):
        for pool_name, value in raw.items():
            if isinstance(value, dict):
                value = value.get("remote_count")
            try:
                cache[str(pool_name)] = int(value)
            except (TypeError, ValueError):
                continue

    _POOL_SCAN_CACHE = cache
    return _POOL_SCAN_CACHE


def check_proxy_valid(proxy_path: str) -> Tuple[bool, Optional[int], Optional[str]]:
    """返回代理是否可用、剩余秒数以及错误信息。"""

    if not proxy_path or not os.path.exists(proxy_path):
        return False, None, f"代理文件不存在: {proxy_path}"

    if not command_exists("voms-proxy-info"):
        return True, None, None

    try:
        result = subprocess.run(
            ["voms-proxy-info", "-file", proxy_path, "-timeleft"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as exc:
        return False, None, str(exc)

    if result.returncode != 0:
        return False, None, result.stderr.strip() or "voms-proxy-info 返回非零"

    try:
        timeleft = int(result.stdout.strip())
    except ValueError:
        return False, None, f"无法解析代理剩余时间: {result.stdout!r}"

    return timeleft > 0, timeleft, None


def count_lhe_files_on_t2(pool_name: str, proxy_path: str) -> Tuple[int, Optional[str]]:
    """统计远端 pool 内已有的 .lhe 文件数量。"""

    cache = load_pool_scan_cache()
    if pool_name in cache:
        return cache[pool_name], None

    storage_name = pool_storage_name(pool_name)
    local_proxy_path = ensure_local_xrootd_proxy(proxy_path)
    env = os.environ.copy()
    env["X509_USER_PROXY"] = local_proxy_path

    try:
        result = subprocess.run(
            ["xrdfs", EOS_XRDFS_TARGET, "ls", f"{EOS_PATH_BASE}/lhe_pools/{storage_name}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
            check=False,
            env=env,
        )
    except Exception as exc:
        return 0, str(exc)

    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "No such file" in stderr or "Unable to locate" in stderr:
            return 0, None
        return 0, stderr or "xrdfs ls 返回非零"

    count = sum(1 for line in result.stdout.splitlines() if accepts_lhe_ext(line.strip()))
    return count, None


def count_lhe_files_local(pool_name: str, local_output_base: str) -> Tuple[int, Optional[str]]:
    """统计本地 pool 内已有的 .lhe 文件数量。"""

    cache = load_pool_scan_cache()
    cache_key = f"local_{pool_name}"
    if cache_key in cache:
        return cache[cache_key], None

    storage_name = pool_storage_name(pool_name)
    local_pool_dir = os.path.join(local_output_base, "lhe_pools", storage_name)
    try:
        if not os.path.exists(local_pool_dir):
            return 0, None
        count = sum(1 for filename in os.listdir(local_pool_dir) if accepts_lhe_ext(filename))
        return count, None
    except Exception as exc:
        return 0, str(exc)


def _list_remote_dir(eos_path: str, proxy_path: str) -> List[str]:
    """Run xrdfs ls on a remote directory. Returns list of full paths, or empty list."""
    local_proxy_path = ensure_local_xrootd_proxy(proxy_path)
    env = os.environ.copy()
    env["X509_USER_PROXY"] = local_proxy_path
    try:
        result = subprocess.run(
            ["xrdfs", EOS_XRDFS_TARGET, "ls", eos_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=30, check=False, env=env,
        )
    except Exception:
        return []
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def list_lhe_files_remote(pool_name: str, proxy_path: str, existing_lhe_base: str = "") -> List[str]:
    """Discover existing LHE files for a pool by scanning the remote pool directory tree.

    Tries multiple directory layouts:
      1. {base}/lhe_pools/{storage_name}/  (standard production layout)
      2. {base}/LHE_pool/<any_subdir>/     (user's subprocess-named layout)

    Files are matched by the ``sample_{storage_name}_`` prefix.
    Returns sorted list of full XRootD URLs.
    """
    storage_name = pool_storage_name(pool_name)
    base = existing_lhe_base or EOS_BASE
    eos_prefix = f"root://{EOS_HOST}/"

    # Strategy 1: standard layout — single known subdirectory
    for parent in ("lhe_pools", "LHE_pool"):
        std_dir = f"{base}/{parent}/{storage_name}".replace(eos_prefix, "")
        for line in _list_remote_dir(std_dir, proxy_path):
            fname = line.rsplit("/", 1)[-1] if "/" in line else line
            if accepts_lhe_ext(fname) and fname.startswith(f"sample_{storage_name}_"):
                return sorted([f"{eos_prefix}{l.strip()}" for l in [line]])

    # Strategy 2: scan subdirectories of LHE_pool / lhe_pools
    for parent in ("LHE_pool", "lhe_pools"):
        parent_eos = f"{base}/{parent}".replace(eos_prefix, "")
        subdirs = _list_remote_dir(parent_eos, proxy_path)
        if not subdirs:
            continue
        all_files = []
        for subdir in subdirs:
            for line in _list_remote_dir(subdir, proxy_path):
                fname = line.rsplit("/", 1)[-1] if "/" in line else line
                if accepts_lhe_ext(fname) and fname.startswith(f"sample_{storage_name}_"):
                    all_files.append(f"{eos_prefix}{line.strip()}")
        if all_files:
            return sorted(all_files)

    return []


def list_lhe_files_local(pool_name: str, local_output_base: str) -> List[str]:
    """Discover existing LHE files for a pool in the local pool directory tree.

    Tries the same layouts as the remote version.
    Returns sorted list of full local paths.
    """
    storage_name = pool_storage_name(pool_name)
    prefix = f"sample_{storage_name}_"

    for parent in ("lhe_pools", "LHE_pool"):
        parent_dir = os.path.join(local_output_base, parent)
        if not os.path.isdir(parent_dir):
            continue
        # Strategy 1: standard layout — single known subdirectory
        std_dir = os.path.join(parent_dir, storage_name)
        if os.path.isdir(std_dir):
            files = sorted(
                os.path.join(std_dir, f) for f in os.listdir(std_dir)
                if accepts_lhe_ext(f) and f.startswith(prefix)
            )
            if files:
                return files
        # Strategy 2: scan all subdirectories
        try:
            all_files = []
            for subdir_name in sorted(os.listdir(parent_dir)):
                subdir = os.path.join(parent_dir, subdir_name)
                if not os.path.isdir(subdir):
                    continue
                for f in os.listdir(subdir):
                    if accepts_lhe_ext(f) and f.startswith(prefix):
                        all_files.append(os.path.join(subdir, f))
            if all_files:
                return sorted(all_files)
        except Exception:
            pass

    return []


def pool_remote_path(pool_name: str, local_output_base: str = "") -> str:
    if local_output_base:
        return os.path.join(local_output_base, "lhe_pools", pool_storage_name(pool_name))
    return f"{EOS_BASE}/lhe_pools/{pool_storage_name(pool_name)}"


def scan_existing_pools(
    pool_requirements: Dict[str, int],
    proxy_path: str,
    local_output_base: str = "",
) -> Dict[str, Dict[str, object]]:
    """扫描已有 LHE 池，数量不足时视为需要全量重生。"""

    result: Dict[str, Dict[str, object]] = OrderedDict()
    for pool_name, required_count in pool_requirements.items():
        if local_output_base:
            count, error = count_lhe_files_local(pool_name, local_output_base)
        else:
            count, error = count_lhe_files_on_t2(pool_name, proxy_path)
        result[pool_name] = {
            "required_count": required_count,
            "remote_count": count,
            "use_existing": error is None and count >= required_count,
            "error": error,
            "remote_path": pool_remote_path(pool_name, local_output_base),
        }
    return result


def expand_campaign_selection(items: Sequence[str]) -> List[str]:
    """支持 ALL/JJP_ALL/JUP_ALL 和逗号分隔写法。"""

    if not items:
        raise ValueError("至少需要指定一个 campaign")

    resolved: List[str] = []
    for item in items:
        for token in [part.strip() for part in item.split(",") if part.strip()]:
            if token == "ALL":
                resolved.extend(CAMPAIGNS.keys())
            elif token == "JJP_ALL":
                resolved.extend(name for name in CAMPAIGNS if name.startswith("JJP"))
            elif token == "JUP_ALL":
                resolved.extend(name for name in CAMPAIGNS if name.startswith("JUP"))
            elif token in CAMPAIGNS:
                resolved.append(token)
            else:
                raise ValueError(f"未知的 campaign: {token}")

    deduped: List[str] = []
    seen = set()
    for name in resolved:
        if name not in seen:
            deduped.append(name)
            seen.add(name)
    return deduped


def compute_pool_requirements(campaign_names: Sequence[str], jobs_per_campaign: int) -> Dict[str, int]:
    """统计本次 DAG 对每个 LHE pool 的最小文件需求量。"""

    pool_requirements: Dict[str, int] = OrderedDict()
    for campaign_name in campaign_names:
        campaign = CAMPAIGNS[campaign_name]
        for pool_name in campaign.inputs:
            for real_pool_name in real_pool_names(pool_name):
                pool_requirements.setdefault(real_pool_name, 0)
                pool_requirements[real_pool_name] += jobs_per_campaign
    return pool_requirements


def validate_efficiency_campaigns(campaign_names: Sequence[str]) -> None:
    """Efficiency ntuples are currently defined only for JpsiJpsiPhi/JJP."""

    unsupported = [name for name in campaign_names if CAMPAIGNS[name].analysis_type != "JJP"]
    if unsupported:
        raise ValueError(
            "--efficiency-ntuple 目前只支持 JJP/JpsiJpsiPhi campaigns；不支持: "
            + ", ".join(unsupported)
        )


def build_ntuple_manifest(
    campaign_names: Sequence[str] = (),
    jobs_per_campaign: int = 0,
    local_output_base: str = "",
    campaign_jobs_map: Optional[Dict[str, List[int]]] = None,
) -> Dict[str, List[str]]:
    """Build the downstream multileppat efficiency file manifest.

    当提供了 campaign_jobs_map 时，使用其中的 job index 列表；
    否则回退到原有的 range(jobs_per_campaign) 行为。
    """
    manifest: Dict[str, List[str]] = OrderedDict()

    def _job_indices(name: str) -> List[int]:
        if campaign_jobs_map is not None:
            return campaign_jobs_map.get(name, [])
        return list(range(jobs_per_campaign))

    for campaign_name in campaign_names:
        campaign = CAMPAIGNS[campaign_name]
        if campaign.analysis_type != "JJP":
            continue
        if local_output_base:
            manifest[campaign_name] = [
                os.path.join(local_output_base, "output", campaign_name, str(job_index), "output_ntuple.root")
                for job_index in _job_indices(campaign_name)
            ]
        else:
            manifest[campaign_name] = [
                f"{EOS_BASE}/output/{campaign_name}/{job_index}/output_ntuple.root"
                for job_index in _job_indices(campaign_name)
            ]
    return manifest


def bool_string(value: bool) -> str:
    return "true" if value else "false"


def helac_state_is_octet(state: str) -> bool:
    return state.strip().upper().endswith("8")


def helac_matrix_mass(base_mass: float, state: str) -> float:
    if helac_state_is_octet(state):
        return base_mass + HELAC_MATRIX_OCTET_MASS_SHIFT
    return base_mass


def helac_matrix_slug(charm_state: str, bottom_state: str, extra_gluon: bool) -> str:
    suffix = "g" if extra_gluon else "born"
    return f"c{charm_state}_b{bottom_state}_{suffix}"


def helac_matrix_process(charm_state: str, bottom_state: str, extra_gluon: bool) -> str:
    process = f"generate g g > cc~({charm_state}) bb~({bottom_state})"
    if extra_gluon:
        process += " g"
    return process


def display_remote_target(target: str) -> str:
    if target.startswith("root://"):
        return target
    if target.startswith("/eos/"):
        return f"root://eosuser.cern.ch/{target}"
    if target.startswith("/store/"):
        return f"root://{EOS_HOST}/{target}"
    return f"{EOS_BASE}/{target.strip('/')}"


def iter_helac_matrix_jobs(seed_base: int) -> Iterable[Dict[str, object]]:
    index = 0
    for charm_state in HELAC_MATRIX_STATES:
        for bottom_state in HELAC_MATRIX_STATES:
            for extra_gluon in (False, True):
                seed = seed_base + index
                slug = helac_matrix_slug(charm_state, bottom_state, extra_gluon)
                yield {
                    "index": index,
                    "slug": slug,
                    "job_name": f"HELAC_{slug.upper()}",
                    "charm_state": charm_state,
                    "bottom_state": bottom_state,
                    "extra_gluon": extra_gluon,
                    "seed": seed,
                    "process": helac_matrix_process(charm_state, bottom_state, extra_gluon),
                    "charm_octet": helac_state_is_octet(charm_state),
                    "bottom_octet": helac_state_is_octet(bottom_state),
                    "cmass": helac_matrix_mass(HELAC_MATRIX_CHARM_BASE_MASS, charm_state),
                    "bmass": helac_matrix_mass(HELAC_MATRIX_BOTTOM_BASE_MASS, bottom_state),
                }
                index += 1


def dag_escape(value: object) -> str:
    """DAG VARS 使用双引号，这里只做最小必要转义。"""

    text = str(value)
    return text.replace("\\", "\\\\").replace('"', '\\"')


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ensure_submit_visible_output_dir(output_dir: str) -> None:
    """确保输出目录使用持久存储（workfs2 或 AFS），而非临时目录。"""

    normalized = os.path.abspath(output_dir)
    for prefix in ("/tmp/", "/var/tmp/"):
        if normalized.startswith(prefix):
            msg = (
                f"输出目录不能位于 {prefix[:-1]}: {normalized}。"
                "请改用 workfs2 路径，避免节点读取不到作业文件。"
            )
            print(f"警告: {msg}")
            return



def pool_dag_label(pool_name: str) -> str:
    return POOL_DAG_LABELS.get(pool_name, pool_name.replace("pool_", ""))


def build_bundle(bundle_path: str, items: Sequence[Tuple[str, str]]) -> None:
    """把运行时需要的文件打成 tar.gz，worker 侧统一解压后运行。"""

    ensure_dir(os.path.dirname(bundle_path))
    with tarfile.open(bundle_path, "w:gz") as archive:
        for source_path, arcname in items:
            archive.add(source_path, arcname=arcname, recursive=True)


def build_proxy_bundle(output_dir: str, proxy_path: str) -> Tuple[str, str]:
    """把当前代理单独打包，worker 上解压后直接作为 X509_USER_PROXY 使用。"""

    if not proxy_path or not os.path.exists(proxy_path):
        raise FileNotFoundError(f"代理文件不存在，无法打包: {proxy_path}")

    bundle_name = BUNDLE_NAMES["proxy"]
    bundle_path = os.path.join(output_dir, bundle_name)
    build_bundle(bundle_path, ((proxy_path, os.path.join("credentials", "x509_user_proxy")),))
    return bundle_path, bundle_name


def build_compression_bundle(output_dir: str) -> Tuple[str, str]:
    """打包压缩工具及其依赖，worker 解压后从 runtime/tools/ 运行。"""

    bundle_name = BUNDLE_NAMES["compression"]
    bundle_path = os.path.join(output_dir, bundle_name)
    build_bundle(
        bundle_path,
        (
            (os.path.join(BASE_DIR, "tools", "compress_existing_lhe.py"),
             "runtime/tools/compress_existing_lhe.py"),
            (os.path.join(BASE_DIR, "common", "compression_util.py"),
             "runtime/common/compression_util.py"),
        ),
    )
    return bundle_path, bundle_name


def build_tpsonia2mumu_package(output_dir: str) -> Tuple[str, str]:
    """从 git submodule 生成 worker 侧使用的 TPS-Onia2MuMu tarball。"""

    if not os.path.isdir(TPS_ONIA2MUMU_SUBMODULE):
        raise FileNotFoundError(
            "TPS-Onia2MuMu submodule 不存在，请先执行 "
            "`git submodule update --init --recursive`。"
        )

    package_name = "tpsonia2mumu_code.tar.gz"
    package_path = os.path.join(output_dir, package_name)
    source_root = TPS_ONIA2MUMU_SUBMODULE
    arc_root = "HeavyFlavorAnalysis/TPS-Onia2MuMu"

    ensure_dir(output_dir)
    with tarfile.open(package_path, "w:gz") as archive:
        for root, dirs, files in os.walk(source_root):
            rel_root = os.path.relpath(root, source_root)
            dirs[:] = [
                entry
                for entry in dirs
                if entry not in {".git", "__pycache__", "crabData"}
            ]

            if rel_root == ".":
                arc_dir = arc_root
            else:
                arc_dir = os.path.join(arc_root, rel_root)
                archive.add(root, arcname=arc_dir, recursive=False)

            for filename in files:
                if filename in {".git"}:
                    continue
                if filename.endswith((".pyc", ".pyo", ".root")):
                    continue
                source_path = os.path.join(root, filename)
                archive.add(source_path, arcname=os.path.join(arc_dir, filename), recursive=False)

    return package_path, package_name


def build_cmssw15_runtime_tarball(output_dir: Optional[str] = None) -> str:
    """从 CVMFS 和 git submodule 构建预编译的 CMSSW15 ntuple runtime tarball。

    仅在 tarball 缺失时调用，产物写入 common/packages/（或 output_dir），
    后续 DAG 生成直接复用。

    Returns:
        生成的 tarball 路径。
    """

    if output_dir is None:
        output_dir = os.path.dirname(DEFAULT_CMSSW15_RUNTIME_TARBALL)

    ensure_dir(output_dir)
    target_path = os.path.join(output_dir, CMSSW15_RUNTIME_TARBALL_NAME)

    if os.path.isfile(target_path):
        try:
            validate_cmssw15_runtime_tarball(target_path)
            return target_path
        except ValueError:
            os.remove(target_path)

    build_dir = tempfile.mkdtemp(prefix="build_cmssw15_")
    submodule_dir = TPS_ONIA2MUMU_SUBMODULE
    try:
        if not os.path.isdir(submodule_dir):
            raise FileNotFoundError(
                "TPS-Onia2MuMu submodule 不存在，请先执行 "
                "`git submodule update --init --recursive`。"
            )

        print("  [cmssw15-runtime] 创建 CMSSW_15_0_15 项目（通过 CVMFS）...")
        subprocess.run(
            [
                "/bin/bash",
                "-c",
                "source /cvmfs/cms.cern.ch/cmsset_default.sh && "
                f"export SCRAM_ARCH=el9_amd64_gcc12 && "
                f"cd {shlex.quote(build_dir)} && "
                "scramv1 project CMSSW CMSSW_15_0_15",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=600,
        )

        project_dir = os.path.join(build_dir, "CMSSW_15_0_15")
        src_dir = os.path.join(project_dir, "src")
        tps_target = os.path.join(src_dir, "HeavyFlavorAnalysis", "TPS-Onia2MuMu")

        print("  [cmssw15-runtime] 复制 TPS-Onia2MuMu 代码...")
        shutil.copytree(submodule_dir, tps_target, symlinks=True, dirs_exist_ok=True)
        _clean_git_artifacts(tps_target)

        print("  [cmssw15-runtime] 编译 HeavyFlavorAnalysis/TPS-Onia2MuMu ...")
        subprocess.run(
            [
                "/bin/bash",
                "-c",
                "source /cvmfs/cms.cern.ch/cmsset_default.sh && "
                f"export SCRAM_ARCH=el9_amd64_gcc12 && "
                f"cd {shlex.quote(src_dir)} && "
                "eval $(scramv1 runtime -sh) && "
                "scram b clean && "
                "scram b -j 8",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=1800,
        )

        print(f"  [cmssw15-runtime] 打包 {target_path} ...")
        _clean_git_artifacts(project_dir)
        # 清理构建过程中临时创建的 openssl 符号链接
        for name in ("libssl.so", "libcrypto.so"):
            link_path = os.path.join(project_dir, name)
            if os.path.islink(link_path):
                os.unlink(link_path)
        with tarfile.open(target_path, "w:gz") as archive:
            archive.add(project_dir, arcname="CMSSW_15_0_15")

        validate_cmssw15_runtime_tarball(target_path)

        size_mb = os.path.getsize(target_path) / (1024 * 1024)
        print(f"  [cmssw15-runtime] 构建完成 ({size_mb:.0f} MB)")
        return target_path
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def _ensure_openssl_dev_symlinks(project_dir: str) -> None:
    """若无 openssl-devel，在 project_dir 中创建 libssl.so / libcrypto.so 的符号链接。

    部分系统（如 EL9 最小安装）只有带版本号的 .so.3 文件，缺少供 ld 使用的
    未版本化 .so 符号链接，导致 scram 链接阶段失败。
    """
    needed = False
    for lib in ("libssl.so", "libcrypto.so"):
        if not os.path.exists(os.path.join(project_dir, lib)):
            needed = True
            break
    if not needed:
        return

    candidates = (
        "/usr/lib64",
        "/usr/lib/x86_64-linux-gnu",
        "/lib64",
        "/lib/x86_64-linux-gnu",
        "/usr/lib",
    )
    for lib_base in ("libssl", "libcrypto"):
        link_path = os.path.join(project_dir, lib_base + ".so")
        if os.path.exists(link_path):
            continue
        for candidate_dir in candidates:
            for variant in (f"{lib_base}.so.3", f"{lib_base}.so"):
                candidate = os.path.join(candidate_dir, variant)
                if os.path.exists(candidate) and not os.path.islink(link_path):
                    os.symlink(candidate, link_path)
                    break
            if os.path.exists(link_path):
                break


def _clean_git_artifacts(directory: str) -> None:
    """递归删除目录中的 .git / __pycache__ / .pyc 构���品。"""
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in {".git", "__pycache__"}]
        for name in files:
            if name.endswith((".pyc", ".pyo")):
                os.remove(os.path.join(root, name))
    git_dir = os.path.join(directory, ".git")
    if os.path.isdir(git_dir):
        shutil.rmtree(git_dir, ignore_errors=True)


def normalize_tar_name(name: str) -> str:
    return name.lstrip("./").rstrip("/")


def tar_contains_path(member_names: Iterable[str], path: str) -> bool:
    normalized = path.rstrip("/")
    prefix = f"{normalized}/"
    return any(name == normalized or name.startswith(prefix) for name in member_names)


def tar_member_names(path: str) -> List[str]:
    try:
        with tarfile.open(path, "r:gz") as archive:
            return [normalize_tar_name(name) for name in archive.getnames()]
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(f"Cannot read tarball {path}: {exc}") from exc


def validate_cmssw15_runtime_tarball(path: str) -> None:
    """Validate the prebuilt CMSSW15 ntuple runtime contract."""

    member_names = tar_member_names(path)
    missing = [
        member
        for member in CMSSW15_RUNTIME_REQUIRED_MEMBERS
        if not tar_contains_path(member_names, member)
    ]
    if missing:
        raise ValueError(
            "CMSSW15 runtime tarball is missing required paths: "
            + ", ".join(missing)
        )


def inspect_helac_package(path: str) -> Tuple[bool, str]:
    """Return whether helac_package.tar.gz satisfies a usable worker contract."""

    if not os.path.exists(path):
        return False, "missing"

    try:
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getmembers()
    except (tarfile.TarError, OSError) as exc:
        return False, f"invalid tarball: {exc}"

    member_names = [normalize_tar_name(member.name) for member in members]
    has_source_fallback = (
        "HELAC-Onia-2.7.6.tar.gz" in member_names
        and "hepmc2.06.11.tgz" in member_names
    )
    has_prebuilt_helac = tar_contains_path(member_names, "HELAC-Onia-2.7.6/ho_cluster")
    has_prebuilt_hepmc = tar_contains_path(member_names, "HepMC/HepMC-2.06.11/install")
    absolute_symlinks = [
        member.name
        for member in members
        if member.issym() and member.linkname.startswith("/")
    ]

    if has_prebuilt_helac:
        detail = "prebuilt HELAC runtime"
        if has_prebuilt_hepmc:
            detail += " + prebuilt HepMC"
        if absolute_symlinks:
            detail += f"; {len(absolute_symlinks)} absolute symlink(s) will be normalized where known"
        return True, detail

    if has_source_fallback:
        return True, "source fallback tarballs"

    return False, "missing HELAC-Onia-2.7.6/ho_cluster or source fallback tarballs"


def resolve_cmssw15_runtime_tarball(
    path: Optional[str],
    build_if_missing: bool = False,
) -> Optional[str]:
    """Return a CMSSW15 ntuple runtime tarball path when one is available.

    Args:
        path: 用户显式传入的路径（优先级最高）。
        build_if_missing: 若为 True 且 tarball 不存在，则尝试从 CVMFS + submodule 构建。
    """

    if path:
        resolved = os.path.abspath(path)
        if not os.path.isfile(resolved):
            raise FileNotFoundError(f"CMSSW15 runtime tarball does not exist: {resolved}")
        validate_cmssw15_runtime_tarball(resolved)
        return resolved
    if os.path.isfile(DEFAULT_CMSSW15_RUNTIME_TARBALL):
        validate_cmssw15_runtime_tarball(DEFAULT_CMSSW15_RUNTIME_TARBALL)
        return DEFAULT_CMSSW15_RUNTIME_TARBALL
    if build_if_missing and os.path.isdir(TPS_ONIA2MUMU_SUBMODULE):
        return build_cmssw15_runtime_tarball()
    return None


def prepare_runtime_assets(
    output_dir: str,
    require_analysis_package: bool = False,
    cmssw15_runtime_tarball: Optional[str] = None,
    include_ntuple_in_processing: bool = False,
) -> Dict[str, str]:
    """生成 LHE / processing / summary 运行 bundle。"""

    ensure_dir(output_dir)
    assets: Dict[str, str] = OrderedDict()

    # Compile lhe_shuffle_split natively on el9 for planner/coordinator runtime.
    shuffle_split_src = os.path.join(BASE_DIR, "lhe_generation", "lhe_shuffle_split.cc")
    shuffle_split_bin = os.path.join(output_dir, "lhe_shuffle_split")
    print("  [lhe-bundle] Compiling lhe_shuffle_split natively (el9)...")
    subprocess.run(
        ["g++", "-std=c++14", "-O2", "-Wall", "-o", shuffle_split_bin, shuffle_split_src],
        check=True,
        timeout=120,
    )
    print(f"  [lhe-bundle] Compiled: {shuffle_split_bin}")

    lhe_bundle_name = BUNDLE_NAMES["lhe"]
    lhe_bundle_path = os.path.join(output_dir, lhe_bundle_name)
    build_bundle(
        lhe_bundle_path,
        (
            (os.path.join(BASE_DIR, "lhe_generation", "run_helac.sh"), "runtime/lhe_generation/run_helac.sh"),
            (shuffle_split_bin, "runtime/lhe_generation/lhe_shuffle_split"),
            (
                os.path.join(BASE_DIR, "lhe_generation", "input_templates", "user.inp"),
                "runtime/lhe_generation/input_templates/user.inp",
            ),
            (
                os.path.join(BASE_DIR, "lhe_generation", "lhe_pythia6_pythia8.f"),
                "runtime/lhe_generation/lhe_pythia6_pythia8.f",
            ),
            (
                os.path.join(BASE_DIR, "common", "packages", "helac_package.tar.gz"),
                "runtime/lhe_generation/helac_package.tar.gz",
            ),
            (os.path.join(BASE_DIR, "common", "octet_pdg.py"), "runtime/common/octet_pdg.py"),
            (
                os.path.join(BASE_DIR, "common", "compression_helpers.sh"),
                "runtime/common/compression_helpers.sh",
            ),
        ),
    )
    assets["lhe_bundle_path"] = lhe_bundle_path
    assets["lhe_bundle_name"] = lhe_bundle_name

    processing_items: List[Tuple[str, str]] = [
        (os.path.join(BASE_DIR, "processing", "run_chain.sh"), "runtime/processing/run_chain.sh"),
        (
            os.path.join(BASE_DIR, "processing", "pythia_shower"),
            "runtime/processing/pythia_shower",
        ),
        (
            os.path.join(BASE_DIR, "common", "cmssw_configs"),
            "runtime/common/cmssw_configs",
        ),
        (os.path.join(BASE_DIR, "common", "octet_pdg.py"), "runtime/common/octet_pdg.py"),
    ]
    processing_bundle_name = BUNDLE_NAMES["processing"]
    processing_bundle_path = os.path.join(output_dir, processing_bundle_name)
    build_bundle(processing_bundle_path, processing_items)
    assets["processing_bundle_path"] = processing_bundle_path
    assets["processing_bundle_name"] = processing_bundle_name

    if require_analysis_package:
        analysis_package_items: List[Tuple[str, str]] = []
        runtime_tarball = resolve_cmssw15_runtime_tarball(
            cmssw15_runtime_tarball, build_if_missing=True
        )
        if runtime_tarball:
            analysis_package_items.append(
                (
                    runtime_tarball,
                    os.path.join(
                        "runtime",
                        "common",
                        "packages",
                        CMSSW15_RUNTIME_TARBALL_NAME,
                    ),
                )
            )
            assets["cmssw15_runtime_tarball_path"] = runtime_tarball
            assets["cmssw15_runtime_tarball_name"] = CMSSW15_RUNTIME_TARBALL_NAME
        elif os.path.isdir(TPS_ONIA2MUMU_SUBMODULE):
            package_path, package_name = build_tpsonia2mumu_package(output_dir)
            analysis_package_items.append(
                (
                    package_path,
                    os.path.join("runtime", "common", "packages", package_name),
                )
            )
            assets["tpsonia2mumu_package_path"] = package_path
            assets["tpsonia2mumu_package_name"] = package_name
        else:
            raise FileNotFoundError(
                "需要打包 TPS-Onia2MuMu，但既没有预编译 CMSSW15 runtime tarball，"
                "也没有初始化 submodule。请提供 --cmssw15-runtime-tarball，"
                "或执行 `git submodule update --init --recursive`。"
            )

        if include_ntuple_in_processing:
            processing_items.extend(analysis_package_items)
            build_bundle(processing_bundle_path, processing_items)
            return_assets = assets
            return_assets["processing_bundle_path"] = processing_bundle_path
            return_assets["processing_bundle_name"] = processing_bundle_name
            summary_bundle_name = BUNDLE_NAMES["summary"]
            summary_bundle_path = os.path.join(output_dir, summary_bundle_name)
            build_bundle(
                summary_bundle_path,
                (
                    (
                        os.path.join(BASE_DIR, "processing", "templates", "summary.sh"),
                        "runtime/processing/templates/summary.sh",
                    ),
                ),
            )
            return_assets["summary_bundle_path"] = summary_bundle_path
            return_assets["summary_bundle_name"] = summary_bundle_name
            return return_assets

        ntuple_items: List[Tuple[str, str]] = [
            (os.path.join(BASE_DIR, "processing", "run_chain.sh"), "runtime/processing/run_chain.sh"),
            (
                os.path.join(BASE_DIR, "common", "cmssw_configs"),
                "runtime/common/cmssw_configs",
            ),
            (os.path.join(BASE_DIR, "common", "octet_pdg.py"), "runtime/common/octet_pdg.py"),
        ]
        ntuple_items.extend(analysis_package_items)

        ntuple_bundle_name = BUNDLE_NAMES["ntuple"]
        ntuple_bundle_path = os.path.join(output_dir, ntuple_bundle_name)
        build_bundle(ntuple_bundle_path, ntuple_items)
        assets["ntuple_bundle_path"] = ntuple_bundle_path
        assets["ntuple_bundle_name"] = ntuple_bundle_name

    summary_bundle_name = BUNDLE_NAMES["summary"]
    summary_bundle_path = os.path.join(output_dir, summary_bundle_name)
    build_bundle(
        summary_bundle_path,
        (
            (
                os.path.join(BASE_DIR, "processing", "templates", "summary.sh"),
                "runtime/processing/templates/summary.sh",
            ),
        ),
    )
    assets["summary_bundle_path"] = summary_bundle_path
    assets["summary_bundle_name"] = summary_bundle_name

    # Planner bundle (per-pool block planner)
    plan_bundle_name = BUNDLE_NAMES["plan"]
    plan_bundle_path = os.path.join(output_dir, plan_bundle_name)
    plan_items: List[Tuple[str, str]] = [
        (os.path.join(BASE_DIR, "tools", "plan_lhe_blocks.py"),
         "runtime/tools/plan_lhe_blocks.py"),
        (shuffle_split_bin, "runtime/tools/lhe_shuffle_split"),
        (os.path.join(BASE_DIR, "common", "compression_util.py"),
         "runtime/common/compression_util.py"),
    ]
    print("  [bundle] Building planner runtime bundle...")
    build_bundle(plan_bundle_path, plan_items)
    assets["plan_bundle_path"] = plan_bundle_path
    assets["plan_bundle_name"] = plan_bundle_name

    # Coordinator bundle (multi-source campaign-level coordinator)
    coord_bundle_name = BUNDLE_NAMES["coordinate"]
    coord_bundle_path = os.path.join(output_dir, coord_bundle_name)
    coord_items: List[Tuple[str, str]] = [
        (os.path.join(BASE_DIR, "tools", "coordinate_lhe_blocks.py"),
         "runtime/tools/coordinate_lhe_blocks.py"),
        (os.path.join(BASE_DIR, "common", "compression_util.py"),
         "runtime/common/compression_util.py"),
    ]
    print("  [bundle] Building coordinator runtime bundle...")
    build_bundle(coord_bundle_path, coord_items)
    assets["coordinate_bundle_path"] = coord_bundle_path
    assets["coordinate_bundle_name"] = coord_bundle_name

    return assets


def prepare_ntuple_only_assets(
    output_dir: str,
    cmssw15_runtime_tarball: Optional[str] = None,
) -> Dict[str, str]:
    """生成仅含 ntuple runtime 的 bundle（无 LHE/processing/summary）。"""
    ensure_dir(output_dir)
    assets: Dict[str, str] = OrderedDict()

    analysis_package_items: List[Tuple[str, str]] = []
    runtime_tarball = resolve_cmssw15_runtime_tarball(
        cmssw15_runtime_tarball, build_if_missing=True
    )
    if runtime_tarball:
        analysis_package_items.append(
            (
                runtime_tarball,
                os.path.join(
                    "runtime",
                    "common",
                    "packages",
                    CMSSW15_RUNTIME_TARBALL_NAME,
                ),
            )
        )
        assets["cmssw15_runtime_tarball_path"] = runtime_tarball
        assets["cmssw15_runtime_tarball_name"] = CMSSW15_RUNTIME_TARBALL_NAME
    elif os.path.isdir(TPS_ONIA2MUMU_SUBMODULE):
        package_path, package_name = build_tpsonia2mumu_package(output_dir)
        analysis_package_items.append(
            (
                package_path,
                os.path.join("runtime", "common", "packages", package_name),
            )
        )
        assets["tpsonia2mumu_package_path"] = package_path
        assets["tpsonia2mumu_package_name"] = package_name
    else:
        raise FileNotFoundError(
            "需要打包 TPS-Onia2MuMu，但既没有预编译 CMSSW15 runtime tarball，"
            "也没有初始化 submodule。请提供 --cmssw15-runtime-tarball，"
            "或执行 `git submodule update --init --recursive`。"
        )

    ntuple_items: List[Tuple[str, str]] = [
        (os.path.join(BASE_DIR, "processing", "run_chain.sh"), "runtime/processing/run_chain.sh"),
        (
            os.path.join(BASE_DIR, "common", "cmssw_configs"),
            "runtime/common/cmssw_configs",
        ),
        (os.path.join(BASE_DIR, "common", "octet_pdg.py"), "runtime/common/octet_pdg.py"),
    ]
    ntuple_items.extend(analysis_package_items)

    ntuple_bundle_name = BUNDLE_NAMES["ntuple"]
    ntuple_bundle_path = os.path.join(output_dir, ntuple_bundle_name)
    build_bundle(ntuple_bundle_path, ntuple_items)
    assets["ntuple_bundle_path"] = ntuple_bundle_path
    assets["ntuple_bundle_name"] = ntuple_bundle_name

    return assets


class DAGBuilder:
    """负责生成 DAG 内容和元数据。"""

    def __init__(
        self,
        output_dir: str,
        options: WorkflowOptions,
        existing_pools: Dict[str, Dict[str, object]],
        pool_requirements: Dict[str, int],
        runtime_assets: Dict[str, str],
    ):
        self.output_dir = output_dir
        self.options = options
        self.existing_pools = existing_pools
        self.pool_requirements = pool_requirements
        self.runtime_assets = runtime_assets
        self.generated_jobs_by_pool: Dict[str, List[str]] = OrderedDict()
        self.generated_specs_by_pool: Dict[str, List[str]] = OrderedDict()
        self.generated_planners_by_pool: Dict[str, List[str]] = OrderedDict()
        self.allocations_by_pool: Dict[str, int] = OrderedDict()
        self.dag_lines: List[str] = []
        self.metadata: Dict[str, object] = OrderedDict()
        self._existing_lhe_cache: Dict[str, List[str]] = {}

    def seed_for_pool_index(self, pool_name: str, index: int) -> int:
        pool = LHE_POOLS[pool_name]
        seed = 100 + pool.seed_offset + index
        if seed >= 100000:
            seed = 110 + (seed % 80000)
        return seed

    def pool_uses_existing(self, pool_name: str) -> bool:
        info = self.existing_pools.get(pool_name, {})
        return bool(info.get("use_existing"))

    def lhe_resource_request(self) -> Tuple[str, str, str]:
        if self.options.test_mode:
            return "4", "8GB", "8GB"
        return "8", "15GB", "10GB"

    def processing_resource_request(self) -> Tuple[str, str, str]:
        if os.environ.get("PREMIX_INPUT_MODE") == "localcache":
            return "2", "12GB", os.environ.get("PREMIX_LOCALCACHE_REQUEST_DISK", "80GB")
        if self.options.test_mode:
            return "2", "8GB", "4GB"
        return "2", "12GB", "8GB"

    def ntuple_resource_request(self, is_ntuple_only: bool = False) -> Tuple[str, str, str]:
        if self.options.test_mode:
            if is_ntuple_only:
                return "1", "2GB", "2GB"
            return "2", "6GB", "4GB"
        if is_ntuple_only:
            return "1", "2GB", "2GB"
        return "2", "12GB", "8GB"

    def ensure_lhe_jobs(self, pool_name: str, required_count: int) -> None:
        """全局共享同一个 pool 的生成节点，避免跨 campaign 重复生成。"""

        if self.pool_uses_existing(pool_name):
            return

        pool = LHE_POOLS[pool_name]
        if pool.is_composite:
            raise ValueError(f"复合池 {pool_name} 不能直接生成 LHE job")
        jobs = self.generated_jobs_by_pool.setdefault(pool_name, [])
        specs = self.generated_specs_by_pool.setdefault(pool_name, [])

        while len(jobs) < required_count:
            index = len(jobs)
            seed = self.seed_for_pool_index(pool_name, index)
            job_name = f"LHE_{pool_dag_label(pool_name)}_{index}"
            request_cpus, request_memory, request_disk = self.lhe_resource_request()
            jobs.append(job_name)
            specs.append(f"GEN:{pool_name}:{index}:{seed}")
            self.dag_lines.append(
                f"JOB {job_name} {os.path.join(BASE_DIR, self.options.machine_env.lhe_submit_template)}"
            )
            self.dag_lines.append(f"CATEGORY {job_name} lhe")
            self.dag_lines.append(
                "VARS {job} pool=\"{pool}\" seed=\"{seed}\" "
                "min_pt_conia=\"{min_pt_conia}\" min_pt_bonia=\"{min_pt_bonia}\" "
                "min_pt_q=\"{min_pt_q}\" unwevt=\"{unwevt}\" test_mode=\"{test_mode}\" "
                "request_cpus=\"{request_cpus}\" request_memory=\"{request_memory}\" request_disk=\"{request_disk}\" "
                "lhe_bundle_path=\"{lhe_bundle_path}\" lhe_bundle_name=\"{lhe_bundle_name}\" "
                "proxy_bundle_path=\"{proxy_bundle_path}\" proxy_bundle_name=\"{proxy_bundle_name}\" "
                "log_dir=\"{log_dir}\" local_output_base=\"{local_output_base}\" "
                "log_root=\"{log_root}\" compress_lhe=\"{compress_lhe}\" lhe_compression_level=\"{lhe_compression_level}\" "
                "lhe_shuffle_split=\"{lhe_shuffle_split}\" lhe_events_per_block=\"{lhe_events_per_block}\" "
                "lhe_shuffle_mode=\"{lhe_shuffle_mode}\" lhe_n_strata=\"{lhe_n_strata}\" "
                "lhe_drop_incomplete_last_block=\"{lhe_drop_incomplete_last_block}\" "
                "lhe_wrapper_path=\"{lhe_wrapper_path}\" target_machine=\"{target_machine}\"".format(
                    job=job_name,
                    pool=dag_escape(pool.name),
                    seed=dag_escape(seed),
                    min_pt_conia=dag_escape(pool.min_pt_conia),
                    min_pt_bonia=dag_escape(pool.min_pt_bonia),
                    min_pt_q=dag_escape(pool.min_pt_q),
                    unwevt=dag_escape(self.options.resolved_lhe_unwevt()),
                    test_mode=dag_escape(bool_string(self.options.test_mode)),
                    request_cpus=dag_escape(request_cpus),
                    request_memory=dag_escape(request_memory),
                    request_disk=dag_escape(request_disk),
                    lhe_bundle_path=dag_escape(self.runtime_assets["lhe_bundle_path"]),
                    lhe_bundle_name=dag_escape(self.runtime_assets["lhe_bundle_name"]),
                    proxy_bundle_path=dag_escape(self.runtime_assets["proxy_bundle_path"]),
                    proxy_bundle_name=dag_escape(self.runtime_assets["proxy_bundle_name"]),
                    log_dir=dag_escape(self.options.local_log_dir),
                    local_output_base=dag_escape(self.options.local_output_base),
                    compress_lhe=dag_escape(bool_string(self.options.compress_lhe)),
                    lhe_compression_level=dag_escape(self.options.lhe_compression_level),
                    lhe_shuffle_split=dag_escape(bool_string(self.options.lhe_shuffle_split)),
                    lhe_events_per_block=dag_escape(self.options.lhe_events_per_block),
                    lhe_shuffle_mode=dag_escape(self.options.lhe_shuffle_mode),
                    lhe_n_strata=dag_escape(self.options.lhe_n_strata),
                    lhe_drop_incomplete_last_block=dag_escape(bool_string(self.options.lhe_drop_incomplete_last_block)),
                    lhe_wrapper_path=dag_escape(
                        os.path.join(BASE_DIR, "lhe_generation", "condor_wrappers", "run_lhe_gen.sh")
                    ),
                    target_machine=dag_escape(self.options.machine_env.target_machine),
                    log_root=dag_escape(self.options.log_root),
                )
            )
            self.dag_lines.append(f"RETRY {job_name} 2")

    def allocate_input_spec(self, pool_name: str, job_index: int, usage_index: int) -> Tuple[str, List[str]]:
        """为 processing 节点分配输入引用和父节点依赖。"""

        if self.pool_uses_existing(pool_name):
            return f"EOS:{pool_name}:{job_index}:{usage_index}", []

        next_index = self.allocations_by_pool.setdefault(pool_name, 0)
        self.ensure_lhe_jobs(pool_name, next_index + 1)
        job_name = self.generated_jobs_by_pool[pool_name][next_index]
        spec = self.generated_specs_by_pool[pool_name][next_index]
        self.allocations_by_pool[pool_name] = next_index + 1
        return spec, [job_name]

    def add_processing_job(self, campaign_name: str, job_index: int) -> str:
        campaign = CAMPAIGNS[campaign_name]
        input_specs: List[str] = []
        parent_jobs: List[str] = []
        usage_counter: Dict[str, int] = {}

        for pool_name in campaign.inputs:
            usage_index = usage_counter.get(pool_name, 0)
            usage_counter[pool_name] = usage_index + 1
            spec, parents = self.allocate_input_spec(pool_name, job_index, usage_index)
            input_specs.append(spec)
            parent_jobs.extend(parents)

        job_name = f"PROC_{campaign_name}_{job_index}"
        request_cpus, request_memory, request_disk = self.processing_resource_request()
        processing_enable_ntuple = (
            self.options.enable_ntuple if self.options.machine_env.uses_local_storage else False
        )
        self.dag_lines.append(
            f"JOB {job_name} {os.path.join(BASE_DIR, self.options.machine_env.processing_submit_template)}"
        )
        self.dag_lines.append(f"CATEGORY {job_name} processing")
        self.dag_lines.append(
            "VARS {job} campaign=\"{campaign}\" job_id=\"{job_id}\" "
            "inputs=\"{inputs}\" modes=\"{modes}\" analysis=\"{analysis}\" "
            "n_sources=\"{n_sources}\" max_events=\"{max_events}\" "
            "enable_ntuple=\"{enable_ntuple}\" efficiency_ntuple=\"{efficiency_ntuple}\" cleanup=\"{cleanup}\" "
            "request_cpus=\"{request_cpus}\" request_disk=\"{request_disk}\" request_memory=\"{request_memory}\" "
            "shuffle_mixing=\"{shuffle_mixing}\" "
            "processing_bundle_path=\"{processing_bundle_path}\" "
            "processing_bundle_name=\"{processing_bundle_name}\" "
            "proxy_bundle_path=\"{proxy_bundle_path}\" proxy_bundle_name=\"{proxy_bundle_name}\" "
            "log_dir=\"{log_dir}\" local_output_base=\"{local_output_base}\" "
            "log_root=\"{log_root}\" "
            "processing_wrapper_path=\"{processing_wrapper_path}\" target_machine=\"{target_machine}\" "
            "premix_input_mode=\"{premix_input_mode}\" "
            "premix_redirector=\"{premix_redirector}\" "
            "premix_cache_files=\"{premix_cache_files}\" "
            "premix_cache_redirector=\"{premix_cache_redirector}\"".format(
                job=job_name,
                campaign=dag_escape(campaign.name),
                job_id=dag_escape(job_index),
                inputs=dag_escape(",".join(input_specs)),
                modes=dag_escape(",".join(campaign.shower_modes)),
                analysis=dag_escape(campaign.analysis_type),
                n_sources=dag_escape(campaign.n_sources),
                max_events=dag_escape(self.options.max_events),
                enable_ntuple=dag_escape(bool_string(processing_enable_ntuple)),
                efficiency_ntuple=dag_escape(bool_string(self.options.efficiency_ntuple)),
                cleanup=dag_escape(bool_string(self.options.cleanup)),
                request_cpus=dag_escape(request_cpus),
                request_disk=dag_escape(request_disk),
                request_memory=dag_escape(request_memory),
                shuffle_mixing=dag_escape(bool_string(self.options.shuffle_mixing)),
                processing_bundle_path=dag_escape(self.runtime_assets["processing_bundle_path"]),
                processing_bundle_name=dag_escape(self.runtime_assets["processing_bundle_name"]),
                proxy_bundle_path=dag_escape(self.runtime_assets["proxy_bundle_path"]),
                proxy_bundle_name=dag_escape(self.runtime_assets["proxy_bundle_name"]),
                log_dir=dag_escape(self.options.local_log_dir),
                log_root=dag_escape(self.options.log_root),
                local_output_base=dag_escape(self.options.local_output_base),
                processing_wrapper_path=dag_escape(
                    os.path.join(BASE_DIR, "processing", "condor_wrappers", "run_processing.sh")
                ),
                target_machine=dag_escape(self.options.machine_env.target_machine),
                premix_input_mode=dag_escape(os.environ.get("PREMIX_INPUT_MODE", "eoscms")),
                premix_redirector=dag_escape(os.environ.get("PREMIX_REDIRECTOR", "root://eoscms.cern.ch")),
                premix_cache_files=dag_escape(os.environ.get("PREMIX_CACHE_FILES", "1")),
                premix_cache_redirector=dag_escape(
                    os.environ.get("PREMIX_CACHE_REDIRECTOR", os.environ.get("PREMIX_REDIRECTOR", "root://eoscms.cern.ch"))
                ),
            )
        )
        self.dag_lines.append(f"RETRY {job_name} 1")
        if parent_jobs:
            self.dag_lines.append(f"PARENT {' '.join(parent_jobs)} CHILD {job_name}")
        return job_name

    def add_ntuple_job(
        self,
        campaign_name: str,
        job_index: int,
        parent_job: Optional[str] = None,
        miniaod_input: Optional[str] = None,
        is_ntuple_only: bool = False,
        target_eos_base: str = "",
        custom_output_subpath: str = "",
        custom_ntuple_basename: str = "",
    ) -> str:
        campaign = CAMPAIGNS[campaign_name]
        job_name = f"NTUPLE_{campaign_name}_{job_index}"
        request_cpus, request_memory, request_disk = self.ntuple_resource_request(
            is_ntuple_only=is_ntuple_only
        )
        if miniaod_input is None:
            miniaod_input = f"{EOS_OUTPUT}/{campaign.name}/{job_index}/output_MINIAOD.root"
        local_output_base = self.options.local_output_base
        self.dag_lines.append(f"JOB {job_name} {os.path.join(BASE_DIR, 'processing/templates/ntuple.sub')}")
        self.dag_lines.append(f"CATEGORY {job_name} ntuple")
        self.dag_lines.append(
            "VARS {job} campaign=\"{campaign}\" job_id=\"{job_id}\" "
            "analysis=\"{analysis}\" max_events=\"{max_events}\" cleanup=\"{cleanup}\" "
            "efficiency_ntuple=\"{efficiency_ntuple}\" "
            "miniaod_input=\"{miniaod_input}\" "
            "local_output_base=\"{local_output_base}\" "
            "request_cpus=\"{request_cpus}\" request_memory=\"{request_memory}\" request_disk=\"{request_disk}\" "
            "ntuple_bundle_path=\"{ntuple_bundle_path}\" ntuple_bundle_name=\"{ntuple_bundle_name}\" "
            "proxy_bundle_path=\"{proxy_bundle_path}\" proxy_bundle_name=\"{proxy_bundle_name}\" "
            "ntuple_wrapper_path=\"{ntuple_wrapper_path}\" ntuple_wrapper_name=\"{ntuple_wrapper_name}\" "
            "log_root=\"{log_root}\" "
            "target_eos_base=\"{target_eos_base}\" "
            "custom_output_subpath=\"{custom_output_subpath}\" "
            "custom_ntuple_basename=\"{custom_ntuple_basename}\"".format(
                job=job_name,
                campaign=dag_escape(campaign.name),
                job_id=dag_escape(job_index),
                analysis=dag_escape(campaign.analysis_type),
                max_events=dag_escape(self.options.max_events),
                cleanup=dag_escape(bool_string(self.options.cleanup)),
                efficiency_ntuple=dag_escape(bool_string(self.options.efficiency_ntuple)),
                miniaod_input=dag_escape(miniaod_input),
                local_output_base=dag_escape(local_output_base),
                request_cpus=dag_escape(request_cpus),
                request_memory=dag_escape(request_memory),
                request_disk=dag_escape(request_disk),
                ntuple_bundle_path=dag_escape(self.runtime_assets["ntuple_bundle_path"]),
                ntuple_bundle_name=dag_escape(self.runtime_assets["ntuple_bundle_name"]),
                proxy_bundle_path=dag_escape(self.runtime_assets["proxy_bundle_path"]),
                proxy_bundle_name=dag_escape(self.runtime_assets["proxy_bundle_name"]),
                ntuple_wrapper_path=dag_escape(NTUPLE_WRAPPER_PATH),
                ntuple_wrapper_name=dag_escape(NTUPLE_WRAPPER_NAME),
                log_root=dag_escape(self.options.log_root),
                target_eos_base=dag_escape(target_eos_base),
                custom_output_subpath=dag_escape(custom_output_subpath),
                custom_ntuple_basename=dag_escape(custom_ntuple_basename),
            )
        )
        self.dag_lines.append(f"RETRY {job_name} 1")
        if parent_job is not None:
            self.dag_lines.append(f"PARENT {parent_job} CHILD {job_name}")
        return job_name

    # ------------------------------------------------------------------
    # Block SubDAG methods
    # ------------------------------------------------------------------

    def _resolve_lhe_path(self, pool_name: str, seed: int) -> str:
        """Return the full path (EOS URL or local) to the HELAC LHE output."""
        storage = pool_storage_name(pool_name)
        if self.options.machine_env.uses_local_storage and self.options.local_output_base:
            return os.path.join(
                self.options.local_output_base,
                "lhe_pools", storage,
                f"sample_{storage}_{seed}.lhe.gz",
            )
        return f"{EOS_BASE}/lhe_pools/{storage}/sample_{storage}_{seed}.lhe.gz"

    def _resolve_block_output_dir(self, pool_name: str, seed: int) -> str:
        """Return the directory where block .lhe.gz files should be stored."""
        storage = pool_storage_name(pool_name)
        base = self.options.existing_lhe_base or EOS_BASE
        if self.options.machine_env.uses_local_storage and self.options.local_output_base:
            return os.path.join(
                self.options.local_output_base,
                "lhe_pools", storage, "lhe_blocks",
            )
        return f"{base}/lhe_pools/{storage}/lhe_blocks"

    def _resolve_plan_manifest_path(self, pool_name: str, seed: int) -> str:
        """Return the path where the plan manifest JSON will be written."""
        subdir = os.path.join(
            self.output_dir, "plan_subdags",
            pool_dag_label(pool_name),
            f"seed_{seed}",
        )
        return os.path.join(subdir, f"plan_manifest_{pool_name}_{seed}.json")

    def _ensure_skip_lhe_planning_job(self, pool_name: str, job_index: int) -> str:
        """Get or create a planner node for skip-lhe-generation mode.

        Deduplicates across campaigns: the same pool+job_index always maps
        to the same planner node, even when the pool appears in multiple
        campaigns.
        """
        planners = self.generated_planners_by_pool.setdefault(pool_name, [])
        while len(planners) <= job_index:
            planners.append("")
        if planners[job_index]:
            return planners[job_index]
        seed = self.seed_for_pool_index(pool_name, job_index)
        lhe_path = self._resolve_existing_lhe_path(pool_name, job_index, seed)
        plan_job = self.add_planning_job(
            pool_name, job_index, seed, lhe_path_override=lhe_path,
        )
        planners[job_index] = plan_job
        return plan_job

    def _resolve_existing_lhe_path(self, pool_name: str, job_index: int, seed: int) -> str:
        """Discover and return the path to an existing LHE file for the given pool + job_index.

        Scans the remote/local pool directory tree (trying both ``lhe_pools/`` and
        ``LHE_pool/`` layouts). Results are cached per pool to avoid redundant xrdfs
        calls when generating many jobs.
        Falls back to the standard naming convention when listing returns nothing
        (e.g. dry-run without proxy).
        """
        storage = pool_storage_name(pool_name)
        base = self.options.existing_lhe_base or EOS_BASE
        cache_key = f"{pool_name}::{base}"
        if cache_key not in self._existing_lhe_cache:
            if self.options.machine_env.uses_local_storage and self.options.local_output_base:
                files = list_lhe_files_local(pool_name, self.options.local_output_base)
            else:
                files = list_lhe_files_remote(pool_name, self.options.proxy_path, base)
            self._existing_lhe_cache[cache_key] = files
        files = self._existing_lhe_cache[cache_key]
        fallback_dir = f"{base}/lhe_pools/{storage}"
        if self.options.machine_env.uses_local_storage and self.options.local_output_base:
            fallback_dir = os.path.join(self.options.local_output_base, "lhe_pools", storage)
        if job_index < len(files):
            return files[job_index]
        return f"{fallback_dir}/sample_{storage}_{seed}.lhe.gz"

    def add_planning_job(self, pool_name: str, index: int, seed: int,
                         lhe_path_override: str = "") -> str:
        """Emit a per-pool LHE block planner DAG node."""
        pool_label = pool_dag_label(pool_name)
        job_name = f"PLAN_{pool_label}_{index}"
        lhe_path = lhe_path_override or self._resolve_lhe_path(pool_name, seed)
        block_output_dir = self._resolve_block_output_dir(pool_name, seed)
        plan_manifest_path = self._resolve_plan_manifest_path(pool_name, seed)
        output_dir = os.path.dirname(plan_manifest_path)
        shuffle_seed = self.options.lhe_shuffle_seed_base or (seed * 1000 + 37)

        self.dag_lines.append(
            f"JOB {job_name} {os.path.join(BASE_DIR, PLAN_SUBMIT_TEMPLATE)}"
        )
        self.dag_lines.append(f"CATEGORY {job_name} lhe_planning")
        self.dag_lines.append(
            "VARS {job} "
            "plan_wrapper_path=\"{plan_wrapper_path}\" "
            "plan_bundle_path=\"{plan_bundle_path}\" plan_bundle_name=\"{plan_bundle_name}\" "
            "proxy_bundle_path=\"{proxy_bundle_path}\" proxy_bundle_name=\"{proxy_bundle_name}\" "
            "pool=\"{pool}\" seed=\"{seed}\" "
            "lhe_path=\"{lhe_path}\" output_dir=\"{output_dir}\" "
            "events_per_block=\"{events_per_block}\" shuffle_seed=\"{shuffle_seed}\" "
            "shuffle_mode=\"{shuffle_mode}\" n_strata=\"{n_strata}\" "
            "drop_incomplete=\"{drop_incomplete}\" "
            "block_output_dir=\"{block_output_dir}\" "
            "local_output_base=\"{local_output_base}\" "
            "reuse_blocks=\"False\" "
            "manifest_output_path=\"{manifest_output_path}\" "
            "log_root=\"{log_root}\"".format(
                job=job_name,
                plan_wrapper_path=dag_escape(PLAN_WRAPPER_PATH),
                plan_bundle_path=dag_escape(self.runtime_assets["plan_bundle_path"]),
                plan_bundle_name=dag_escape(self.runtime_assets["plan_bundle_name"]),
                proxy_bundle_path=dag_escape(self.runtime_assets["proxy_bundle_path"]),
                proxy_bundle_name=dag_escape(self.runtime_assets["proxy_bundle_name"]),
                pool=dag_escape(pool_name),
                seed=dag_escape(seed),
                lhe_path=dag_escape(lhe_path),
                output_dir=dag_escape(output_dir),
                events_per_block=dag_escape(self.options.lhe_events_per_block),
                shuffle_seed=dag_escape(shuffle_seed),
                shuffle_mode=dag_escape(self.options.lhe_shuffle_mode),
                n_strata=dag_escape(self.options.lhe_n_strata),
                drop_incomplete=dag_escape(bool_string(self.options.lhe_drop_incomplete_last_block)),
                block_output_dir=dag_escape(block_output_dir),
                local_output_base=dag_escape(self.options.local_output_base),
                manifest_output_path=dag_escape(plan_manifest_path),
                log_root=dag_escape(self.options.log_root),
            )
        )
        self.dag_lines.append(f"RETRY {job_name} 2")
        return job_name

    def add_coordinator_job(
        self,
        campaign_name: str,
        job_index: int,
        source_infos: List[Tuple[str, int]],
    ) -> str:
        """Emit a campaign-level LHE block coordinator DAG node (multi-source only)."""
        campaign = CAMPAIGNS[campaign_name]
        job_name = f"COORD_{campaign_name}_{job_index}"

        # Build source_manifests JSON: list of {pool, seed, path}
        source_manifest_entries = []
        for pool_name, seed in source_infos:
            source_manifest_entries.append({
                "pool": pool_name,
                "seed": seed,
                "path": self._resolve_plan_manifest_path(pool_name, seed),
            })
        source_manifests_json = dag_escape(json.dumps(source_manifest_entries))

        subdag_dir = os.path.join(
            self.output_dir, "plan_subdags", campaign_name,
            f"job_{job_index}",
        )
        subdag_output_path = os.path.join(subdag_dir, "blocks_processing.dag")

        request_cpus, request_memory, request_disk = self.processing_resource_request()

        ntuple_sub_template = ""
        ntuple_bundle_path = ""
        ntuple_bundle_name = ""
        ntuple_wrapper_path = ""
        if self.options.enable_ntuple and not self.options.machine_env.uses_local_storage:
            ntuple_sub_template = os.path.join(BASE_DIR, "processing/templates/ntuple.sub")
            ntuple_bundle_path = self.runtime_assets.get("ntuple_bundle_path", "")
            ntuple_bundle_name = self.runtime_assets.get("ntuple_bundle_name", "")
            ntuple_wrapper_path = NTUPLE_WRAPPER_PATH

        self.dag_lines.append(
            f"JOB {job_name} {os.path.join(BASE_DIR, COORDINATE_SUBMIT_TEMPLATE)}"
        )
        self.dag_lines.append(f"CATEGORY {job_name} lhe_coordination")
        self.dag_lines.append(
            "VARS {job} "
            "coord_wrapper_path=\"{coord_wrapper}\" "
            "coord_bundle_path=\"{coord_bundle_path}\" coord_bundle_name=\"{coord_bundle_name}\" "
            "proxy_bundle_path=\"{proxy_bundle_path}\" proxy_bundle_name=\"{proxy_bundle_name}\" "
            "campaign=\"{campaign}\" job_index=\"{job_index}\" "
            "source_manifests=\"{source_manifests}\" "
            "shower_modes=\"{shower_modes}\" analysis_type=\"{analysis_type}\" "
            "n_sources=\"{n_sources}\" max_events=\"{max_events}\" "
            "enable_ntuple=\"{enable_ntuple}\" efficiency_ntuple=\"{efficiency_ntuple}\" "
            "cleanup=\"{cleanup}\" shuffle_mixing=\"{shuffle_mixing}\" "
            "log_root=\"{log_root}\" "
            "request_cpus=\"{request_cpus}\" request_memory=\"{request_memory}\" "
            "request_disk=\"{request_disk}\" "
            "target_machine=\"{target_machine}\" "
            "output_dir=\"{output_dir}\" "
            "processing_sub_template_path=\"{processing_sub_template_path}\" "
            "processing_bundle_path=\"{processing_bundle_path}\" "
            "processing_bundle_name=\"{processing_bundle_name}\" "
            "proxy_bundle_path2=\"{proxy_bundle_path}\" "
            "proxy_bundle_name2=\"{proxy_bundle_name}\" "
            "processing_wrapper_path=\"{processing_wrapper_path}\" "
            "ntuple_sub_template_path=\"{ntuple_sub_template_path}\" "
            "ntuple_bundle_path=\"{ntuple_bundle_path}\" "
            "ntuple_bundle_name=\"{ntuple_bundle_name}\" "
            "ntuple_wrapper_path=\"{ntuple_wrapper_path}\" "
            "subdag_output_path=\"{subdag_output_path}\" "
            "max_block_subdag_jobs=\"{max_block_subdag_jobs}\" "
            "local_output_base=\"{local_output_base}\"".format(
                job=job_name,
                coord_wrapper=dag_escape(COORDINATE_WRAPPER_PATH),
                coord_bundle_path=dag_escape(self.runtime_assets["coordinate_bundle_path"]),
                coord_bundle_name=dag_escape(self.runtime_assets["coordinate_bundle_name"]),
                proxy_bundle_path=dag_escape(self.runtime_assets["proxy_bundle_path"]),
                proxy_bundle_name=dag_escape(self.runtime_assets["proxy_bundle_name"]),
                campaign=dag_escape(campaign_name),
                job_index=dag_escape(job_index),
                source_manifests=source_manifests_json,
                shower_modes=dag_escape(",".join(campaign.shower_modes)),
                analysis_type=dag_escape(campaign.analysis_type),
                n_sources=dag_escape(campaign.n_sources),
                max_events=dag_escape(self.options.max_events),
                enable_ntuple=dag_escape(bool_string(
                    self.options.enable_ntuple and not self.options.machine_env.uses_local_storage
                )),
                efficiency_ntuple=dag_escape(bool_string(self.options.efficiency_ntuple)),
                cleanup=dag_escape(bool_string(self.options.cleanup)),
                shuffle_mixing=dag_escape(bool_string(self.options.shuffle_mixing)),
                log_root=dag_escape(self.options.log_root),
                request_cpus=dag_escape(request_cpus),
                request_memory=dag_escape(request_memory),
                request_disk=dag_escape(request_disk),
                target_machine=dag_escape(self.options.machine_env.target_machine),
                output_dir=dag_escape(subdag_dir),
                processing_sub_template_path=dag_escape(
                    os.path.join(BASE_DIR, self.options.machine_env.processing_submit_template)
                ),
                processing_bundle_path=dag_escape(self.runtime_assets["processing_bundle_path"]),
                processing_bundle_name=dag_escape(self.runtime_assets["processing_bundle_name"]),
                processing_wrapper_path=dag_escape(
                    os.path.join(BASE_DIR, "processing", "condor_wrappers", "run_processing.sh")
                ),
                ntuple_sub_template_path=dag_escape(ntuple_sub_template),
                ntuple_bundle_path=dag_escape(ntuple_bundle_path),
                ntuple_bundle_name=dag_escape(ntuple_bundle_name),
                ntuple_wrapper_path=dag_escape(ntuple_wrapper_path),
                subdag_output_path=dag_escape(subdag_output_path),
                max_block_subdag_jobs=dag_escape(self.options.max_block_subdag_jobs),
                local_output_base=dag_escape(self.options.local_output_base),
            )
        )
        self.dag_lines.append(f"RETRY {job_name} 2")
        return job_name

    def add_block_subdag_node(
        self,
        campaign_name: str,
        job_index: int,
        is_single_source: bool,
        pool_name: str = "",
    ) -> str:
        """Emit a SUBDAG EXTERNAL node for a block processing SubDAG."""
        if is_single_source:
            pool_label = pool_dag_label(pool_name)
            subdag_name = f"PROC_{campaign_name}_{pool_label}_{job_index}"
        else:
            subdag_name = f"MIX_{campaign_name}_{job_index}"

        subdag_path = os.path.join(
            self.output_dir, "plan_subdags", campaign_name,
            f"job_{job_index}", "blocks_processing.dag",
        )
        # Pre-create the directory as a placeholder so DAGMan can validate the path
        os.makedirs(os.path.dirname(subdag_path), exist_ok=True)

        self.dag_lines.append(
            f"SUBDAG EXTERNAL {subdag_name} {subdag_path}"
        )
        return subdag_name

    def build(self, campaign_names: Sequence[str], dag_filename: str) -> str:
        dagman_config_path = os.path.join(self.output_dir, "dagman.config")
        processing_jobs: List[str] = []

        self.dag_lines = [
            "# ================================================",
            "# workbook_v2 MC 生产 DAG",
            f"# 生成时间: {datetime.now().isoformat()}",
            f"# Campaigns: {', '.join(campaign_names)}",
            f"# 每个 campaign 作业数: {self.options.jobs_per_campaign}",
            f"# 测试模式: {bool_string(self.options.test_mode)}",
            "# ================================================",
            "",
            f"CONFIG {dagman_config_path}",
            "",
        ]
        if self.options.maxjobs_lhe > 0:
            self.dag_lines.append(f"MAXJOBS lhe {self.options.maxjobs_lhe}")
        if self.options.maxjobs_processing > 0:
            self.dag_lines.append(f"MAXJOBS processing {self.options.maxjobs_processing}")
        if self.options.enable_ntuple and not self.options.machine_env.uses_local_storage and self.options.maxjobs_ntuple > 0:
            self.dag_lines.append(f"MAXJOBS ntuple {self.options.maxjobs_ntuple}")
        if self.options.enable_lhe_block_subdags:
            self.dag_lines.append(f"MAXJOBS lhe_planning {self.options.maxjobs_lhe}")
            self.dag_lines.append(f"MAXJOBS lhe_coordination {self.options.maxjobs_lhe}")
        self.dag_lines.append("")

        for pool_name, required_count in self.pool_requirements.items():
            self.ensure_lhe_jobs(pool_name, required_count)

        for campaign_name in campaign_names:
            campaign = CAMPAIGNS[campaign_name]
            self.dag_lines.append(f"# -------- Campaign: {campaign.name} --------")
            self.dag_lines.append(f"# {campaign.description}")
            if campaign.notes:
                self.dag_lines.append(f"# 备注: {campaign.notes}")

            use_block_subdags = (
                self.options.enable_lhe_block_subdags
                and not self.options.keep_legacy_single_processing_path
            )

            # Block SubDAGs require freshly generated LHE; existing pools
            # can't provide block files so fall back to legacy processing.
            # When --skip-lhe-generation is set, existing files are expected
            # (planners point at them directly), so skip this guard.
            if use_block_subdags and not self.options.skip_lhe_generation:
                any_existing = any(
                    self.pool_uses_existing(pn) for pn in campaign.inputs
                )
                if any_existing:
                    print(
                        f"[WARNING] Campaign {campaign_name}: some pools use existing LHE. "
                        f"Falling back to legacy flat DAG (block SubDAGs require fresh LHE generation).",
                        file=sys.stderr,
                    )
                    use_block_subdags = False

            if use_block_subdags and self.options.skip_lhe_generation:
                # Short-circuit: skip HELAC generation, use existing LHE files.
                # Planners are deduplicated across campaigns (same pool+index → one node).
                if campaign.n_sources == 1:
                    pool_name = campaign.inputs[0]
                    for job_index in range(self.options.jobs_per_campaign):
                        plan_job = self._ensure_skip_lhe_planning_job(pool_name, job_index)
                        subdag_name = self.add_block_subdag_node(
                            campaign_name, job_index, is_single_source=True,
                            pool_name=pool_name,
                        )
                        self.dag_lines.append(f"PARENT {plan_job} CHILD {subdag_name}")
                        processing_jobs.append(subdag_name)
                else:
                    # Multi-source: planner per unique pool → coordinator → SubDAG
                    for job_index in range(self.options.jobs_per_campaign):
                        plan_jobs = []
                        source_infos = []
                        seen_pools: set = set()
                        for pool_name in campaign.inputs:
                            if pool_name in seen_pools:
                                continue
                            seen_pools.add(pool_name)
                            plan_job = self._ensure_skip_lhe_planning_job(pool_name, job_index)
                            plan_jobs.append(plan_job)
                            seed = self.seed_for_pool_index(pool_name, job_index)
                            source_infos.append((pool_name, seed))
                        coord_job = self.add_coordinator_job(campaign_name, job_index, source_infos)
                        for pj in plan_jobs:
                            self.dag_lines.append(f"PARENT {pj} CHILD {coord_job}")
                        subdag_name = self.add_block_subdag_node(
                            campaign_name, job_index, is_single_source=False,
                        )
                        self.dag_lines.append(f"PARENT {coord_job} CHILD {subdag_name}")
                        processing_jobs.append(subdag_name)

            elif use_block_subdags and campaign.n_sources == 1:
                # Single-source: planner writes SubDAG directly
                pool_name = campaign.inputs[0]
                for job_index in range(self.options.jobs_per_campaign):
                    seed = self.seed_for_pool_index(pool_name, job_index)
                    plan_job = self.add_planning_job(pool_name, job_index, seed)
                    lhe_job = self.generated_jobs_by_pool[pool_name][job_index]
                    self.dag_lines.append(f"PARENT {lhe_job} CHILD {plan_job}")
                    subdag_name = self.add_block_subdag_node(
                        campaign_name, job_index, is_single_source=True,
                        pool_name=pool_name,
                    )
                    self.dag_lines.append(f"PARENT {plan_job} CHILD {subdag_name}")
                    processing_jobs.append(subdag_name)

            elif use_block_subdags and campaign.n_sources >= 2:
                # Multi-source: planner per source → coordinator → SubDAG
                for job_index in range(self.options.jobs_per_campaign):
                    plan_jobs = []
                    source_infos = []
                    seen_pools: set = set()
                    for pool_name in campaign.inputs:
                        if pool_name in seen_pools:
                            continue
                        seen_pools.add(pool_name)
                        seed = self.seed_for_pool_index(pool_name, job_index)
                        plan_job = self.add_planning_job(pool_name, job_index, seed)
                        lhe_job = self.generated_jobs_by_pool[pool_name][job_index]
                        self.dag_lines.append(f"PARENT {lhe_job} CHILD {plan_job}")
                        plan_jobs.append(plan_job)
                        source_infos.append((pool_name, seed))
                    coord_job = self.add_coordinator_job(campaign_name, job_index, source_infos)
                    for pj in plan_jobs:
                        self.dag_lines.append(f"PARENT {pj} CHILD {coord_job}")
                    subdag_name = self.add_block_subdag_node(
                        campaign_name, job_index, is_single_source=False,
                    )
                    self.dag_lines.append(f"PARENT {coord_job} CHILD {subdag_name}")
                    processing_jobs.append(subdag_name)

            else:
                # Legacy flat DAG (current behavior)
                for job_index in range(self.options.jobs_per_campaign):
                    processing_job = self.add_processing_job(campaign_name, job_index)
                    processing_jobs.append(processing_job)
                    if self.options.enable_ntuple and not self.options.machine_env.uses_local_storage:
                        self.add_ntuple_job(campaign_name, job_index, processing_job)

            self.dag_lines.append("")

        if processing_jobs:
            self.dag_lines.append("# -------- 汇总节点 --------")
            self.dag_lines.append(f"FINAL SUMMARY {os.path.join(BASE_DIR, self.options.machine_env.summary_submit_template)}")
            self.dag_lines.append(
                "VARS SUMMARY summary_bundle_path=\"{summary_bundle_path}\" "
                "summary_bundle_name=\"{summary_bundle_name}\" "
                "log_dir=\"{log_dir}\" log_root=\"{log_root}\"".format(
                    summary_bundle_path=dag_escape(self.runtime_assets["summary_bundle_path"]),
                    summary_bundle_name=dag_escape(self.runtime_assets["summary_bundle_name"]),
                    log_dir=dag_escape(self.options.local_log_dir),
                    log_root=dag_escape(self.options.log_root),
                )
            )

        self.metadata = OrderedDict(
            [
                ("created_at", datetime.now().isoformat()),
                ("dag_path", os.path.join(self.output_dir, dag_filename)),
                ("dagman_config_path", dagman_config_path),
                ("options", self.options.to_dict()),
                ("runtime_assets", self.runtime_assets),
                ("campaigns", [CAMPAIGNS[name].to_dict() for name in campaign_names]),
                (
                    "pool_plan",
                    OrderedDict(
                        (
                            pool_name,
                            {
                                "pool": LHE_POOLS[pool_name].to_dict(),
                                "scan": self.existing_pools.get(pool_name, {}),
                                "generated_jobs": list(self.generated_jobs_by_pool.get(pool_name, [])),
                            },
                        )
                        for pool_name in self.existing_pools
                    ),
                ),
                (
                    "ntuple_manifest",
                    build_ntuple_manifest(
                        campaign_names,
                        self.options.jobs_per_campaign,
                        self.options.local_output_base,
                    )
                    if self.options.efficiency_ntuple
                    else OrderedDict(),
                ),
            ]
        )
        return "\n".join(self.dag_lines)

    def build_ntuple_only(
        self,
        campaign_jobs_map: Dict[str, List[int]],
        miniaod_input_fn: Callable[[str, int], str],
        dag_filename: str,
    ) -> str:
        """生成仅含 ntuple 节点的 DAG（无 LHE / processing / summary 节点）。"""
        dagman_config_path = os.path.join(self.output_dir, "dagman.config")
        campaign_names = list(campaign_jobs_map.keys())

        self.dag_lines = [
            "# ================================================",
            "# workbook_v2 Ntuple-only DAG",
            f"# 生成时间: {datetime.now().isoformat()}",
            f"# Campaigns: {', '.join(campaign_names)}",
            "# ================================================",
            "",
            f"CONFIG {dagman_config_path}",
            "",
        ]
        if self.options.maxjobs_ntuple > 0:
            self.dag_lines.append(f"MAXJOBS ntuple {self.options.maxjobs_ntuple}")
        self.dag_lines.append("")

        for campaign_name in campaign_names:
            campaign = CAMPAIGNS[campaign_name]
            job_indices = campaign_jobs_map.get(campaign_name, [])
            self.dag_lines.append(f"# -------- Campaign: {campaign.name} --------")
            self.dag_lines.append(f"# {campaign.description}")
            self.dag_lines.append(f"# Jobs: {len(job_indices)}")
            if campaign.notes:
                self.dag_lines.append(f"# 备注: {campaign.notes}")
            for job_index in job_indices:
                miniaod_path = miniaod_input_fn(campaign_name, job_index)
                target_eos_base = ""
                custom_output_subpath = ""
                custom_ntuple_basename = ""
                if self.options.use_subprocess_naming:
                    subprocess_id = SUBPROCESS_MAP.get(campaign_name, "")
                    if subprocess_id:
                        target_eos_base = self.options.target_base_url or CHIW_EOS_OUTPUT_BASE
                        custom_output_subpath = f"JpsiJpsiPhi/Ntuple/{subprocess_id}"
                        version = self.options.ntuple_version or NTUPLE_VERSION
                        custom_ntuple_basename = f"{subprocess_id}-Ntuple-{version}-{job_index}.root"
                self.add_ntuple_job(
                    campaign_name, job_index,
                    parent_job=None,
                    miniaod_input=miniaod_path,
                    is_ntuple_only=True,
                    target_eos_base=target_eos_base,
                    custom_output_subpath=custom_output_subpath,
                    custom_ntuple_basename=custom_ntuple_basename,
                )
            self.dag_lines.append("")

        # 无 FINAL SUMMARY 节点（没有 processing job 需要汇总）

        self.metadata = OrderedDict(
            [
                ("created_at", datetime.now().isoformat()),
                ("dag_path", os.path.join(self.output_dir, dag_filename)),
                ("dagman_config_path", dagman_config_path),
                ("options", self.options.to_dict()),
                ("runtime_assets", self.runtime_assets),
                ("campaigns", [CAMPAIGNS[name].to_dict() for name in campaign_names]),
                ("ntuple_only", True),
                ("campaign_jobs", {name: list(indices) for name, indices in campaign_jobs_map.items()}),
                (
                    "ntuple_manifest",
                    build_ntuple_manifest(
                        campaign_names,
                        campaign_jobs_map=campaign_jobs_map,
                        local_output_base=self.options.local_output_base,
                    )
                    if self.options.efficiency_ntuple
                    else OrderedDict(),
                ),
            ]
        )
        return "\n".join(self.dag_lines)


def render_dagman_config(options: WorkflowOptions) -> str:
    lines = ["# DAGMan 基础配置"]
    if options.dagman_max_jobs_submitted > 0:
        lines.append(f"DAGMAN_MAX_JOBS_SUBMITTED = {options.dagman_max_jobs_submitted}")
    if options.dagman_max_jobs_idle > 0:
        lines.append(f"DAGMAN_MAX_JOBS_IDLE = {options.dagman_max_jobs_idle}")
    if options.dagman_max_jobs_submitted > 0 or options.dagman_max_jobs_idle > 0:
        lines.extend(
            (
                "DAGMAN_MAX_SUBMITS_PER_INTERVAL = 20",
                "DAGMAN_SUBMIT_DELAY = 1",
            )
        )
    lines.extend(
        (
            "DAGMAN_SUPPRESS_NOTIFICATION = True",
            "DAGMAN_GENERATE_RESCUE_DAG = True",
            "",
        )
    )
    return "\n".join(lines)


def write_generated_files(
    output_dir: str,
    dag_filename: str,
    dag_content: str,
    dagman_config_content: str,
    metadata: Dict[str, object],
) -> Tuple[str, str, str]:
    ensure_dir(output_dir)
    dag_path = os.path.join(output_dir, dag_filename)
    config_path = os.path.join(output_dir, "dagman.config")
    metadata_path = os.path.join(output_dir, "metadata.json")

    with open(dag_path, "w", encoding="utf-8") as handle:
        handle.write(dag_content)

    with open(config_path, "w", encoding="utf-8") as handle:
        handle.write(dagman_config_content)

    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    return dag_path, config_path, metadata_path


def write_ntuple_manifest(output_dir: str, manifest: Dict[str, List[str]]) -> str:
    manifest_path = os.path.join(output_dir, "ntuple_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    return manifest_path


def print_pools() -> None:
    print("\n可用 LHE pools")
    print("=" * 72)
    for pool in LHE_POOLS.values():
        if not pool.public:
            continue
        print(f"- {pool.name}")
        print(f"  描述: {pool.description}")
        print(f"  过程: {pool.process_text}")
        print(
            f"  cuts: min_pt_conia={pool.min_pt_conia}, "
            f"min_pt_bonia={pool.min_pt_bonia}, min_pt_q={pool.min_pt_q}"
        )
        if pool.notes:
            print(f"  备注: {pool.notes}")
        print()


def print_campaigns() -> None:
    print("\n可用 campaigns")
    print("=" * 72)
    for campaign in CAMPAIGNS.values():
        print(f"- {campaign.name}")
        print(f"  分析类型: {campaign.analysis_type}")
        print(f"  输入池: {' + '.join(campaign.inputs)}")
        print(f"  shower 模式: {' / '.join(campaign.shower_modes)}")
        print(f"  描述: {campaign.description}")
        if campaign.notes:
            print(f"  备注: {campaign.notes}")
        print()


def validate_environment(
    campaign_names: Optional[Sequence[str]],
    proxy_path: str,
    scan_existing: bool,
    strict_analysis_packages: bool,
    machine_env: Optional[MachineEnv] = None,
    local_output_base: str = "",
    cmssw15_runtime_tarball: Optional[str] = None,
) -> int:
    machine_env = machine_env or MACHINE_ENVS["lxplus_t2_ihep"]
    if machine_env.uses_local_storage and not local_output_base:
        local_output_base = machine_env.local_output_base
    required = campaign_names or []
    exit_code = 0

    print("环境校验")
    print("=" * 72)
    print(f"Machine env: {machine_env.name}")
    print(f"Submit host: {machine_env.submit_host}")
    print(f"Storage: {machine_env.storage_description}")
    if local_output_base:
        print(f"Local output base: {local_output_base}")
    print()

    print("命令检查:")
    for command in machine_env.required_commands:
        ok = command_exists(command)
        status = "OK" if ok else "缺失"
        print(f"  - {command:<18} {status}")
        if not ok:
            exit_code = 1

    print("\n文件检查:")
    for relative_path in required_files_for_env(machine_env):
        path = os.path.join(BASE_DIR, relative_path)
        ok = os.path.exists(path)
        status = "OK" if ok else "缺失"
        print(f"  - {relative_path:<40} {status}")
        if not ok:
            exit_code = 1

    print("\n包检查:")
    helac_package_path = os.path.join(BASE_DIR, "common", "packages", "helac_package.tar.gz")
    helac_ok, helac_status = inspect_helac_package(helac_package_path)
    print(f"  - {'common/packages/helac_package.tar.gz':<40} {'OK' if helac_ok else '缺失/无效'} ({helac_status})")
    if not helac_ok:
        exit_code = 1

    cmssw15_runtime_path: Optional[str] = None
    cmssw15_runtime_status = "缺失(可选)"
    try:
        cmssw15_runtime_path = resolve_cmssw15_runtime_tarball(cmssw15_runtime_tarball)
        if cmssw15_runtime_path:
            cmssw15_runtime_status = f"OK ({cmssw15_runtime_path})"
    except (FileNotFoundError, ValueError) as exc:
        cmssw15_runtime_status = f"无效: {exc}"
        if strict_analysis_packages or cmssw15_runtime_tarball:
            exit_code = 1

    print(f"  - {CMSSW15_RUNTIME_TARBALL_NAME:<40} {cmssw15_runtime_status}")

    tpsonia2mumu_ok = os.path.isdir(TPS_ONIA2MUMU_SUBMODULE)
    tpsonia2mumu_required = strict_analysis_packages and not cmssw15_runtime_path
    tpsonia2mumu_status = "OK" if tpsonia2mumu_ok else ("缺失(可选)" if not tpsonia2mumu_required else "缺失")
    print(f"  - {'external/TPS-Onia2MuMu':<40} {tpsonia2mumu_status}")
    if tpsonia2mumu_required and not tpsonia2mumu_ok:
        exit_code = 1

    print("\n代理检查:")
    proxy_ok, timeleft, proxy_error = check_proxy_valid(proxy_path)
    if proxy_ok:
        if timeleft is None:
            print(f"  - 代理路径: {proxy_path} (未检查剩余时间)")
        else:
            print(f"  - 代理路径: {proxy_path}")
            print(f"  - 剩余时间: {timeleft} 秒")
    else:
        print(f"  - 代理不可用: {proxy_error or proxy_path}")
        if scan_existing and not local_output_base:
            exit_code = 1

    if required:
        print("\nCampaign 需求:")
        pool_requirements = compute_pool_requirements(required, 1)
        for pool_name, count in pool_requirements.items():
            print(f"  - {pool_name:<24} 至少 {count} 个文件")

        if scan_existing and (proxy_ok or local_output_base):
            scan_target = "本地" if local_output_base else "远端"
            print(f"\n{scan_target} pool 扫描:")
            scan = scan_existing_pools(pool_requirements, proxy_path, local_output_base)
            for pool_name, info in scan.items():
                status = "复用已有文件" if info["use_existing"] else "需要重新生成"
                error = info.get("error")
                suffix = f", 错误={error}" if error else ""
                print(
                    f"  - {pool_name:<24} "
                    f"{scan_target} {info['remote_count']}/{info['required_count']} -> {status}{suffix}"
                )

    print("\n校验结束")
    return exit_code


def discover_ntuple_jobs(
    miniaod_dir: str,
    campaign_names: Sequence[str],
    filename: str = "output_MINIAOD.root",
    max_jobs: int = 0,
) -> Dict[str, List[int]]:
    """扫描本地目录结构，找到可用的 MiniAOD 文件及其 job index。

    期望结构：{miniaod_dir}/{campaign_name}/{job_index}/{filename}
    返回 campaign_name -> 已排序 job index 列表 的映射。
    """
    result: Dict[str, List[int]] = OrderedDict()
    for campaign_name in campaign_names:
        campaign_dir = os.path.join(miniaod_dir, campaign_name)
        if not os.path.isdir(campaign_dir):
            print(f"警告: campaign 目录不存在: {campaign_dir}")
            continue
        indices: List[int] = []
        for entry in os.listdir(campaign_dir):
            try:
                idx = int(entry)
            except ValueError:
                continue
            job_path = os.path.join(campaign_dir, entry)
            if not os.path.isdir(job_path):
                continue
            miniaod_path = os.path.join(job_path, filename)
            if os.path.isfile(miniaod_path):
                indices.append(idx)
        indices.sort()
        if max_jobs > 0:
            indices = indices[:max_jobs]
        if indices:
            result[campaign_name] = indices
        else:
            print(f"警告: campaign {campaign_name} 中没有找到有效的 {filename} 文件")
    return result


def execute_ntuple_only_generation(
    campaign_names: Sequence[str],
    miniaod_dir: str,
    miniaod_base_url: str,
    miniaod_filename: str,
    output_dir: str,
    dag_filename: str,
    options: WorkflowOptions,
    jobs: object,  # int or Dict[str, int] from parse_jobs_arg()
    dry_run: bool,
) -> int:
    """生成仅含 ntuple 重跑节点的 DAG（从已有 MiniAOD 出发）。"""
    if options.strict_vtx_smearing_check:
        result = subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "tools", "check_gensim_vtxsmeared_config.py")],
            check=False,
        )
        if result.returncode != 0:
            print("Error: GEN-SIM vertex smearing check failed.", file=sys.stderr)
            return 1

    output_dir = os.path.abspath(output_dir)
    if not options.local_log_dir:
        options.local_log_dir = os.path.join(output_dir, "logs")
    if not options.log_root:
        options.log_root = options.local_log_dir

    if not dry_run:
        ensure_submit_visible_output_dir(output_dir)
        ensure_dir(options.local_log_dir)
        if options.log_root != options.local_log_dir:
            ensure_dir(options.log_root)

    use_miniaod_dir = bool(miniaod_dir)
    use_miniaod_base_url = bool(miniaod_base_url)

    if use_miniaod_dir and use_miniaod_base_url:
        print("错误: --miniaod-dir 和 --miniaod-base-url 不能同时使用", file=sys.stderr)
        return 1
    if not use_miniaod_dir and not use_miniaod_base_url:
        print("错误: 必须指定 --miniaod-dir 或 --miniaod-base-url", file=sys.stderr)
        return 1

    # Resolve job indices
    if use_miniaod_dir:
        max_jobs_int = jobs if isinstance(jobs, int) else 0
        campaign_jobs_map = discover_ntuple_jobs(
            miniaod_dir, campaign_names, miniaod_filename, max_jobs=max_jobs_int,
        )
    else:
        if not jobs:
            print("错误: 使用 --miniaod-base-url 时必须提供 --jobs", file=sys.stderr)
            return 1
        if isinstance(jobs, dict):
            campaign_jobs_map = OrderedDict()
            for name in campaign_names:
                count = jobs.get(name)
                if count is None:
                    print(f"警告: campaign {name} 未在 --jobs 中指定，跳过", file=sys.stderr)
                    continue
                if count <= 0:
                    continue
                campaign_jobs_map[name] = list(range(count))
        else:
            campaign_jobs_map = OrderedDict(
                (name, list(range(jobs))) for name in campaign_names
            )

    if not campaign_jobs_map:
        print("错误: 没有任何可用的 ntuple job（未找到 MiniAOD 文件）", file=sys.stderr)
        return 1

    total_jobs = sum(len(indices) for indices in campaign_jobs_map.values())
    print(f"发现 {total_jobs} 个 ntuple job，分布在 {len(campaign_jobs_map)} 个 campaign")

    # MiniAOD 输入路径工厂
    def miniaod_input_fn(campaign_name: str, job_index: int) -> str:
        if use_miniaod_dir:
            raw_path = os.path.join(miniaod_dir, campaign_name, str(job_index), miniaod_filename)
            return f"file:{raw_path}"
        else:
            base = miniaod_base_url.rstrip("/")
            return f"{base}/{campaign_name}/{job_index}/{miniaod_filename}"

    # 准备 runtime assets（仅 ntuple bundle + proxy）
    runtime_assets: Dict[str, str]
    if dry_run:
        runtime_assets = {
            "ntuple_bundle_path": "<dry-run>/ntuple_runtime_bundle.tar.gz",
            "ntuple_bundle_name": BUNDLE_NAMES["ntuple"],
            "proxy_bundle_path": "<dry-run>/proxy_bundle.tar.gz",
            "proxy_bundle_name": BUNDLE_NAMES["proxy"],
        }
    else:
        runtime_assets = prepare_ntuple_only_assets(
            output_dir,
            cmssw15_runtime_tarball=options.cmssw15_runtime_tarball,
        )
        proxy_bundle_path, proxy_bundle_name = build_proxy_bundle(output_dir, options.proxy_path)
        runtime_assets["proxy_bundle_path"] = proxy_bundle_path
        runtime_assets["proxy_bundle_name"] = proxy_bundle_name

    # 构建 DAG
    builder = DAGBuilder(
        output_dir=output_dir,
        options=options,
        existing_pools=OrderedDict(),
        pool_requirements=OrderedDict(),
        runtime_assets=runtime_assets,
    )
    dag_content = builder.build_ntuple_only(
        campaign_jobs_map=campaign_jobs_map,
        miniaod_input_fn=miniaod_input_fn,
        dag_filename=dag_filename,
    )

    if dry_run:
        print(dag_content)
        return 0

    dag_path, config_path, metadata_path = write_generated_files(
        output_dir=output_dir,
        dag_filename=dag_filename,
        dag_content=dag_content,
        dagman_config_content=render_dagman_config(options),
        metadata=builder.metadata,
    )

    manifest_path = ""
    if options.efficiency_ntuple:
        manifest_path = write_ntuple_manifest(
            output_dir,
            build_ntuple_manifest(
                campaign_names=list(campaign_jobs_map.keys()),
                campaign_jobs_map=campaign_jobs_map,
                local_output_base=options.local_output_base,
            ),
        )

    print("Ntuple-only DAG 生成完成")
    print(f"  - DAG:              {dag_path}")
    print(f"  - DAGMan 配置:      {config_path}")
    print(f"  - 元数据:           {metadata_path}")
    if manifest_path:
        print(f"  - Ntuple manifest:  {manifest_path}")
    print(f"  - Machine env:      {options.machine_env.name} ({options.machine_env.submit_host})")
    print(f"  - Total ntuple jobs: {total_jobs}")
    print(f"  - 提交命令:          condor_submit_dag {dag_path}")
    return 0


def execute_generation(
    campaign_names: Sequence[str],
    output_dir: str,
    dag_filename: str,
    options: WorkflowOptions,
    dry_run: bool,
) -> int:
    if options.strict_vtx_smearing_check:
        result = subprocess.run(
            [sys.executable, os.path.join(BASE_DIR, "tools", "check_gensim_vtxsmeared_config.py")],
            check=False,
        )
        if result.returncode != 0:
            print("Error: GEN-SIM vertex smearing check failed.", file=sys.stderr)
            return 1

    output_dir = os.path.abspath(output_dir)
    if not options.local_log_dir:
        options.local_log_dir = os.path.join(output_dir, "logs")
    if not options.log_root:
        options.log_root = options.local_log_dir
    local_output_base = options.local_output_base if options.machine_env.uses_local_storage else ""
    split_ntuple = options.enable_ntuple and not options.machine_env.uses_local_storage
    if not dry_run:
        ensure_submit_visible_output_dir(output_dir)
        ensure_dir(options.local_log_dir)
        if options.log_root != options.local_log_dir:
            ensure_dir(options.log_root)
    pool_requirements = compute_pool_requirements(campaign_names, options.jobs_per_campaign)
    if options.skip_lhe_generation:
        # Short-circuit: mark all pools as "existing" so ensure_lhe_jobs()
        # is a no-op. Planners will be fed existing file paths directly.
        existing_pools = OrderedDict(
            (
                pool_name,
                {
                    "required_count": required_count,
                    "remote_count": 0,
                    "use_existing": True,
                    "error": None,
                    "remote_path": pool_remote_path(pool_name, local_output_base),
                },
            )
            for pool_name, required_count in pool_requirements.items()
        )
    elif options.force_generate_lhe:
        existing_pools = OrderedDict(
            (
                pool_name,
                {
                    "required_count": required_count,
                    "remote_count": 0,
                    "use_existing": False,
                    "error": "已禁用远端复用",
                    "remote_path": pool_remote_path(pool_name, local_output_base),
                },
            )
            for pool_name, required_count in pool_requirements.items()
        )
    elif options.scan_existing:
        existing_pools = scan_existing_pools(pool_requirements, options.proxy_path, local_output_base)
    else:
        existing_pools = OrderedDict(
            (
                pool_name,
                {
                    "required_count": required_count,
                    "remote_count": 0,
                    "use_existing": False,
                    "error": None,
                    "remote_path": pool_remote_path(pool_name, local_output_base),
                },
            )
            for pool_name, required_count in pool_requirements.items()
        )

    runtime_assets: Dict[str, str]
    if dry_run:
        runtime_assets = {
            "lhe_bundle_path": "<dry-run>/lhe_runtime_bundle.tar.gz",
            "lhe_bundle_name": BUNDLE_NAMES["lhe"],
            "processing_bundle_path": "<dry-run>/processing_runtime_bundle.tar.gz",
            "processing_bundle_name": BUNDLE_NAMES["processing"],
            "summary_bundle_path": "<dry-run>/summary_runtime_bundle.tar.gz",
            "summary_bundle_name": BUNDLE_NAMES["summary"],
            "proxy_bundle_path": "<dry-run>/proxy_bundle.tar.gz",
            "proxy_bundle_name": BUNDLE_NAMES["proxy"],
            "plan_bundle_path": "<dry-run>/plan_runtime_bundle.tar.gz",
            "plan_bundle_name": BUNDLE_NAMES["plan"],
            "coordinate_bundle_path": "<dry-run>/coordinate_runtime_bundle.tar.gz",
            "coordinate_bundle_name": BUNDLE_NAMES["coordinate"],
        }
        if split_ntuple:
            runtime_assets["ntuple_bundle_path"] = "<dry-run>/ntuple_runtime_bundle.tar.gz"
            runtime_assets["ntuple_bundle_name"] = BUNDLE_NAMES["ntuple"]
    else:
        runtime_assets = prepare_runtime_assets(
            output_dir,
            require_analysis_package=options.enable_ntuple,
            cmssw15_runtime_tarball=options.cmssw15_runtime_tarball,
            include_ntuple_in_processing=options.enable_ntuple and not split_ntuple,
        )
        proxy_bundle_path, proxy_bundle_name = build_proxy_bundle(output_dir, options.proxy_path)
        runtime_assets["proxy_bundle_path"] = proxy_bundle_path
        runtime_assets["proxy_bundle_name"] = proxy_bundle_name

    builder = DAGBuilder(
        output_dir=output_dir,
        options=options,
        existing_pools=existing_pools,
        pool_requirements=pool_requirements,
        runtime_assets=runtime_assets,
    )
    dag_content = builder.build(campaign_names, dag_filename)

    if dry_run:
        print(dag_content)
        return 0

    dag_path, config_path, metadata_path = write_generated_files(
        output_dir=output_dir,
        dag_filename=dag_filename,
        dag_content=dag_content,
        dagman_config_content=render_dagman_config(options),
        metadata=builder.metadata,
    )
    manifest_path = ""
    if options.efficiency_ntuple:
        manifest_path = write_ntuple_manifest(
            output_dir,
            build_ntuple_manifest(campaign_names, options.jobs_per_campaign, local_output_base),
        )

    print("DAG 生成完成")
    print(f"  - DAG: {dag_path}")
    print(f"  - DAGMan 配置: {config_path}")
    print(f"  - 元数据: {metadata_path}")
    if manifest_path:
        print(f"  - Ntuple manifest: {manifest_path}")
    print(f"  - Machine env: {options.machine_env.name} ({options.machine_env.submit_host})")
    print(f"  - 提交命令: condor_submit_dag {dag_path}")
    return 0


def execute_helac_matrix_generation(
    output_dir: str,
    dag_filename: str,
    proxy_path: str,
    seed_base: int,
    stageout_dir: str,
    lhe_unwevt: int,
    test_mode: bool,
    dagman_max_jobs_submitted: int,
    dagman_max_jobs_idle: int,
    log_root: str,
    maxjobs_lhe: int,
    dry_run: bool,
) -> int:
    output_dir = os.path.abspath(output_dir)
    log_root = os.path.abspath(log_root)
    if seed_base <= 10 or seed_base + (len(HELAC_MATRIX_STATES) ** 2 * 2) >= 100000:
        raise ValueError("seed-base must leave all 162 HELAC matrix seeds between 11 and 99999")
    if lhe_unwevt <= 0:
        raise ValueError("--lhe-unwevt must be positive")
    if not dry_run:
        ensure_submit_visible_output_dir(output_dir)
        helac_package_path = os.path.join(BASE_DIR, "common", "packages", "helac_package.tar.gz")
        helac_ok, helac_status = inspect_helac_package(helac_package_path)
        if not helac_ok:
            raise FileNotFoundError(
                "common/packages/helac_package.tar.gz is required for HELAC matrix jobs "
                f"and is not usable: {helac_status}"
            )

    resource_options = WorkflowOptions(
        jobs_per_campaign=1,
        max_events=-1,
        enable_ntuple=False,
        efficiency_ntuple=False,
        cleanup=True,
        test_mode=test_mode,
        scan_existing=False,
        force_generate_lhe=True,
        proxy_path=proxy_path,
        lhe_unwevt=lhe_unwevt,
        dagman_max_jobs_submitted=dagman_max_jobs_submitted,
        dagman_max_jobs_idle=dagman_max_jobs_idle,
        log_root=log_root,
        maxjobs_lhe=maxjobs_lhe,
        maxjobs_processing=0,
        maxjobs_ntuple=0,
        cmssw15_runtime_tarball=None,
    )
    request_cpus, request_memory, request_disk = DAGBuilder(
        output_dir=output_dir,
        options=resource_options,
        existing_pools={},
        pool_requirements={},
        runtime_assets={},
    ).lhe_resource_request()

    if dry_run:
        runtime_assets = {
            "lhe_bundle_path": "<dry-run>/lhe_runtime_bundle.tar.gz",
            "lhe_bundle_name": BUNDLE_NAMES["lhe"],
            "proxy_bundle_path": "<dry-run>/proxy_bundle.tar.gz",
            "proxy_bundle_name": BUNDLE_NAMES["proxy"],
        }
    else:
        ensure_dir(log_root)
        runtime_assets = prepare_runtime_assets(output_dir, require_analysis_package=False)
        proxy_bundle_path, proxy_bundle_name = build_proxy_bundle(output_dir, proxy_path)
        runtime_assets["proxy_bundle_path"] = proxy_bundle_path
        runtime_assets["proxy_bundle_name"] = proxy_bundle_name

    dagman_config_path = os.path.join(output_dir, "dagman.config")
    submit_template = os.path.join(BASE_DIR, "processing", "templates", "helac_matrix.sub")
    matrix_jobs = list(iter_helac_matrix_jobs(seed_base))
    dag_lines = [
        "# ================================================",
        "# HELAC-Onia J/psi + Upsilon Fock-state matrix DAG",
        f"# 生成时间: {datetime.now().isoformat()}",
        f"# Jobs: {len(matrix_jobs)}",
        f"# Stageout: {display_remote_target(stageout_dir)}",
        f"# 测试模式: {bool_string(test_mode)}",
        "# ================================================",
        "",
        f"CONFIG {dagman_config_path}",
        "",
    ]
    if maxjobs_lhe > 0:
        dag_lines.extend([f"MAXJOBS lhe {maxjobs_lhe}", ""])
    else:
        dag_lines.append("")

    for job in matrix_jobs:
        dag_lines.append(
            "# {process}; cmass={cmass:.5f}, bmass={bmass:.5f}".format(
                process=job["process"],
                cmass=job["cmass"],
                bmass=job["bmass"],
            )
        )
        dag_lines.append(f"JOB {job['job_name']} {submit_template}")
        dag_lines.append(f"CATEGORY {job['job_name']} lhe")
        dag_lines.append(
            "VARS {job_name} charm_state=\"{charm_state}\" bottom_state=\"{bottom_state}\" "
            "extra_gluon=\"{extra_gluon}\" job_slug=\"{job_slug}\" seed=\"{seed}\" "
            "stageout_dir=\"{stageout_dir}\" min_pt_conia=\"6.0\" min_pt_bonia=\"4.0\" "
            "min_pt_q=\"0.0\" unwevt=\"{unwevt}\" test_mode=\"{test_mode}\" "
            "request_cpus=\"{request_cpus}\" request_memory=\"{request_memory}\" request_disk=\"{request_disk}\" "
            "lhe_bundle_path=\"{lhe_bundle_path}\" lhe_bundle_name=\"{lhe_bundle_name}\" "
            "proxy_bundle_path=\"{proxy_bundle_path}\" proxy_bundle_name=\"{proxy_bundle_name}\" "
            "log_root=\"{log_root}\"".format(
                job_name=job["job_name"],
                charm_state=dag_escape(job["charm_state"]),
                bottom_state=dag_escape(job["bottom_state"]),
                extra_gluon=dag_escape(bool_string(bool(job["extra_gluon"]))),
                job_slug=dag_escape(job["slug"]),
                seed=dag_escape(job["seed"]),
                stageout_dir=dag_escape(stageout_dir.rstrip("/")),
                unwevt=dag_escape(lhe_unwevt),
                test_mode=dag_escape(bool_string(test_mode)),
                request_cpus=dag_escape(request_cpus),
                request_memory=dag_escape(request_memory),
                request_disk=dag_escape(request_disk),
                lhe_bundle_path=dag_escape(runtime_assets["lhe_bundle_path"]),
                lhe_bundle_name=dag_escape(runtime_assets["lhe_bundle_name"]),
                proxy_bundle_path=dag_escape(runtime_assets["proxy_bundle_path"]),
                proxy_bundle_name=dag_escape(runtime_assets["proxy_bundle_name"]),
                log_root=dag_escape(log_root),
            )
        )
        dag_lines.append(f"RETRY {job['job_name']} 2")
        dag_lines.append("")

    metadata = OrderedDict(
        [
            ("created_at", datetime.now().isoformat()),
            ("dag_path", os.path.join(output_dir, dag_filename)),
            ("dagman_config_path", dagman_config_path),
            (
                "options",
                OrderedDict(
                    [
                        ("seed_base", seed_base),
                        ("stageout_dir", stageout_dir.rstrip("/")),
                        ("lhe_unwevt", lhe_unwevt),
                        ("test_mode", test_mode),
                        ("dagman_max_jobs_submitted", dagman_max_jobs_submitted),
                        ("dagman_max_jobs_idle", dagman_max_jobs_idle),
                        ("log_root", log_root),
                        ("maxjobs_lhe", maxjobs_lhe),
                    ]
                ),
            ),
            ("runtime_assets", runtime_assets),
            ("jobs", matrix_jobs),
        ]
    )
    dag_content = "\n".join(dag_lines)

    if dry_run:
        print(dag_content)
        return 0

    dag_path, config_path, metadata_path = write_generated_files(
        output_dir=output_dir,
        dag_filename=dag_filename,
        dag_content=dag_content,
        dagman_config_content=render_dagman_config(resource_options),
        metadata=metadata,
    )
    print("HELAC matrix DAG 生成完成")
    print(f"  - DAG: {dag_path}")
    print(f"  - DAGMan 配置: {config_path}")
    print(f"  - 元数据: {metadata_path}")
    print(f"  - 作业数: {len(matrix_jobs)}")
    print(f"  - 提交命令: condor_submit_dag {dag_path}")
    return 0


def execute_prepare_runtime(
    output_dir: str,
    proxy_path: str,
    machine_env: Optional[MachineEnv] = None,
    include_ntuple: bool = False,
    cmssw15_runtime_tarball: Optional[str] = None,
) -> int:
    machine_env = machine_env or MACHINE_ENVS["lxplus_t2_ihep"]
    output_dir = os.path.abspath(output_dir)
    ensure_submit_visible_output_dir(output_dir)
    runtime_assets = prepare_runtime_assets(
        output_dir,
        require_analysis_package=include_ntuple,
        cmssw15_runtime_tarball=cmssw15_runtime_tarball,
        include_ntuple_in_processing=include_ntuple and machine_env.uses_local_storage,
    )
    proxy_bundle_path, proxy_bundle_name = build_proxy_bundle(output_dir, proxy_path)
    runtime_assets["proxy_bundle_path"] = proxy_bundle_path
    runtime_assets["proxy_bundle_name"] = proxy_bundle_name
    runtime_assets["machine_env"] = machine_env.to_dict()
    print(json.dumps(runtime_assets, indent=2, ensure_ascii=False))
    return 0


def execute_hepjob_delegate(args: argparse.Namespace) -> int:
    """Use the shared CLI selector while keeping the existing HepJob backend."""

    if getattr(args, "dry_run", False):
        raise ValueError("--dry-run is not supported for machine-env=ihep/HepJob")

    hepjob_script = os.path.join(BASE_DIR, "hepjob_workflow.py")
    command = [sys.executable, hepjob_script, args.command]
    for campaign_arg in args.campaign:
        command.extend(["--campaign", campaign_arg])

    if args.command in {"generate", "generate-test"}:
        command.extend(["--jobs", str(args.jobs)])
        command.extend(["--output-dir", args.output_dir])
        command.extend(["--max-events", str(args.max_events)])
        command.extend(["--proxy-path", args.proxy_path])
        command.extend(["--group", args.hepjob_group])

        if args.enable_ntuple:
            command.append("--enable-ntuple")
        else:
            command.append("--disable-ntuple")
        if args.efficiency_ntuple:
            command.append("--efficiency-ntuple")
        if args.shuffle_mixing:
            command.append("--shuffle-mixing")
        else:
            command.append("--no-shuffle-mixing")

    if args.command == "generate":
        if args.cleanup:
            command.append("--cleanup")
        else:
            command.append("--no-cleanup")
        if args.scan_existing:
            command.append("--scan-existing")
        else:
            command.append("--no-scan-existing")
        if args.force_generate_lhe:
            command.append("--force-generate-lhe")
        if args.lhe_unwevt is not None:
            command.extend(["--lhe-unwevt", str(args.lhe_unwevt)])
        command.extend(["--walltime", args.hepjob_walltime])

    if args.command == "generate-ntuple-only":
        command.extend(["--jobs", str(args.jobs)])
        command.extend(["--output-dir", args.output_dir])
        command.extend(["--max-events", str(args.max_events)])
        command.extend(["--proxy-path", args.proxy_path])
        command.extend(["--group", args.hepjob_group])
        if args.efficiency_ntuple:
            command.append("--efficiency-ntuple")
        if args.cleanup:
            command.append("--cleanup")
        else:
            command.append("--no-cleanup")
        if args.miniaod_dir:
            command.extend(["--miniaod-dir", args.miniaod_dir])
        if args.miniaod_base_url:
            command.extend(["--miniaod-base-url", args.miniaod_base_url])
        if args.miniaod_filename:
            command.extend(["--miniaod-filename", args.miniaod_filename])
        if args.local_output_base:
            command.extend(["--local-output-base", args.local_output_base])
        command.extend(["--walltime", args.hepjob_walltime])

    print("Delegating to HepJob backend:")
    print("  " + " ".join(command))
    result = subprocess.run(command, check=False)
    return result.returncode


def default_test_output_dir() -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(TEST_OUTPUT_DIR, f"batch_{timestamp}")


def add_common_generation_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--machine-env",
        choices=machine_env_choices(),
        default="auto",
        help=(
            "运行环境选择。t2_cn_beijing 是 lxplus_t2_ihep 的别名：在 CERN lxplus 提交，"
            "数据写到 IHEP T2_CN_Beijing。"
        ),
    )
    parser.add_argument(
        "--campaign",
        action="append",
        required=True,
        help="可重复指定，也支持 ALL/JJP_ALL/JUP_ALL 或逗号分隔。",
    )
    parser.add_argument("--jobs", type=int, default=1, help="每个 campaign 的 job 数。")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="输出目录。")
    parser.add_argument("--output", default="mc_production.dag", help="输出 DAG 文件名。")
    parser.add_argument(
        "--lhe-unwevt",
        type=int,
        default=None,
        help="LHE 节点的 unwevt；默认正式模式 100000、测试模式 100。",
    )
    parser.add_argument("--max-events", type=int, default=-1, help="processing 节点的 max-events。")
    parser.add_argument(
        "--compress-lhe",
        action="store_true",
        default=False,
        help="压缩新生成的 LHE 输出为 .lhe.gz。",
    )
    parser.add_argument(
        "--lhe-compression-level",
        type=int,
        default=1,
        choices=range(1, 10),
        metavar="[1-9]",
        help="gzip 压缩级别，默认 1（快速）。",
    )
    parser.add_argument(
        "--lhe-shuffle-split",
        action="store_true",
        default=False,
        help="启用 LHE 分层 shuffle 并按 events-per-block 分块。",
    )
    parser.add_argument(
        "--lhe-events-per-block",
        type=int,
        default=1000,
        help="每个 block 的事件数，默认 1000。",
    )
    parser.add_argument(
        "--lhe-shuffle-mode",
        default="stratified",
        choices=("stratified", "original-order"),
        help="LHE shuffle 模式，默认 stratified。",
    )
    parser.add_argument(
        "--lhe-n-strata",
        default="auto",
        help="分层数，auto 或正整数。",
    )
    parser.add_argument(
        "--lhe-drop-incomplete-last-block",
        action="store_true",
        default=False,
        help="丢弃不完整的最末 block。",
    )
    parser.add_argument(
        "--enable-lhe-block-subdags",
        action="store_true",
        default=False,
        help="启用 LHE block SubDAG 模式：每个 HELAC 作业生成 block 级处理子 DAG。",
    )
    parser.add_argument(
        "--keep-legacy-single-processing-path",
        action="store_true",
        default=False,
        help="强制使用旧版单文件处理路径（flat DAG），即使 --enable-lhe-block-subdags 已设置。",
    )
    parser.add_argument(
        "--lhe-shuffle-seed-base",
        type=int,
        default=None,
        help="LHE shuffle 种子基值；默认从 HELAC seed 派生 (seed * 1000 + 37)。",
    )
    parser.add_argument(
        "--max-block-subdag-jobs",
        type=int,
        default=0,
        help="block SubDAG 内部 MAXJOBS block_processing 节流值；默认与 --maxjobs-processing 相同。",
    )
    parser.add_argument(
        "--skip-lhe-generation",
        action="store_true",
        default=False,
        help="跳过 HELAC LHE 生成；使用池目录中已有的 LHE 文件，配合 --enable-lhe-block-subdags 使用。",
    )
    parser.add_argument(
        "--existing-lhe-base",
        type=str,
        default="",
        help="已有 LHE 文件的基础 URL/路径；设置后会覆盖默认 EOS_BASE。与 --skip-lhe-generation 配合使用。",
    )
    parser.add_argument(
        "--enable-ntuple",
        dest="enable_ntuple",
        action="store_true",
        default=True,
        help="保留 ntuple 步骤。",
    )
    parser.add_argument(
        "--disable-ntuple",
        dest="enable_ntuple",
        action="store_false",
        help="跳过 ntuple，仅保留到 MiniAOD 再做 transfer。",
    )
    parser.add_argument(
        "--efficiency-ntuple",
        action="store_true",
        help="生成 multileppat 效率/acceptance 可用的 JJP full-GEN truth ntuple，并写出 ntuple manifest。",
    )
    parser.add_argument(
        "--shuffle-mixing",
        dest="shuffle_mixing",
        action="store_true",
        default=False,
        help="启用确定性的多输入源 shuffle mixing。",
    )
    parser.add_argument(
        "--no-shuffle-mixing",
        dest="shuffle_mixing",
        action="store_false",
        help="禁用 shuffle mixing（默认顺序 mixing）。",
    )
    parser.add_argument(
        "--cleanup",
        dest="cleanup",
        action="store_true",
        default=True,
        help="作业结束后清理中间文件。",
    )
    parser.add_argument(
        "--no-cleanup",
        dest="cleanup",
        action="store_false",
        help="保留 worker 节点上的中间文件。",
    )
    parser.add_argument(
        "--scan-existing",
        dest="scan_existing",
        action="store_true",
        default=True,
        help="扫描 T2 远端已有的 LHE pool。",
    )
    parser.add_argument(
        "--no-scan-existing",
        dest="scan_existing",
        action="store_false",
        help="不扫描远端，所有 pool 都按需生成。",
    )
    parser.add_argument(
        "--force-generate-lhe",
        action="store_true",
        help="即使远端已有文件，也强制生成本次需要的全部 LHE。",
    )
    parser.add_argument(
        "--proxy-path",
        default=detect_proxy_path(),
        help="X509 代理路径；默认自动探测。",
    )
    parser.add_argument(
        "--dagman-max-jobs-submitted",
        type=int,
        default=2000,
        help="DAGMan 允许同时提交/运行的最大节点数。",
    )
    parser.add_argument(
        "--dagman-max-jobs-idle",
        type=int,
        default=2000,
        help="DAGMan 允许同时处于 idle 状态的最大节点数。",
    )
    parser.add_argument(
        "--local-log-dir",
        default="",
        help="本地 HTCondor 日志目录；未指定时使用 machine-env/log-root 默认值。",
    )
    parser.add_argument(
        "--log-root",
        default="",
        help="HTCondor stdout/stderr/event log 输出目录；未指定时使用 --local-log-dir 或 machine-env 默认。",
    )
    parser.add_argument(
        "--local-output-base",
        default="",
        help="本地输出基础目录；hepthu 默认使用 ~/MC_Production_result。",
    )
    parser.add_argument(
        "--local-condor",
        action="store_true",
        help="快捷方式：等价于 --machine-env local_condor。",
    )
    parser.add_argument(
        "--strict-vtx-smearing-check",
        action="store_true",
        help="在生成 DAG 前运行 GEN-SIM vertex smearing 静态校验。",
    )
    parser.add_argument(
        "--maxjobs-lhe",
        type=int,
        default=2000,
        help="DAGMan LHE category throttle。",
    )
    parser.add_argument(
        "--maxjobs-processing",
        type=int,
        default=2000,
        help="DAGMan MiniAOD/processing category throttle。",
    )
    parser.add_argument(
        "--maxjobs-ntuple",
        type=int,
        default=2000,
        help="DAGMan ntuple category throttle。",
    )
    parser.add_argument(
        "--cmssw15-runtime-tarball",
        default=None,
        help=(
            "预编译 CMSSW_15_0_15 TPS-Onia2MuMu runtime tarball；"
            "默认查找 common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz。"
        ),
    )
    parser.add_argument("--hepjob-group", default="cms", help="machine-env=ihep 时传给 hep_sub 的组名。")
    parser.add_argument(
        "--hepjob-walltime",
        default="test",
        choices=("test", "short", "mid", "long", "special"),
        help="machine-env=ihep 时传给 hep_sub 的 walltime 等级。",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印 DAG，不写文件。")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="workbook_v2 版 MC DAGMan 工作流工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    list_parser = subparsers.add_parser("list", help="列出可用的 pools 或 campaigns")
    list_parser.add_argument(
        "--kind",
        choices=("all", "campaigns", "pools"),
        default="all",
        help="输出类型。",
    )

    validate_parser = subparsers.add_parser("validate", help="校验本地环境与关键文件")
    validate_parser.add_argument(
        "--machine-env",
        choices=machine_env_choices(),
        default="auto",
        help="运行环境选择。",
    )
    validate_parser.add_argument(
        "--campaign",
        action="append",
        help="可选；若提供则额外检查相关 pool 需求。",
    )
    validate_parser.add_argument(
        "--proxy-path",
        default=detect_proxy_path(),
        help="X509 代理路径；默认自动探测。",
    )
    validate_parser.add_argument(
        "--scan-existing",
        action="store_true",
        help="对指定 campaign 扫描远端已有 pool。",
    )
    validate_parser.add_argument(
        "--strict-analysis-packages",
        action="store_true",
        help="要求 ntuple runtime 可用：预编译 CMSSW15 tarball 或 TPS-Onia2MuMu submodule。",
    )
    validate_parser.add_argument(
        "--cmssw15-runtime-tarball",
        default=None,
        help=(
            "预编译 CMSSW_15_0_15 TPS-Onia2MuMu runtime tarball；"
            "存在且有效时可替代 ntuple source submodule。"
        ),
    )
    validate_parser.add_argument(
        "--local-output-base",
        default="",
        help="本地输出基础目录；hepthu 默认使用 ~/MC_Production_result。",
    )
    validate_parser.add_argument(
        "--local-condor",
        action="store_true",
        help="快捷方式：等价于 --machine-env local_condor。",
    )

    runtime_parser = subparsers.add_parser("prepare-runtime", help="生成 worker 运行所需的压缩包")
    runtime_parser.add_argument(
        "--machine-env",
        choices=machine_env_choices(),
        default="auto",
        help="运行环境选择。",
    )
    runtime_parser.add_argument("--output-dir", required=True, help="bundle 输出目录。")
    runtime_parser.add_argument(
        "--proxy-path",
        default=detect_proxy_path(),
        help="要一起打包到 worker 的代理路径。",
    )
    runtime_parser.add_argument(
        "--include-ntuple",
        action="store_true",
        help="同时生成 ntuple runtime bundle。",
    )
    runtime_parser.add_argument(
        "--cmssw15-runtime-tarball",
        default=None,
        help=(
            "预编译 CMSSW_15_0_15 TPS-Onia2MuMu runtime tarball；"
            "默认查找 common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz。"
        ),
    )
    runtime_parser.add_argument(
        "--local-condor",
        action="store_true",
        help="快捷方式：等价于 --machine-env local_condor。",
    )

    generate_parser = subparsers.add_parser("generate", help="生成正式 DAG")
    add_common_generation_arguments(generate_parser)
    generate_parser.set_defaults(test_mode=False)
    generate_parser.add_argument(
        "--test-mode",
        action="store_true",
        default=False,
        help="把 LHE 生成切到 fast-test 模式。",
    )

    test_parser = subparsers.add_parser("generate-test", help="生成小批量测试 DAG")
    add_common_generation_arguments(test_parser)
    test_parser.set_defaults(
        jobs=1,
        output_dir=default_test_output_dir(),
        output="mc_test.dag",
        max_events=5,
        enable_ntuple=False,
        cleanup=True,
        scan_existing=True,
        force_generate_lhe=False,
        test_mode=True,
    )

    matrix_parser = subparsers.add_parser(
        "generate-helac-matrix",
        help="生成 HELAC-only J/psi+Upsilon Fock-state matrix DAG",
    )
    matrix_parser.add_argument(
        "--output-dir",
        default=os.path.join(DEFAULT_OUTPUT_DIR, "helac_matrix"),
        help="输出目录。",
    )
    matrix_parser.add_argument("--output", default="helac_matrix.dag", help="输出 DAG 文件名。")
    matrix_parser.add_argument(
        "--proxy-path",
        default=detect_proxy_path(),
        help="X509 代理路径；默认自动探测。",
    )
    matrix_parser.add_argument(
        "--seed-base",
        type=int,
        default=92000,
        help="162 个 HELAC matrix job 的起始 seed。",
    )
    matrix_parser.add_argument(
        "--stageout-dir",
        default=HELAC_MATRIX_STAGEOUT_DIR,
        help="远端输出目录；支持 root:// URL、/eos/...、/store/... 或 T2 相对目录。",
    )
    matrix_parser.add_argument(
        "--lhe-unwevt",
        type=int,
        default=100000,
        help="每个 HELAC matrix job 的 unwevt。",
    )
    matrix_parser.add_argument(
        "--test-mode",
        action="store_true",
        default=False,
        help="把 HELAC matrix job 切到 fast-test 积分设置。",
    )
    matrix_parser.add_argument(
        "--dagman-max-jobs-submitted",
        type=int,
        default=0,
        help="DAGMan 允许同时提交/运行的最大节点数；0 表示不写该限流配置。",
    )
    matrix_parser.add_argument(
        "--dagman-max-jobs-idle",
        type=int,
        default=0,
        help="DAGMan 允许同时处于 idle 状态的最大节点数；0 表示不写该限流配置。",
    )
    matrix_parser.add_argument(
        "--log-root",
        default=DEFAULT_LOG_ROOT,
        help="HTCondor stdout/stderr/event log 输出目录。",
    )
    matrix_parser.add_argument(
        "--maxjobs-lhe",
        type=int,
        default=0,
        help="DAGMan HELAC matrix category throttle；0 表示不写 MAXJOBS 限流。",
    )
    matrix_parser.add_argument("--dry-run", action="store_true", help="只打印 DAG，不写文件。")

    ntuple_only_parser = subparsers.add_parser(
        "generate-ntuple-only",
        help="从已有 MiniAOD 文件生成仅含 ntuple 重跑节点的 DAG",
    )
    ntuple_only_parser.add_argument(
        "--machine-env",
        choices=machine_env_choices(),
        default="auto",
        help="运行环境选择。",
    )
    ntuple_only_parser.add_argument(
        "--campaign",
        action="append",
        default=None,
        help="可重复指定，支持 ALL/JJP_ALL/JUP_ALL 或逗号分隔。未指定时从 --miniaod-dir 自动发现。",
    )
    ntuple_only_parser.add_argument(
        "--miniaod-dir",
        default="",
        help="本地 MiniAOD 基础目录，包含 campaign_name/job_index/MINIAOD.root 结构。",
    )
    ntuple_only_parser.add_argument(
        "--miniaod-base-url",
        default="",
        help="远端 MiniAOD URL 基础 (e.g. root://cceos.ihep.ac.cn//eos/.../output)，与 --jobs 配合使用。",
    )
    ntuple_only_parser.add_argument(
        "--miniaod-filename",
        default="output_MINIAOD.root",
        help="MiniAOD 文件名（默认: MINIAOD.root）。",
    )
    ntuple_only_parser.add_argument(
        "--jobs",
        default="",
        help="每个 campaign 的 job 数；用于 --miniaod-base-url 时必填。可指定单个整数（所有 campaign 统一）"
             "或逗号分隔的 campaign=count 对（例如 JJP_SPS_CS=999,JJP_DPS1=996）。",
    )
    ntuple_only_parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="输出目录。")
    ntuple_only_parser.add_argument("--output", default="ntuple_only.dag", help="输出 DAG 文件名。")
    ntuple_only_parser.add_argument("--max-events", type=int, default=-1, help="ntuple 节点的 max-events。")
    ntuple_only_parser.add_argument(
        "--efficiency-ntuple",
        action="store_true",
        help="生成 multileppat 效率 ntuple，并写出 ntuple manifest。",
    )
    ntuple_only_parser.add_argument(
        "--cleanup", dest="cleanup", action="store_true", default=True,
        help="作业结束后清理中间文件。",
    )
    ntuple_only_parser.add_argument(
        "--no-cleanup", dest="cleanup", action="store_false",
        help="保留 worker 节点上的中间文件。",
    )
    ntuple_only_parser.add_argument(
        "--local-output-base",
        default="",
        help="本地输出基目录；会通过 LOCAL_OUTPUT_BASE 环境变量传递给 worker。",
    )
    ntuple_only_parser.add_argument(
        "--local-condor",
        action="store_true",
        help="快捷方式：等价于 --machine-env local_condor。",
    )
    ntuple_only_parser.add_argument(
        "--proxy-path",
        default=detect_proxy_path(),
        help="X509 代理路径；默认自动探测。",
    )
    ntuple_only_parser.add_argument(
        "--local-log-dir", default="",
        help="本地 HTCondor 日志目录。",
    )
    ntuple_only_parser.add_argument(
        "--log-root", default="",
        help="HTCondor stdout/stderr/event log 输出目录。",
    )
    ntuple_only_parser.add_argument(
        "--maxjobs-ntuple", type=int, default=30,
        help="DAGMan ntuple category throttle。",
    )
    ntuple_only_parser.add_argument(
        "--cmssw15-runtime-tarball", default=None,
        help="预编译 CMSSW_15_0_15 TPS-Onia2MuMu runtime tarball。",
    )
    ntuple_only_parser.add_argument(
        "--strict-vtx-smearing-check",
        action="store_true",
        help="在生成 DAG 前运行 GEN-SIM vertex smearing 静态校验。",
    )
    ntuple_only_parser.add_argument(
        "--use-subprocess-naming",
        action="store_true",
        help="使用 subprocess ID 命名输出目录和文件 (SPS-JpsiJpsiPhi-LO 等)。",
    )
    ntuple_only_parser.add_argument(
        "--target-base-url",
        default="",
        help="自定义输出 EOS 基地址 (默认: chiw MC_Production_v3)。",
    )
    ntuple_only_parser.add_argument(
        "--ntuple-version",
        default="",
        help="ntuple 文件名中的版本字符串 (默认: v01_06)。",
    )
    ntuple_only_parser.add_argument("--dry-run", action="store_true", help="只打印 DAG，不写文件。")

    return parser


def normalize_args(argv: Sequence[str]) -> Sequence[str]:
    """
    兼容旧接口：
    - --list-campaigns -> list --kind campaigns
    - --list-pools -> list --kind pools
    - --campaign ... -> generate ...
    """

    if len(argv) <= 1:
        return argv

    if argv[1] in {
        "list",
        "validate",
        "prepare-runtime",
        "generate",
        "generate-test",
        "generate-helac-matrix",
        "generate-ntuple-only",
    }:
        return argv

    if "--list-campaigns" in argv[1:]:
        return [argv[0], "list", "--kind", "campaigns"]
    if "--list-pools" in argv[1:]:
        return [argv[0], "list", "--kind", "pools"]
    if "--campaign" in argv[1:]:
        return [argv[0], "generate"] + list(argv[1:])
    return argv


def main(argv: Optional[Sequence[str]] = None) -> int:
    argv = normalize_args(argv or sys.argv)
    parser = build_parser()
    args = parser.parse_args(list(argv[1:]))

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "list":
        if args.kind in ("all", "campaigns"):
            print_campaigns()
        if args.kind in ("all", "pools"):
            print_pools()
        return 0

    if args.command == "validate":
        try:
            machine_env = resolve_machine_env(requested_machine_env_name(args))
        except ValueError as exc:
            parser.error(str(exc))
        campaign_names = expand_campaign_selection(args.campaign) if args.campaign else None
        local_output_base = args.local_output_base or (machine_env.local_output_base if machine_env.uses_local_storage else "")
        return validate_environment(
            campaign_names=campaign_names,
            proxy_path=args.proxy_path,
            scan_existing=args.scan_existing,
            strict_analysis_packages=args.strict_analysis_packages,
            machine_env=machine_env,
            local_output_base=local_output_base,
            cmssw15_runtime_tarball=args.cmssw15_runtime_tarball,
        )

    if args.command == "prepare-runtime":
        try:
            machine_env = resolve_machine_env(requested_machine_env_name(args))
        except ValueError as exc:
            parser.error(str(exc))
        return execute_prepare_runtime(
            output_dir=args.output_dir,
            proxy_path=args.proxy_path,
            machine_env=machine_env,
            include_ntuple=args.include_ntuple,
            cmssw15_runtime_tarball=args.cmssw15_runtime_tarball,
        )

    if args.command == "generate-helac-matrix":
        try:
            return execute_helac_matrix_generation(
                output_dir=args.output_dir,
                dag_filename=args.output,
                proxy_path=args.proxy_path,
                seed_base=args.seed_base,
                stageout_dir=args.stageout_dir,
                lhe_unwevt=args.lhe_unwevt,
                test_mode=args.test_mode,
                dagman_max_jobs_submitted=args.dagman_max_jobs_submitted,
                dagman_max_jobs_idle=args.dagman_max_jobs_idle,
                log_root=args.log_root,
                maxjobs_lhe=args.maxjobs_lhe,
                dry_run=args.dry_run,
            )
        except (OSError, ValueError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1

    if args.command in {"generate", "generate-test"}:
        try:
            machine_env = resolve_machine_env(requested_machine_env_name(args))
        except ValueError as exc:
            parser.error(str(exc))
        campaign_names = expand_campaign_selection(args.campaign)
        if args.efficiency_ntuple:
            try:
                validate_efficiency_campaigns(campaign_names)
            except ValueError as exc:
                parser.error(str(exc))
            args.enable_ntuple = True
        if machine_env.is_hepjob:
            try:
                return execute_hepjob_delegate(args)
            except ValueError as exc:
                parser.error(str(exc))
        local_output_base = args.local_output_base or (machine_env.local_output_base if machine_env.uses_local_storage else "")
        options = WorkflowOptions(
            jobs_per_campaign=args.jobs,
            max_events=args.max_events,
            enable_ntuple=args.enable_ntuple,
            efficiency_ntuple=args.efficiency_ntuple,
            cleanup=args.cleanup,
            test_mode=args.test_mode,
            scan_existing=args.scan_existing,
            force_generate_lhe=args.force_generate_lhe,
            proxy_path=args.proxy_path,
            lhe_unwevt=args.lhe_unwevt,
            dagman_max_jobs_submitted=args.dagman_max_jobs_submitted,
            dagman_max_jobs_idle=args.dagman_max_jobs_idle,
            machine_env=machine_env,
            local_log_dir=args.local_log_dir,
            local_output_base=local_output_base,
            log_root=os.path.abspath(args.log_root) if args.log_root else "",
            maxjobs_lhe=args.maxjobs_lhe,
            maxjobs_processing=args.maxjobs_processing,
            maxjobs_ntuple=args.maxjobs_ntuple,
            cmssw15_runtime_tarball=args.cmssw15_runtime_tarball,
            shuffle_mixing=args.shuffle_mixing,
            strict_vtx_smearing_check=args.strict_vtx_smearing_check,
            compress_lhe=args.compress_lhe,
            lhe_compression_level=args.lhe_compression_level,
            lhe_shuffle_split=args.lhe_shuffle_split,
            lhe_events_per_block=args.lhe_events_per_block,
            lhe_shuffle_mode=args.lhe_shuffle_mode,
            lhe_n_strata=args.lhe_n_strata,
            lhe_drop_incomplete_last_block=args.lhe_drop_incomplete_last_block,
            enable_lhe_block_subdags=args.enable_lhe_block_subdags,
            keep_legacy_single_processing_path=args.keep_legacy_single_processing_path,
            lhe_shuffle_seed_base=args.lhe_shuffle_seed_base,
            max_block_subdag_jobs=args.max_block_subdag_jobs,
            skip_lhe_generation=args.skip_lhe_generation,
            existing_lhe_base=args.existing_lhe_base,
        )
        return execute_generation(
            campaign_names=campaign_names,
            output_dir=args.output_dir,
            dag_filename=args.output,
            options=options,
            dry_run=args.dry_run,
        )

    if args.command == "generate-ntuple-only":
        try:
            machine_env = resolve_machine_env(requested_machine_env_name(args))
        except ValueError as exc:
            parser.error(str(exc))

        # Resolve campaign names
        if args.campaign:
            campaign_names = expand_campaign_selection(args.campaign)
        elif args.miniaod_dir:
            # Auto-discover campaigns from directory
            if not os.path.isdir(args.miniaod_dir):
                parser.error(f"--miniaod-dir 目录不存在: {args.miniaod_dir}")
            campaign_names = sorted([
                name for name in os.listdir(args.miniaod_dir)
                if os.path.isdir(os.path.join(args.miniaod_dir, name))
            ])
            if not campaign_names:
                parser.error(f"--miniaod-dir {args.miniaod_dir} 中没有找到 campaign 子目录")
        else:
            parser.error("需要 --campaign 或 --miniaod-dir")

        if args.efficiency_ntuple:
            try:
                validate_efficiency_campaigns(campaign_names)
            except ValueError as exc:
                parser.error(str(exc))

        if machine_env.is_hepjob:
            try:
                return execute_hepjob_delegate(args)
            except ValueError as exc:
                parser.error(str(exc))

        local_output_base = args.local_output_base or (
            machine_env.local_output_base if machine_env.uses_local_storage else ""
        )
        options = WorkflowOptions(
            jobs_per_campaign=0,  # ntuple-only 模式不使用
            max_events=args.max_events,
            enable_ntuple=True,
            efficiency_ntuple=args.efficiency_ntuple,
            cleanup=args.cleanup,
            test_mode=False,
            scan_existing=False,
            force_generate_lhe=False,
            proxy_path=args.proxy_path,
            lhe_unwevt=None,
            dagman_max_jobs_submitted=0,
            dagman_max_jobs_idle=0,
            machine_env=machine_env,
            local_log_dir=args.local_log_dir,
            local_output_base=local_output_base,
            log_root=os.path.abspath(args.log_root) if args.log_root else "",
            maxjobs_lhe=0,
            maxjobs_processing=0,
            maxjobs_ntuple=args.maxjobs_ntuple,
            cmssw15_runtime_tarball=args.cmssw15_runtime_tarball,
            shuffle_mixing=False,
            strict_vtx_smearing_check=args.strict_vtx_smearing_check,
            compress_lhe=False,
            lhe_compression_level=1,
            lhe_shuffle_split=False,
            lhe_events_per_block=1000,
            lhe_shuffle_mode="stratified",
            lhe_n_strata="auto",
            lhe_drop_incomplete_last_block=False,
            use_subprocess_naming=args.use_subprocess_naming,
            target_base_url=args.target_base_url,
            ntuple_version=args.ntuple_version,
            enable_lhe_block_subdags=False,
            keep_legacy_single_processing_path=False,
            lhe_shuffle_seed_base=None,
            max_block_subdag_jobs=0,
        )
        return execute_ntuple_only_generation(
            campaign_names=campaign_names,
            miniaod_dir=args.miniaod_dir,
            miniaod_base_url=args.miniaod_base_url,
            miniaod_filename=args.miniaod_filename,
            output_dir=args.output_dir,
            dag_filename=args.output,
            options=options,
            jobs=parse_jobs_arg(args.jobs),
            dry_run=args.dry_run,
        )

    parser.error(f"未知命令: {args.command}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
