"""Explicit stage runtimes; host discovery never selects physics software."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os
import shlex
import subprocess

@dataclass(frozen=True)
class RuntimeProfile:
    identifier: str
    image: str
    scram_arch: str = ""
    cmssw_base: str = ""
    lcg_view: str = ""

IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/"
# CVMFS CA bundle for GSI proxy auth inside containers (worker images have no
# /etc/grid-security/certificates and inherit no X509_CERT_DIR under getenv=False).
GRID_CA_DIR = "/cvmfs/grid.cern.ch/etc/grid-security/certificates"
PROFILES = {
    "helac-el7": RuntimeProfile("helac-el7", IMAGE+"el7:x86_64",
        lcg_view="/cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh"),
    # Native el9 ships XRootD 5.9.7 with working GSI; LCG_109a's 5.9.1 breaks
    # GSI proxy auth ("ztn disallowed for non-TLS connections"), so run bare.
    "utilities-el9": RuntimeProfile("utilities-el9", IMAGE+"el9:x86_64"),
    "pythia-el8": RuntimeProfile("pythia-el8", IMAGE+"el8:x86_64", "el8_amd64_gcc10",
        "/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw/CMSSW_12_4_14"),
    "cmssw12-sim-el8": RuntimeProfile("cmssw12-sim-el8", IMAGE+"el8:x86_64", "el8_amd64_gcc10",
        "/cvmfs/cms.cern.ch/el8_amd64_gcc10/cms/cmssw/CMSSW_12_4_14"),
    "cmssw15-sim-el9": RuntimeProfile("cmssw15-sim-el9", IMAGE+"el9:x86_64", "el9_amd64_gcc12",
        "/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_15_0_15"),
    "cmssw15-ntuple-el9": RuntimeProfile("cmssw15-ntuple-el9", IMAGE+"el9:x86_64", "el9_amd64_gcc12",
        "/cvmfs/cms.cern.ch/el9_amd64_gcc12/cms/cmssw/CMSSW_15_0_15"),
    "analyzer-el9": RuntimeProfile("analyzer-el9", IMAGE+"el9:x86_64",
        lcg_view="/cvmfs/sft.cern.ch/lcg/views/LCG_109a/x86_64-el9-gcc13-opt/setup.sh"),
}
PROFILES.update({
    # Compatibility names are retained for already prepared non-schema-v2
    # runtime descriptors; new task JSON uses the explicit role names above.
    "cmssw12-el8": PROFILES["cmssw12-sim-el8"],
    "cmssw15-el9": PROFILES["cmssw15-ntuple-el9"],
})
STAGE_RUNTIME = dict(lhe="helac-el7", plan="utilities-el9", mix="pythia-el8",
    miniaod="cmssw12-sim-el8", merge="cmssw12-sim-el8", ntuple="cmssw15-ntuple-el9",
    selection="analyzer-el9", efficiency="analyzer-el9", archive="utilities-el9",
    reclaim="utilities-el9")


def runtime_for_profile(profile, role: str) -> str:
    """Return the runtime role frozen by a detector profile."""
    if role == "simulation":
        identifier = profile.get("simulation_runtime")
    elif role == "merge":
        identifier = profile.get("merge_runtime")
    elif role == "ntuple":
        identifier = profile.get("ntuple_runtime")
    else:
        raise ValueError(f"unknown detector runtime role: {role}")
    if not identifier:
        raise ValueError(f"detector profile lacks {role} runtime")
    resolve_profile(identifier)
    return identifier

def resolve_profile(identifier):
    if identifier not in PROFILES:
        raise ValueError(f"unknown runtime profile: {identifier}")
    return PROFILES[identifier]

def profile_shell(profile, command, *, cwd):
    setup = ["set -eo pipefail"]
    if profile.lcg_view:
        setup.append("source " + shlex.quote(profile.lcg_view))
    if profile.cmssw_base:
        setup += ["source /cvmfs/cms.cern.ch/cmsset_default.sh",
                  "export SCRAM_ARCH=" + shlex.quote(profile.scram_arch),
                  "cd " + shlex.quote(profile.cmssw_base + "/src"),
                  'eval "$(scramv1 runtime -sh)"']
    setup += ["cd " + shlex.quote(str(cwd)), "exec " + shlex.join(list(map(str, command)))]
    return "\n".join(setup)

def run_profile(profile, command, *, cwd, env=None, check=True, stdout=None, stderr=None,
                bind_paths=()):
    selected = resolve_profile(profile) if isinstance(profile, str) else profile
    cwd = Path(cwd).resolve()
    cwd.mkdir(parents=True, exist_ok=True)
    (cwd / "tmp").mkdir(exist_ok=True)
    inherited = {k: os.environ[k] for k in
                 ("X509_USER_PROXY", "KRB5CCNAME", "X509_CERT_DIR") if k in os.environ}
    inherited.setdefault("X509_CERT_DIR", GRID_CA_DIR)
    inherited.update({str(k): str(v) for k, v in (env or {}).items()})
    shell = profile_shell(selected, command, cwd=cwd)
    if os.environ.get("FULL_MC_RUNTIME_PROFILE") == selected.identifier:
        argv = ["/bin/bash", "--noprofile", "--norc", "-c", shell]
    else:
        argv = ["apptainer", "exec", "--cleanenv", "--bind", "/cvmfs:/cvmfs",
                "--bind", str(cwd.parent), "--bind", str(Path(__file__).resolve().parents[3])]
        extra = {str(Path(path).resolve()) for path in bind_paths}
        proxy = inherited.get("X509_USER_PROXY")
        if proxy:
            extra.add(str(Path(proxy).resolve().parent))
        for path in sorted(extra):
            argv += ["--bind", path]
        for key, value in inherited.items():
            argv += ["--env", f"{key}={value}"]
        argv += ["--env", f"FULL_MC_RUNTIME_PROFILE={selected.identifier}",
                 selected.image, "/bin/bash", "--noprofile", "--norc", "-c", shell]
    environment = dict(os.environ, **inherited)
    return subprocess.run(argv, cwd=cwd, env=environment, text=True,
                          stdout=stdout, stderr=stderr, check=check)
