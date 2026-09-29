# Production-core simulation-profile refactor plan

## Purpose and fixed decisions

This plan extends the `production-core` refactor in four connected areas:

1. detector-condition and pileup profiles, including matched comparisons across
   several fixed pileup means;
2. explicit CMSSW release and GlobalTag provenance from simulation through the
   ntuple handoff;
3. evidence-backed HELAC profiles whose work controls are not confused with
   observed LHE yield;
4. remote run-namespace reservation before a fresh CERN DAG is submitted.

The implementation targets `mcprod` and the native adapters it currently uses.
It does not add equivalent options to the legacy `dag_generator.py` interface.
Run specifications move cleanly to schema version 2; schema version 1 is not
normalized or supported by the new path.

Only the existing CMSSW 12.4.14 2022 PreMix chain will be runnable initially.
Modern 2022--2025 and fixed-mean pileup profiles will be visible in the
catalogue but deliberately unavailable until their reviewed parameterized
CMSSW configs and, for ordinary mixing, compatible min-bias GEN-SIM inventories
exist. An unavailable profile must fail during planning, before DAG files or
remote products are created.

The snippets below define intended interfaces and control flow. They are not
drop-in patches and may need small adaptations to the surrounding code.

## 1. Run-spec schema version 2

### Public shape

Fresh LHE sources select an immutable generator profile and supply only their
identity and seed. Existing LHE sources continue to carry their exact counted
event totals. Each campaign explicitly selects one or more detector variants.

```json
{
  "schema_version": 2,
  "run_id": "jjp_pu_scan_2022_v1",
  "runtime_release": "production-v2",
  "runtime_dir": "../runtimes/production-v2/cern",
  "block_events": 1000,
  "sources": [
    {
      "pool": "pool_2jpsi_g",
      "source_id": "nlo-70102",
      "seed": 70102,
      "generator_profile": "helac_2jpsi_g_nlostar_v1"
    },
    {
      "pool": "pool_gg",
      "source_id": "counted-existing-40001",
      "seed": 40001,
      "url": "root://cceos.ihep.ac.cn:1094///store/.../sample.lhe.gz",
      "actual_events": 2000,
      "size_bytes": 1234567
    }
  ],
  "campaigns": [
    {
      "name": "JJP_SPS_G",
      "analysis": "JJP",
      "target_mixed_events": 500,
      "max_processing_nodes": 10,
      "merge_events": 5000,
      "detector_variants": [
        "run3_2022_legacy_premix_v1"
      ],
      "sources": [
        {
          "pool": "pool_2jpsi_g",
          "mode": "phi_mpi_off",
          "lhe_budget": 1000,
          "efficiency": 1.0
        }
      ]
    }
  ]
}
```

When the fixed-mean profiles are eventually activated, the same campaign can
request a matched fan-out:

```json
"detector_variants": [
  "run3_2022_summer24_nopu_v1",
  "run3_2022_summer24_mu20_v1",
  "run3_2022_summer24_mu40_v1",
  "run3_2022_summer24_mu50_v1",
  "run3_2022_summer24_mu60_v1"
]
```

### Validation and resolution

The settings boundary must reject ambiguous source definitions and resolve
profiles once. Workers must receive frozen profile data in their task JSON and
must never reopen a mutable catalogue.

```python
def resolve_source(raw, generator_profiles):
    has_existing = bool(raw.get("url"))
    has_generation = bool(raw.get("generator_profile"))
    if has_existing == has_generation:
        raise ValueError(
            "source must select exactly one of an existing URL or generator_profile"
        )

    source = dict(raw)
    if has_generation:
        profile = generator_profiles.require_ready(raw["generator_profile"])
        if raw["pool"] not in profile["compatible_pools"]:
            raise ValueError("generator profile is incompatible with source pool")
        source["resolved_generator_profile"] = profile
    return source


def resolve_detector_variants(campaign, detector_profiles):
    names = campaign.get("detector_variants")
    if not isinstance(names, list) or not names or len(names) != len(set(names)):
        raise ValueError("campaign requires distinct detector_variants")
    return [detector_profiles.require_ready(name) for name in names]
```

`require_ready()` should include the profile ID and its `blocked_reasons` in
the error. Unknown and known-but-unavailable are distinct failures.

## 2. Detector and pileup profiles

### Catalogue records

Add versioned generator and detector catalogues under `common/profiles/` and
include them in the runtime allowlist and installed resources. `mcprod catalog`
should report profile availability and blocking reasons.

The runnable legacy profile freezes the current behavior:

```json
{
  "profile_id": "run3_2022_legacy_premix_v1",
  "availability": "ready",
  "data_year": 2022,
  "cmssw_release": "CMSSW_12_4_14",
  "simulation_runtime": "cmssw12-sim-el8",
  "merge_runtime": "cmssw12-sim-el8",
  "ntuple_runtime": "cmssw15-ntuple-el9",
  "mc_global_tag": "124X_mcRun3_2022_realistic_v12",
  "era": "Run3",
  "geometry": "DB:Extended",
  "beamspot": "Realistic25ns13p6TeVEarly2022Collision",
  "pileup": {
    "mode": "premix",
    "scenario": "Run3Summer22DRPremix",
    "secondary_kind": "premix",
    "inventory": "/cvmfs/cms.cern.ch/offcomp-prod/premixPUlist/PREMIX-Run3Summer22DRPremix.txt",
    "dataset": "/Neutrino_E-10_gun/Run3Summer21PrePremix-Summer22_124X_mcRun3_2022_realistic_v11-v2/PREMIX"
  },
  "configs": {
    "gensim": "simulation/run3_2022_legacy/gensim_cfg.py",
    "raw": "simulation/run3_2022_legacy/raw_premix_cfg.py",
    "reco": "simulation/run3_2022_legacy/reco_cfg.py",
    "miniaod": "simulation/run3_2022_legacy/miniaod_cfg.py"
  }
}
```

Modern year entries record the supplied recommendations but remain closed:

| Year | MC GlobalTag | Exact implementation release | Initial status |
| --- | --- | --- | --- |
| 2022 | `150X_mcRun3_2024_realistic_v2` | `CMSSW_15_0_15` | unavailable |
| 2023 | `150X_mcRun3_2024_realistic_v2` | `CMSSW_15_0_15` | unavailable |
| 2024 | `150X_mcRun3_2024_realistic_v4` | `CMSSW_15_0_15` | unavailable |
| 2025 | `150X_mcRun3_2024_realistic_v2` | `CMSSW_15_0_15` | unavailable |

An unavailable fixed-mean profile should be an explicit manifest rather than a
plausible-looking Python config:

```json
{
  "profile_id": "run3_2022_summer24_mu50_v1",
  "availability": "unavailable",
  "blocked_reasons": [
    "reviewed parameterized CMSSW configs are missing",
    "compatible frozen min-bias GEN-SIM inventory is missing"
  ],
  "data_year": 2022,
  "cmssw_release": "CMSSW_15_0_15",
  "mc_global_tag": "150X_mcRun3_2024_realistic_v2",
  "pileup": {
    "mode": "mixing",
    "scenario": "AVE_50_BX_25ns",
    "secondary_kind": "minbias-gensim",
    "inventory": null,
    "inventory_sha256": null
  }
}
```

Create equivalent stubs for `NoPileUp`, `AVE_20_BX_25ns`,
`AVE_40_BX_25ns`, and `AVE_60_BX_25ns`. `NoPileUp` has no secondary input,
but remains unavailable until its complete modern 2022 chain is reviewed.

### Pileup semantics

Configuration validation must encode the distinction documented in
`docs/cmssw_pileup_premix_report.md`:

```python
def validate_pileup(profile):
    pileup = profile["pileup"]
    mode = pileup["mode"]
    secondary = pileup.get("secondary_kind")

    if mode == "none" and secondary is not None:
        raise ValueError("NoPileUp must not declare secondary input")
    if mode == "mixing" and secondary != "minbias-gensim":
        raise ValueError("ordinary mixing requires min-bias GEN-SIM input")
    if mode == "premix" and secondary != "premix":
        raise ValueError("PreMix stage 2 requires PRemix input")
```

The Kevin Pedro `/PREMIX` file list can be retained as PreMix evidence; it must
never satisfy the `minbias-gensim` contract for `AVE_*_BX_25ns`.

### Checked-in parameterized configs

Each runnable profile owns reviewed Python configs. Physics settings such as
GlobalTag, era, HLT menu, pileup mechanism, and beamspot are fixed in those
files. Only operational values are exposed through `VarParsing`, for example:

```python
options.register("outputFile", "output_RAW.root", singleton, string, "output")
options.register("pileupInputList", "", singleton, string, "frozen file list")
options.register("numThreads", 2, singleton, integer, "threads")
options.register("numStreams", 2, singleton, integer, "streams")
options.parseArguments()

process.source.fileNames = options.inputFiles
process.maxEvents.input = options.maxEvents
process.options.numberOfThreads = options.numThreads
process.options.numberOfStreams = options.numStreams
process.RAWSIMoutput.fileName = options.outputFile
```

The chain adapter selects config paths only from the resolved profile and does
not accept arbitrary cmsDriver fragments:

```python
profile = parameters(task)["detector_profile"]
for step in ("gensim", "raw", "reco", "miniaod"):
    config = core_path(task, "common/cmssw_configs/" + profile["configs"][step])
    run_profile(profile["simulation_runtime"], cmsrun_args(step, config), cwd=work_dir)
```

The ntuple configuration must accept an explicit GlobalTag override. Tasks pass
`runOnMC=True`, the profile's MC era, and its exact MC GlobalTag. Refresh its
data mapping to the supplied `150X_dataRun3_v5`, `150X_dataRun3_v2`, and
`150X_dataRun3_Prompt_v1` recommendations, but do not add a data-processing
workflow to `mcprod`.

### Runtime roles

Replace the overloaded stage mapping with explicit roles:

```python
PROFILES = {
    "cmssw12-sim-el8": RuntimeProfile(..., cmssw_base=".../CMSSW_12_4_14"),
    "cmssw15-sim-el9": RuntimeProfile(..., cmssw_base=".../CMSSW_15_0_15"),
    "cmssw15-ntuple-el9": RuntimeProfile(..., cmssw_base=".../CMSSW_15_0_15"),
}
```

The simulation profile chooses the component and merge runtimes. The ntuple
runtime retains its analyzer package contract; CMSSW15 simulation does not
implicitly request that package.

## 3. Matched detector-variant fan-out

`MIX` remains the repository's hard-scatter HepMC combination stage. It is not
detector pileup. Allocate and publish one mixed HepMC artifact, then create one
downstream detector task per selected profile:

```python
mix = make_mix_task(...)
tasks.append(mix)

for profile in campaign["resolved_detector_variants"]:
    variant = profile["profile_id"]
    identity = f"{campaign['name']}_{variant}_{node['job_id']}"
    product = artifact(
        artifact_id="MINIAOD_" + identity,
        url=(f"{ihep_products}/{run_id}/{campaign['name']}/"
             f"{variant}/{node['job_id']}.root"),
        kind="miniaod",
    )
    task = make_detector_task(
        identity=identity,
        input_hepmc=mix["outputs"][0],
        output=product,
        detector_profile=profile,
    )
    tasks.append(task)
    dependencies.append(([mix["task_id"]], task["task_id"]))
```

The archive node depends on all detector tasks for the wave. The shared mixed
HepMC is archived/reclaimed once, after all variants validate. Storage
reservation for mixed HepMC is therefore not multiplied by the variant count.

Merge state must use a compound key rather than the campaign alone:

```python
sample_key = (campaign_name, detector_profile_id)
state["merge_buffers"].setdefault("::".join(sample_key), []).append(output)
```

Keep the two fields separate in JSON records; the joined key is only an
internal dictionary key. Carry both through merge tasks, handoff records,
ntuple tasks, selection/efficiency products, and aggregation. This prevents
files with different pileup or conditions from entering the same merge group.

`max_processing_nodes` continues to count hard-scatter MIX nodes. A five-point
PU scan creates five detector jobs per MIX node without changing campaign
completion accounting.

## 4. HELAC generation profiles

### Profile records

Create one immutable profile for each measured pool so that process definitions
and cuts remain explicit:

```json
{
  "profile_id": "helac_2jpsi_g_nlostar_v1",
  "availability": "ready",
  "compatible_pools": ["pool_2jpsi_g"],
  "generator_path": "main",
  "process": "generate g g > cc~(3S11) cc~(3S11) g",
  "nmc": 500000,
  "unwevt": 2000,
  "unwevt_semantics": "main-path-stop-target",
  "planning_events_hint": 597,
  "cuts": {
    "min_pt_conia": 6.0,
    "min_pt_bonia": 4.0,
    "min_pt_q": 0.0,
    "min_m_qqp": 3.0969,
    "min_m_q_beam": 3.0969
  }
}
```

Add separate profiles for:

- `pool_gg`: `nmc=500000`, `unwevt=2000`, hint 2,000;
- `pool_2jpsi_cs`: the same controls and hint, with its own process/cuts;
- `pool_jpsi_CSCO_g`: `nmc=500000`, `unwevt=2000`, hint 5,316,
  `generator_path=crystalball-addon`, and
  `unwevt_semantics=addon-input-not-output-cap`.

Fresh generation for `pool_upsilon_CSCO_g` and
`pool_jpsi_upsilon_CSCO` remains unavailable until measured profiles exist.
Existing counted LHE from those pools remains usable.

### Worker and planner behavior

Remove the overloaded `generation.max_events` contract. The command builder
passes the resolved controls directly:

```python
command = [
    "bash", run_helac,
    "--pool", source["pool"],
    "--seed", str(source["seed"]),
    "--nmc", str(profile["nmc"]),
    "--unwevt", str(profile["unwevt"]),
    "--compress-lhe",
]
```

After generation, count the LHE and apply only valid invariants:

```python
events = count_lhe_events(output)
if events <= 0:
    raise RuntimeError("HELAC produced no LHE events")
if (profile["unwevt_semantics"] == "main-path-stop-target"
        and events > profile["unwevt"]):
    raise RuntimeError("main-path output exceeds its accepted-event stop target")
```

The existing byte ceiling is the universal output safety bound. In particular,
the CrystalBall worker must accept 5,316 events when `unwevt=2000` because that
addon does not implement the main-path stopping check.

`planning_events_hint` is used only to decide how many lazy source tasks to
prepare. Allocation remains based on the planner's exact counted output. If a
source underproduces, the next wave prepares another declared source; no hint
is promoted to an authoritative count.

The six-seed pilot described below may justify a later versioned profile with a
different conservative hint. It must not silently mutate `v1`.

## 5. Remote run-namespace reservation

### Submission boundary

Keep `mcprod run plan` offline. Immediately before the first Condor submission,
reserve these exact namespaces:

```text
<cern_staging>/<run_id>/.mcprod-run.json
<ihep_products>/<run_id>/.mcprod-run.json
<ihep_archive>/<run_id>/.mcprod-run.json
```

The marker records at least:

```json
{
  "schema": "full-mc-storage-reservation-v1",
  "run_id": "jjp_pu_scan_2022_v1",
  "run_spec_sha256": "...",
  "core_revision": "...",
  "runtime_release": "production-v2"
}
```

The frozen-spec digest is necessary to distinguish an idempotent retry from a
different run that reused the same `run_id`.

### Reservation algorithm

```python
def reserve_run_namespace(client, root_url, marker):
    run_root = root_url.rstrip("/") + "/" + marker["run_id"]
    marker_url = run_root + "/.mcprod-run.json"

    existing = client.read_json_if_exists(marker_url)
    if existing is not None:
        if existing != marker:
            raise RuntimeError("run_id is already reserved by a different run")
        return marker_url  # idempotent submit/recovery

    if client.directory_has_entries(run_root):
        raise RuntimeError("remote run directory is nonempty and has no owner marker")

    client.mkdir(run_root, parents=True)
    client.copy_json_if_absent(marker, marker_url)
    if client.read_json(marker_url) != marker:
        raise RuntimeError("remote namespace reservation raced or was replaced")
    return marker_url
```

Reserve all three roots before writing the Condor submission attempt. A partial
reservation is safe: retrying with the same marker completes the remaining
roots. A mismatch blocks submission and tells the operator to choose a new
`run_id`; there is no force option for a fresh DAG.

Worker stage-out retains `mkdir -p` for declared nested parents. Forced
artifact replacement is confined to the already reserved run namespace and
the task's exact declared output. Keep ownership markers after completion so a
historical run ID cannot later be reused.

Update URL validation to accept the configured CERN
`root://eosuser.cern.ch///eos/user/...` namespace and IHEP
`root://cceos.ihep.ac.cn:1094///store/...` namespace without rewriting their
triple-slash form.

## 6. Tests and acceptance criteria

### Schema and catalogue

- Reject schema v1, inline generation settings, missing detector variants,
  duplicate variant IDs, unknown profiles, unavailable profiles, and
  generator-profile/pool mismatches.
- Confirm `mcprod catalog` displays ready and unavailable profiles plus their
  blocking reasons.
- Confirm resolved profile contents, not only IDs, are frozen in `run.json` and
  worker task JSON.

### DAG and lifecycle

- Generate a synthetic five-variant campaign and assert one MIX parent, five
  detector children, distinct output paths, and an archive dependency on all
  five children.
- Verify every detector child references the identical mixed HepMC artifact.
- Verify merge buffers, handoff tasks, ntuple tasks, and aggregation never mix
  detector variants.
- Verify campaign completion counts MIX nodes rather than downstream fan-out
  jobs and staging bytes count the mixed HepMC only once.

### CMSSW and pileup

- Run Python syntax checks and a CMSSW configuration-load smoke test for every
  ready checked-in config.
- Run the production-style local worker mock for the legacy profile and compare
  its GlobalTag, era, HLT, pileup mode, thread, and stream settings with the
  current chain.
- Assert `NoPileUp` has no secondary input, ordinary mixing accepts only
  `minbias-gensim`, and PreMix accepts only `premix`.
- Assert modern and fixed-mean stubs cannot create tasks.
- Verify detector provenance reaches MiniAOD sidecars, merge/ntuple tasks, and
  final handoff records. Verify ntuple invocation includes the explicit MC era
  and GlobalTag.

### HELAC

- Assert the exact process, cuts, `nmc`, and `unwevt` flags for all four ready
  generator profiles.
- Accept CrystalBall output above `unwevt`; reject main-path output above its
  stop target; reject zero-event output in all cases.
- Verify lazy preparation uses hints but allocation and result metadata use the
  exact counted LHE total.
- Reject fresh generation for the two unmeasured pools.

### Storage

- With a fake XRootD runner, verify creation of missing roots, idempotent reuse
  of matching markers, rejection of mismatched markers, rejection of nonempty
  unowned directories, and recovery after only some roots were reserved.
- Assert `condor_submit_dag` is never called if reservation fails.
- Preserve exact CERN and IHEP URL spellings in generated tasks.

### Required repository checks

Run the checks required by `AGENTS.md`, the complete core unit-test group, the
worker mock, and a relevant generated-DAG inspection. No modern detector or PU
profile is accepted on the basis of unit tests alone; activating one later
requires reviewed configs, a frozen compatible inventory, configuration-load
checks, and a small end-to-end pilot.

## 7. Separate-session NLOstar pilot prompt

Use this prompt in a separate session; it is intentionally independent of the
refactor implementation:

```text
Work in /afs/cern.ch/user/c/chiw/condor/Full_MC_Production, not
.worktrees/production-core. Read AGENTS.md,
docs/unwevt_investigation_2026-09-17.md, the cms-ihep-condor-pilot skill, and
the cms-condor-job-operator skill.

Run a six-seed post-fix HELAC NLOstar pilot for pool_2jpsi_g using seeds
70102-70107. Every job must use:
  nmc=500000
  unwevt=2000
  min_pt_conia=6.0
  min_pt_bonia=4.0
  min_pt_q=0.0
  min_m_qqp=3.0969
  min_m_q_beam=3.0969
  test_mode=false
  helac_diagnostics=slim

Preserve existing work and do not edit production-core or tracked production
code. Record the exact Git revision and refuse to build from tracked dirty
source. Prepare one six-process Condor cluster with unique CERN EOS output
directories. Inspect all six generated configs before submission; they must
differ only in seed and output identity.

Before submission, state that this requests six LHE files with a 2,000-event
per-file stop cap rather than a promised yield; there are no blocks, merge, or
downstream processing jobs, and subprocess coverage is pool_2jpsi_g only. Run
`myschedd bump` and `myschedd show`, record the schedd together with the cluster
ID, submit, and monitor that exact pair until all six processes are terminal.
Follow the Condor skill for holds or retries. Do not remove or resubmit work
without first inspecting recoverable state.

For every seed, validate the LHE count directly and report exit status,
starts/holds, wall and CPU time, output URL and size, and logged HELAC settings.
Summarize the six new yields with min, max, mean, median, sample standard
deviation, and yield per CPU-hour. Then provide a separate combined summary
with existing seeds 70100 and 70101. Do not model the spread as Poisson and do
not alter a production generator profile automatically. Recommend a revised
conservative planning hint only if the completed evidence supports it.
```

## 8. Explicit exclusions and follow-up gates

- Do not submit a modern CMSSW or fixed-mean PU run in this change.
- Do not use the PRemix inventory as min-bias GEN-SIM input.
- Do not create generator defaults for unmeasured pools.
- Do not add free-form cmsDriver arguments or per-source HELAC overrides.
- Do not retrofit schema v2 onto the legacy `dag_generator.py` interface.
- Do not merge CMSSW stages merely because newer production recipes permit it;
  first land and validate the profile boundary with the existing semantic
  stages.

Activation of the modern 2022 fixed-mean profiles requires a separate reviewed
change supplying exact parameterized configs and a compatible frozen min-bias
GEN-SIM inventory with dataset identity and list checksum.
