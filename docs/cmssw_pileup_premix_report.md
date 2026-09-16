# CMSSW pileup and PreMix findings

## Scope and conclusion

This note documents the pileup configuration used by this repository's Run-3
workflow. It distinguishes two unrelated meanings of "mix": combining the
analysis hard-scatter HepMC sources before GEN-SIM, and detector pileup overlay
during digitization. Only the latter is discussed as pileup here.

The current pipeline is a CMSSW_12_4_14 PreMix stage-2 workflow. It uses a
prebuilt PRemix library, not an ordinary `Mixing.py` scenario selected at RAW
time. Consequently, a modern `--pileup` profile or `AVE_40_BX_25ns` cannot
reprofile the current PRemix library.

## Current repository workflow

`processing/run_chain.sh` uses CMSSW_12_4_14 for both GEN-SIM and RAW:

```text
GEN-SIM: HepMC -> pgen, psim
RAW:     DIGI,DATAMIX,L1,DIGI2RAW,HLT:2022v12
         --datamix PreMix
         --procModifiers premix_stage2,siPixelQualityRawToDigi
         --conditions 124X_mcRun3_2022_realistic_v12
         --era Run3
```

`common/cmssw_configs/hepmc_to_GENSIM.py` schedules `pgen` and `psim`, but
does not load `MixingModule`. Vertex smearing in that configuration is not
detector pileup.

The RAW worker obtains its secondary input from:

```text
/cvmfs/cms.cern.ch/offcomp-prod/premixPUlist/
PREMIX-Run3Summer22DRPremix.txt
```

The list has 8,145 files, all from this dataset:

```text
/Neutrino_E-10_gun/
 Run3Summer21PrePremix-Summer22_124X_mcRun3_2022_realistic_v11-v2/
 PREMIX
```

The `dbs` mode in `run_chain.sh` names the same dataset. `eoscms` and
`filelist` modes merely use the CVMFS list to reach it through a redirector.

## What `NoPileUp`, ordinary mixing, and PreMix configure

### NoPileUp

Neither inspected release has `MixingNoPileUp_cff.py`. `NoPileUp` in
`Configuration/StandardSequences/python/Mixing.py` loads
`SimGeneral.MixingModule.mixNoPU_cfi`.

That fragment still creates `process.mix`, with digitizers and mixing objects,
but has no secondary input source. This is required even at zero pileup:
`Digi_cff.py` states that the MixingModule is needed by ECAL, HCAL, and muon
digitization, and its standard digi task has a placeholder for `mix`.

For a genuine no-pileup RAW workflow, use ordinary digitization without
`DATAMIX` and select `--pileup NoPileUp`. Do not feed a PRemix library to that
mode.

### Ordinary `MixingModule` pileup

For example, `AVE_40_BX_25ns` maps to
`SimGeneral.MixingModule.mix_POISSON_average_cfi`. `ConfigBuilder` sets:

```text
process.mix.input.nbPileupEvents.averageNumber = 40
process.mix.bunchspace = 25
process.mix.minBunch = -12
process.mix.maxBunch = 3
process.mix.input.fileNames = <secondary min-bias GEN-SIM files>
```

This is ordinary mixing, not PreMix. It samples a fixed-mean Poisson pileup
model and needs a compatible min-bias GEN-SIM input list. Generic `AVE_*`
names do not encode collision energy; the selected min-bias files do.

The campaign-labelled profiles use `probFunctionVariable` and `probValue`
instead of one fixed mean. They define the in-time distribution, while the
25 ns base fragment handles the out-of-time treatment.

### PreMix stage 2

With `DATAMIX` present, `ConfigBuilder` changes the interpretation of
`--pileup_input`:

```text
ordinary mixing: --pileup_input -> process.mix.input.fileNames
PreMix DATAMIX:  --pileup_input -> process.mixData.input.fileNames
```

The repository's generated PreMix configuration loads `mixNoPU_cfi` for the
base `mix` object and `DataMixerPreMix_cff` for stage 2. The latter loads
`mixOne_premix_on_sim_cfi`, whose `mixData` is a `PreMixingModule` with one
input PRemix event per hard-scatter event (`type = fixed`, `averageNumber =
1`, `minBunch = maxBunch = 0`). It is not sampling a new pileup distribution
at this point.

Therefore, do not combine `--pileup AVE_40_BX_25ns` with
`--datamix PreMix`. In the DATAMIX branch, its ordinary-mixing parameters are
not applied to `mixData`; the PRemix library controls the pileup already
present in its events.

## What the current PRemix files contain

Read-only `edmProvDump` provenance from a file in the repository's configured
list showed a stage-1 `MixingModule` with:

```text
type                 = probFunction
bunchspace           = 25 ns
minBunch, maxBunch   = -12, +3
support              = 0..99 interactions
mean                 = 36.952
mode                 = 41
```

Its 100-bin probability vector exactly matches
`SimGeneral.MixingModule.Run3_2022_LHC_Simulation_10h_2h_cfi`. This is a
machine-fill distribution, not `Run3_Flat55To75_PoissonOOTPU` and not a
fixed 40-interaction configuration.

In plain language, every library event is already a randomly sampled,
digitization-level pileup stack. The stack size differs event by event, but
the ensemble follows the stage-1 profile above. Stage 2 combines one such
stack with one hard-scatter event.

## Release-specific scenario inventory

The release boundary matters. A key in a newer `Mixing.py` cannot be passed to
the current CMSSW_12_4_14 RAW worker unless a compatible local fragment is
provided and transferred deliberately.

### CMSSW_12_4_14

Available relevant choices include:

- `NoPileUp` (`mixNoPU_cfi`)
- generic fixed-mean 25 ns `AVE_*_BX_25ns` scenarios
- `Run3_Flat55To75_PoissonOOTPU`, a flat 55--75 profile
- `2022_LHC_Simulation_10h_2h`
- three `2023_LHC_Simulation_*` machine-fill profiles

It does not contain modern campaign-labelled keys such as
`2022_25ns_RunIII2022Summer24_PoissonOOTPU` or the 2023--2025 Summer24/
Spring25 profiles.

### CMSSW_15_0_15 and current master

In addition to the preceding Run-3 machine-fill profiles, both catalogues
contain:

| Period | Ordinary-mixing distribution scenario |
|---|---|
| 2022 | `2022_25ns_RunIII2022Summer24_PoissonOOTPU` |
| 2023 | `2023_Fills_8807_8901_ProjectedPileup_PoissonOOTPU` |
| 2023 | `2023_25ns_EraC_PoissonOOTPU` |
| 2023 | `2023_25ns_EraD_PoissonOOTPU` |
| 2023 | `2023_25ns_EraCD_PoissonOOTPU` |
| 2023 | `2023_25ns_RunIII2023Summer24_PoissonOOTPU` |
| 2024 | `2024_25ns_RunIII2024Summer24_PoissonOOTPU` |
| 2025 | `2025_25ns_Run3pp13p6TeVSpring25_PoissonOOTPU` |

The 5.36 TeV 2024 reference profile is intentionally excluded. No dedicated
2026 pp 13.6 TeV key was found in 12_4_14, 15_0_15, or CMSSW master checked on
2026-09-06.

`2022postEE` and `2023postBPix` are conditions subdivisions, not pileup keys.
They are represented by Global Tag aliases such as
`phase1_2022_realistic_postEE` and
`phase1_2023_realistic_postBPix`. The pileup cfi does not choose geometry,
detector era, Global Tag, or HLT menu.

## Why use PreMix?

PreMix is primarily an amortization and I/O strategy. Ordinary mixing makes a
hard-scatter job select and overlay many min-bias events for every signal
event, potentially across many bunch crossings. At production scale this
means large secondary-file lists, high remote I/O, repeated event decoding,
and repeated pileup digitization work in every hard-scatter campaign.

PreMix moves that many-minbias operation to a stage-1 library production:

```text
large min-bias GEN-SIM sample + selected PU profile
        -> stage 1: reusable PRemix "pure pileup" library
hard-scatter GEN-SIM + one PRemix library event
        -> stage 2: DataMixer and final digitization/RAW
```

The stage-1 cost and storage are paid once, then reused across many
hard-scatter samples that need the same conditions and pileup profile. Stage
2 is not free: it still digitizes the hard scatter and combines detector
signals at the appropriate level. It is cheaper operationally because it
consumes one prepared pileup event rather than repeatedly building a full
stack from many min-bias inputs.

This is not merely a file-count shortcut. Correct PreMix preserves required
detector-level behavior by splitting operations between stages: for many
subdetectors, noise and zero suppression are deferred so they are applied to
the combined pileup-plus-hard-scatter signal, not twice. CMS documents these
subdetector-specific rules. Consequently, a home-grown library must be made
with the matching PreMix stage-1 configuration, not by simply concatenating
or ROOT-merging files.

## Can a local PRemix subset be used?

Yes, with two distinct meanings.

### Local copy of an existing official library

This preserves the library's already-embedded profile. It can reduce remote
XRootD dependence and repeated transfer failures, but it does not create a
new pileup distribution or change its mean.

The repository already supports this operationally:

- `PREMIX_INPUT_MODE=localcache` copies the first
  `PREMIX_CACHE_FILES` entries into each job's work directory.
- `PREMIX_INPUT_MODE=localpool` reads ROOT files from
  `PREMIX_LOCAL_POOL_DIR`; an optional `PREMIX_LOCAL_POOL_FILES` limits the
  sorted file list.

The default local cache is only one deterministically selected file. Treat it
as an I/O troubleshooting cache or a small pilot aid, not a production-scale
statistical pool. A production local subset should be selected reproducibly
but broadly from the official library, contain enough distinct events for the
planned reuse, be integrity-checked, and have its dataset, file inventory,
embedded profile, and conditions recorded. A tiny shared subset risks
excessive reuse and correlations between output events or jobs.

Job-local caching also does not necessarily save total site resources: every
job may download the same files. A persistent submit-visible or site-managed
cache/pool can save repeated network transfers, at the cost of managed disk
space and lifecycle control.

### Creating a new local PRemix-equivalent library

This is possible, but it is a new stage-1 simulation production. It needs:

1. an approved compatible min-bias GEN-SIM sample;
2. the selected fixed-mean or probability distribution and bunch-crossing
   window;
3. matching CMSSW release, detector era, geometry, Global Tag, and digitizer
   settings;
4. PreMix stage-1 processing and event content;
5. validation that noise/pedestals/zero suppression are not double-counted;
6. provenance and capacity planning sufficient to prevent excessive reuse.

It is appropriate only when a required library/profile does not exist or a
campaign owner approves a dedicated production. It is not required to use the
current 2022 PRemix library.

## Recommended configuration model

Keep the current CMSSW_12_4_14 chain unless a full GEN-SIM through MiniAOD
release migration is approved. Expose an explicit RAW mode in job/DAG
configuration:

| Mode | RAW configuration | Secondary input |
|---|---|---|
| `none` | ordinary `DIGI,L1,DIGI2RAW,HLT`; `--pileup NoPileUp` | none |
| `mixing` | ordinary DIGI; generic or approved profile | min-bias GEN-SIM list |
| `premix` | current `DIGI,DATAMIX,... --datamix PreMix` | PRemix library list |

Do not move RAW alone to CMSSW_15 merely to obtain a modern profile. That
would create a mixed-release GEN-SIM/RAW/RECO/MiniAOD chain and requires
end-to-end campaign validation. A custom modern profile backport is
technically plausible only for ordinary mixing; it still requires compatible
min-bias input and CMS campaign approval.

## Sources and evidence

- Repository: `processing/run_chain.sh`,
  `common/cmssw_configs/hepmc_to_GENSIM.py`,
  `processing/condor_wrappers/run_processing.sh`, `dag_generator.py`, and
  `common/node_config_defaults.json`.
- CMSSW_12_4_14 and CMSSW_15_0_15: `Mixing.py`, `ConfigBuilder.py`,
  `Digi_cff.py`, `DataMixerPreMix_cff.py`, `DigiDM_cff.py`,
  `DigiToRawDM_cff.py`, `mixNoPU_cfi.py`,
  `mix_probFunction_25ns_PoissonOOTPU_cfi.py`, and
  `mixOne_premix_on_sim_cfi.py`.
- One file from the configured library was inspected with `edmProvDump`; its
  stage-1 probability vector was compared directly with the installed
  `Run3_2022_LHC_Simulation_10h_2h_cfi` fragment.
- CMS Full Simulation guide, "Pre-Mixing Instructions" and "Pre-Mixing
  Documentation": <https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideSimulation#Special_Procedures>.
- CMSSW master `Mixing.py`:
  <https://github.com/cms-sw/cmssw/blob/master/Configuration/StandardSequences/python/Mixing.py>.

The Twiki commands in the historical instructions are useful conceptually but
are not a Run-3 production recipe. Use the selected campaign's release,
conditions, and validated modern configuration rather than copying those old
commands verbatim.
