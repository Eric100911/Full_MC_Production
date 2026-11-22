# mcprod capacity baseline — 2026-10-13

This record freezes the first measured CERN staging and archive capacity
baseline for the refactored `mcprod` controller. The machine-readable copy is
`production_capacity` in both
`common/node_config_defaults.json` and
`src/full_mc/resources/node_config_defaults.json`.

## Measurement scope

The measurement uses the samples already produced by run
`jjp6x1m_600wave_lhegen_20261012T115241Z`:

| Field | Value |
|---|---|
| Campaign | `JJP_SPS_CS` |
| Source pool | `pool_2jpsi_cs` |
| Block size | 1,000 events |
| Mixed target | 1,000 events per processing node |
| LHE originals | 400 |
| LHE blocks | 800 |
| Mixed HepMC results | 200 |
| MiniAOD results | 127 |

This is a measured baseline for the indicated pool and campaign. It is not a
guarantee for `pool_2jpsi_g`, DPS, TPS, or any future generator configuration.
A new pool must be checked against its own result records before its measured
sizes are used as a planning baseline.

## Observed sizes

| Artifact | Mean | Maximum | Total observed |
|---|---:|---:|---:|
| LHE original | 0.198 MiB | 0.198 MiB | 0.077 GiB |
| 1,000-event LHE block | 0.099 MiB | 0.100 MiB | 0.078 GiB |
| 1,000-event mixed HepMC | 15.835 MiB | 16.189 MiB | 3.093 GiB |
| 1,000-event MiniAOD | 54.450 MiB | 55.252 MiB | 6.753 GiB |

MiniAOD is written to IHEP products and is not part of the CERN staging
resident set. The committed resident staging set contained 200 LHE files and
400 LHE blocks and occupied `83,187,828` bytes. Including the 200 uncommitted
mixed HepMC results from the active processing wave, the measured active-wave
staging footprint was `3,404,084,862` bytes.

## Capacity calculation

The default CERN staging budget remains 100 decimal GB with a 10 decimal GB
reserve, giving 90,000,000,000 usable bytes. The measured maximum per-node
staging increment is:

```text
417,071 bytes of source LHE and blocks
+ 16,975,384 bytes of mixed HepMC
= 17,392,455 bytes per processing node
```

Using the current resident set as the starting point gives approximately
5,169 additional nodes before the staging budget is reached. The default
planning value is therefore raised to 1,000 processing nodes per wave, with
substantial room under the measured staging limit.

The archive worker has a 200,000,000,000-byte disk request. The two observed
200-node archive tasks used 10,098,932,769 bytes at most, or about 50,494,664
bytes per processing node when calculated from the maximum observed inputs.
Using the current resident originals and that per-node maximum gives about
3,877 additional nodes before the archive disk request is reached. Archive
capacity therefore does not constrain a 1,000-node wave for this measured
baseline.

The current implementation still reserves the configured
`limits.mixed_hepmc_bytes` ceiling while planning. The new default ceiling is
64,000,000 bytes per mixed HepMC result. The source ceilings are 100,000,000
bytes each for the generated LHE and its block output. These are planning
ceilings with headroom over the measured `pool_2jpsi_cs` files; they are not
event-count estimates.

## Defaults for new specifications

`settings.load_run_spec()` reads `production_capacity.planning_defaults` when
a new specification omits the corresponding values. An explicit value in a
specification continues to take precedence. `source_files_per_step` is the
source-selection request; the planner may still select fewer files when the
declared per-file byte ceilings leave less room than requested. The defaults
are:

```json
{
  "source_lhe_bytes": 100000000,
  "source_block_bytes": 100000000,
  "mixed_hepmc_bytes": 64000000,
  "source_files_per_step": 1000,
  "processing_nodes_per_wave": 1000,
  "archive_disk_bytes": 200000000000
}
```

The archive disk default is applied when the spec uses prefetch mode and does
not declare its own archive disk request. These defaults affect new planning;
they do not rewrite an existing `run.json`, generated DAG, or submitted run.
The active `jjp6x1m` run remains frozen at its recorded values.
