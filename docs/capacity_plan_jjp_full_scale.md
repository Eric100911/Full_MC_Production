# JJP Full-Scale Capacity Plan (6 campaigns)

Generated 2026-08-13. Authoritative inputs: counted inventory
`generated/lhe_inventory_jjp_20260717_184812.json` (sha256 `9c393a706aa3ad4368a3ca3c10a822cdd65f7c4daee86950df3e4e9044369e68`),
pilot processing manifests on EOS, and the frozen full-spec
`common/campaign_job_specs/jjp_efficiency_balanced_full_v2.json`.

## Scope

Six campaigns remain unfinished. Two regimes:

- **G campaigns** (`SPS_G`, `DPS2_G`) — exhaust `pool_2jpsi_g` (387,456 events).
  Overlap allowed: the pool is consumed twice over. 200-event blocks.
- **CS campaigns** (`SPS_CS`, `DPS2_CS`) — full exhaust of `pool_2jpsi_cs`
  (24,701,792 events). Geometry unchanged from full-scale (1000/5000).
- **DPS1 job_0 / job_1** — already frozen by the full spec; never ran.
  Resubmit via the split workflow.

Storage ceiling: **~9 TB** operational (exceeds the full-spec 5 TB hard
limit; accepted by operator).

## Authoritative inventory

| Pool | Files | Counted events |
|---|---|---|
| `pool_2jpsi_cs` | 1000 | 24,701,792 |
| `pool_2jpsi_g` | 1900 | 387,456 |
| `pool_jpsi_CSCO_g` | 2000 | 141,412,548 |
| `pool_gg` | 2000 | 51,721,627 |

## Measured shower efficiencies (accepted HepMC / attempted LHE)

| Pool | Mode | Efficiency | Source |
|---|---|---|---|
| `pool_2jpsi_cs` | `normal` | 1.000 | DPS2_CS pilot (100/100) |
| `pool_2jpsi_cs` | `phi_mpi_off` | 0.763 | SPS_CS pilot (100/131) |
| `pool_2jpsi_g` | `normal` | 1.000 | DPS2_G pilot (100/100) |
| `pool_2jpsi_g` | `phi_mpi_off` | 0.781 | SPS_G pilot (100/128) |
| `pool_gg` | `phi_mpi_off` | 0.909 | DPS2_CS pilot (100/110) |
| `pool_jpsi_CSCO_g` | `normal` | 0.933 | full_spec (DPS1/TPS) |
| `pool_jpsi_CSCO_g` | `phi_mpi_off` | 0.802 | full_spec (DPS1/TPS) |

## Per-campaign model (corrected after generation)

The coordinator consumes **whole blocks**: a node accumulates LHE blocks until
`accumulated >= B_s`, so it consumes `ceil(B_s / block_events)` blocks per slot
per node. Nodes are bounded by `blocks_by_pool // required_blocks`, not
`pool_events / B_s`. `--phi-consumption-mode exhaustive` is required so the
single-phi-source campaigns (SPS_CS/SPS_G) use their full accepted yield
(`block × efficiency`) instead of discarding the block-boundary overshoot.

Final events ≈ `pool_events × efficiency` (exhaustive), independent of budget;
the metadata `expected_final_events` is a lower bound for tail-heavy pools.

### SPS_G — exhaust `pool_2jpsi_g` (phi_mpi_off, e=0.781)

| | |
|---|---|
| Block (mixed) | 200 |
| phi budget | 260 LHE |
| Blocks | 387,456 / 260 ≈ **1490** |
| Final events | **~298k** |
| Merge (5 blk → 1000 ev) | ~298 |

### DPS2_G — exhaust `pool_2jpsi_g` (normal e=1.0) + `pool_gg` (phi e=0.909)

| | |
|---|---|
| Block (mixed) | 200 |
| normal budget | 220 LHE (reconfigured from 110) |
| phi budget | 220 LHE |
| Blocks | min(387,456/220, 51.7M/220) = **1761** |
| Final events | **~352k** |
| Merge (5 blk → 1000 ev) | ~352 |

### SPS_CS — exhaust `pool_2jpsi_cs` (phi_mpi_off, e=0.763)

| | |
|---|---|
| Block (mixed) | 1000 |
| phi budget | 1350 LHE |
| Blocks | 24,701,792 / 1350 ≈ **18,297** |
| Final events | **~18.3M** |
| Merge (5 blk → 5000 ev) | ~3,659 |

### DPS2_CS — exhaust `pool_2jpsi_cs` (normal e=1.0) + `pool_gg` (phi e=0.909)

| | |
|---|---|
| Block (mixed) | 1000 |
| normal budget | 1000 LHE |
| phi budget | 1100 LHE |
| Blocks | min(24,701,792/1000, 51.7M/1100) = **24,701** |
| Final events | **~24.7M** |
| Merge (5 blk → 5000 ev) | ~4,940 |

### DPS1 job_0 + job_1 — frozen full-spec, resubmit only

| | |
|---|---|
| Budgets | [860, 1000] (exhaustive phi) |
| Block (event_id_span) | 1000 |
| Blocks per job | 2000 (job_0), 2000 (job_1) |
| Groups per job | 400 (job_0), 400 (job_1) |
| Final events | ~3.2M total (both jobs) |

## Source-slot recommendation table

| Campaign | Slot | Pool | Mode | Sel. files | Planned LHE | Budget | Eff. | Exp. accepted | Exp. consumed | Unused LHE |
|---|---|---|---|---|---|---|---|---|---|---|
| SPS_G | 0 | pool_2jpsi_g | phi_mpi_off | 1900 | 387,456 | 260 | 0.781 | 203/block | 200 | ~0% (exhaust) |
| DPS2_G | 0 | pool_2jpsi_g | normal | 1900 | 387,456 | 220 | 1.000 | 220/block | 200 | ~9% (margin) |
| DPS2_G | 1 | pool_gg | phi_mpi_off | 2000 | 51.7M | 220 | 0.909 | 200/block | 200 | large (pool not binding) |
| SPS_CS | 0 | pool_2jpsi_cs | phi_mpi_off | 1000 | 24.7M | 1350 | 0.763 | 1030/block | 1000 | ~0% (exhaust) |
| DPS2_CS | 0 | pool_2jpsi_cs | normal | 1000 | 24.7M | 1000 | 1.000 | 1000/block | 1000 | ~0% (exhaust) |
| DPS2_CS | 1 | pool_gg | phi_mpi_off | 2000 | 51.7M | 1100 | 0.909 | 1000/block | 1000 | large (pool not binding) |
| DPS1 j0/j1 | 0 | pool_jpsi_CSCO_g | normal | — | 141M | 860 | 0.933 | ~802/block | ~802 | frozen |
| DPS1 j0/j1 | 1 | pool_jpsi_CSCO_g | phi_mpi_off | — | 141M | 1000 | 0.802 | ~802/block | ~802 | frozen |

## Aggregate scale (as generated — exhaustive mode)

| Campaign | Processing nodes | Final events (metadata) | Final events (exhaust ≈ pool×eff) |
|---|---|---|---|
| SPS_G | 1,461 | ~228k | ~302k |
| DPS2_G | 1,461 | ~292k | ~387k |
| SPS_CS | 24,200 | ~18.46M | ~18.85M |
| DPS2_CS | 24,200 | ~24.2M | ~24.7M |
| DPS1 job_0+1 | 4,000 | — | ~3.2M |
| **Total** | **~55,300** | — | **~47.5M** |

## Storage estimate (generated)

`bytes_per_final_event = 90,000`. Component MiniAOD transient (`cleanup=True`).

| Production | Predicted new bytes |
|---|---|
| CS (SPS_CS + DPS2_CS) | ~3.84 TB |
| G (SPS_G + DPS2_G) | ~0.04 TB |
| DPS1 job_0+1 | ~0.29 TB |
| **New total** | **~4.2 TB** |

vs ~9 TB operational ceiling → **~4.8 TB headroom** (before counting the ~1.3 TB
already retained under `v4_2_full`). The full-spec 4 TB warning / 5 TB hard
limit must not gate this production.

## Generation summary

Two versioned specs + DAGs generated (exhaustive mode):

| Spec | Output dir | Campaigns | block_events | Budgets |
|---|---|---|---|---|
| `jjp_g_full_v2.json` | `..._v4_3_full_G/` | SPS_G, DPS2_G | 200 | SPS_G `[200]`, DPS2_G `[200,400]` |
| `jjp_cs_full_v2.json` | `..._v4_3_full_CS/` | SPS_CS, DPS2_CS | 1000 | SPS_CS `[1000]`, DPS2_CS `[1000,1100]` |

Output prefix: `/store/user/chiw/JpsiJpsiPhi_MC_Production_v4_3_full` (fresh,
avoids pilot collision). Mode `--phi-consumption-mode exhaustive`. G merge 1000,
CS merge 5000.

## Uncertainty

G efficiencies come from a single pilot block each (100 events); the phi
acceptance on `pool_2jpsi_g`/`pool_2jpsi_cs` has ~±3% block-to-block spread
(76–78% observed). This translates to ~±3% on G final events and ~±3% on
SPS_CS final events. CS `normal` and `pool_gg` efficiencies are single-point
too; `pool_gg` phi varies 91–98% across the two pilots, so DPS2_CS/DPS2_G phi
budgets carry the widest uncertainty — the pool is not binding in either case,
so this does not change the scale, only the unused-LHE fraction.
