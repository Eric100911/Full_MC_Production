# Capacity model

## Definitions

For source slot \(s\):

- \(A_s\): selected, plannable LHE events;
- \(B_s\): configured LHE budget per processing node;
- \(e_s\): accepted HepMC events divided by attempted LHE events;
- \(H_s = B_s e_s\): expected accepted HepMC yield per processing node;
- \(M\): expected mixed output per processing node.

For block size \(K\):

```text
blocks_per_slot_s = ceil(B_s / K)
required_blocks_pool_p = sum(blocks_per_slot_s for slots using pool p)
available_nodes_pool_p = floor(available_blocks_p / required_blocks_pool_p)
processing_nodes = min(available_nodes_pool_p across required pools)
```

The common mixer yield is approximately:

```text
M = min(H_s across source slots)
```

Choose budgets so \(B_s e_s\) are similar. For two source modes, a useful
starting ratio is:

```text
B_normal / B_phi = e_phi / e_normal
```

Apply source multiplicity when translating slot budgets to pool demand. If the
same normal pool occupies two slots, its pool demand is twice the per-slot
budget even though the accepted-yield balance is evaluated per slot.

## Utilization measures

Keep these measures distinct:

```text
LHE acceptance efficiency =
    accepted HepMC / attempted LHE

selected-LHE unused fraction =
    (selected plannable LHE - attempted LHE) / selected plannable LHE

accepted-HepMC unused fraction =
    (accepted HepMC - consumed by mixer) / accepted HepMC

final completion fraction =
    produced mixed events / target mixed events
```

Report fractions per source slot before aggregating. Never combine normal and
phi leftovers into a single number that can hide an imbalanced source.

## Why optimized plans still waste events

Even an efficiency-balanced ratio leaves residual waste:

1. Accepted showers are stochastic; pilot efficiencies are estimates.
2. Blocks and source files are discrete, so the last block or file may be only
   partly useful.
3. Mixing consumes the common available count across all source slots.
4. Retry limits change the realized acceptance tail.
5. Repeated inputs from one pool require separate non-overlapping blocks.
6. A shared pool may have capacity that cannot form another complete
   multi-source processing node.
7. `report-and-truncate` deliberately preserves a valid common prefix and
   reports the remainder.

Treat approximately 15% unused per source as a configuration guideline, not a
guarantee. Evaluate warnings using the threshold stored in the generated
metadata.

## Inventory and repository signals

Use the exact inventory passed through `--existing-lhe-inventory`. Verify its
checksum against the campaign job spec. Relevant generated fields include:

```text
campaign_planning_spec.selected_inventory
campaign_planning_spec.campaigns.*.source_lhe_budgets
campaign_planning_spec.campaigns.*.source_efficiencies
production_capacity_signals.blocks_by_pool
production_capacity_signals.campaigns.*.available_processing_nodes
production_capacity_signals.campaigns.*.selected_processing_nodes
production_capacity_signals.campaigns.*.expected_final_events
production_capacity_signals.storage
```

In block mode, `--jobs` counts source LHE files, not processing blocks.

## Storage estimate

Use:

```text
predicted_new_bytes =
    sum(expected_final_events_campaign * bytes_per_final_event)

predicted_total_bytes =
    existing_retained_bytes + predicted_new_bytes
```

Prefer measured per-event product sizes when a representative pilot exists.
State whether the estimate includes HepMC, intermediate component MiniAOD,
merged MiniAOD, Ntuple, logs, and manifests. Do not perform a full remote LHE
size scan unless storage of those retained LHE files is actually in scope.

Classify the result using configured `warning_bytes` and `hard_limit_bytes`.
Also report headroom relative to the operational approximately 5 TB IHEP
budget.

## Required recommendation table

Produce one row per source slot with:

```text
campaign
slot
pool
mode
selected files
available/planned LHE
budget per node
pilot efficiency
expected accepted HepMC
expected mixer consumption
unused LHE fraction
unused accepted-HepMC fraction
```

Then report per-pool blocks, maximum complete nodes, selected nodes, expected
final events, predicted retained bytes, and uncertainty.
