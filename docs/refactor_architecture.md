# Production-core architecture

The refactor separates physics execution, production planning, scheduler access,
and operator-facing commands. It uses a model/controller/CLI split rather than a
web-style MVC framework. The new worker, allocation, handoff, batch, aggregation,
CERN controller, and DAG rendering contracts are implemented and covered by
offline tests. Live scheduler, storage, and full physics runtime validation still
remain before production cutover.

## Code and production records

```text
production-wrapper/                 separate repository
  core/                             pinned production-core submodule
  campaigns/                        operator-owned run specifications
  history/                          production decisions and completed-run records
  work/                             ignored, submit-visible generated jobs
  runtimes/                         ignored, reusable runtime releases

production-core/
  mcprod                            CLI entry point
  src/full_mc/
    domain/                         counts, source identities, deterministic allocation
    planning/                       capacity and DAG construction
    application/                    run lifecycle, handoff, recovery, aggregation
    backends/                       scheduler-specific submission and status
    runtime/                        explicit profiles, packaging, worker bootstrap
    storage/                        exact URLs, transfers, archive operations
    workers/                        LHE, PLAN, MIX, MiniAOD, merge, ntuple, analysis
  common/campaigns/                 physics campaign catalogue
  common/cmssw_configs/             campaign-layer CMSSW configurations
  docs/                             durable operator and developer instructions
```

The wrapper is the intended home for incremental production history, and its
initializer is implemented. Migration of existing history and creation of the
operator's production wrapper remain separate activities. Counted inventories
and selected source IDs remain authoritative. Inventory/job-spec checksums belong at the
planning boundary, not in repeated worker package validation.

## Target DAG

One CERN DAGMan root owns serialized, bounded waves. A wave contains only the
sources needed for that allocation, not a barrier over every source in the run.

```text
source generation/counting -> deterministic block planning -> frozen allocation
                                                               |
                                                    processing SubDAG
                                                    MIX -> MiniAOD
                                                               |
                                             validated component products
                                                               |
                                            archive LHE + mixed HepMC at IHEP
                                                               |
                                             reclaim eligible CERN staging
                                                               |
                                                     next bounded wave

validated MiniAOD groups -> explicit operator handoff -> IHEP combined worker
                                                        merge (EL8)
                                                             |
                                                       raw ntuple (EL9)
                                                         /        \
                                                   selection    efficiency
                                                         \        /
                                                  explicit aggregation batch
```

Selection and efficiency execute sequentially in one allocation, but both read
the full raw ntuple. Their checkpoints are independent. An efficiency retry must
not rerun successful selection or use its filtered output as the denominator.

Frozen allocation replay does not allocate a new wave. Committed pool cursors
advance after archive/reclamation is recorded. Exhausted inventory is distinct
from satisfying the finite campaign node limit. Repeated pool slots consume
distinct blocks in selected-inventory order, including usable final tails.

## Runtime and transfer boundaries

| Stage | Runtime profile | Persistent product |
| --- | --- | --- |
| LHE generation | `helac-el7` | compressed LHE |
| Block planning | `utilities-el9` | compressed blocks; one task result |
| Shower and mixing | `pythia-el8` | mixed HepMC at CERN EOS |
| MiniAOD | `cmssw12-el8` | component MiniAOD at IHEP |
| Merge | `cmssw12-el8` | merged MiniAOD checkpoint |
| Ntuple | `cmssw15-el9` | full raw ntuple checkpoint |
| Selection/efficiency | `analyzer-el9` | selected ROOT / parquet shard archive |

Profiles are declared by stage; host OS detection must not choose physics
software. Native scripts currently remain behind narrow stage adapters while
their responsibilities are migrated. The two-CPU MiniAOD stage retains the
native two-thread CMSSW settings.

`mcprod runtime prepare` creates an allowlisted code bundle and separate heavy
assets. It does not walk the generated directory, inventories, campaign job
specs, or Git data. Heavy packages are prepared once per runtime release, not
copied into every node directory. The transfer bootstrap unpacks the selected
profile's assets and executes the bundled CLI in a new Python process.

Each worker gets one task JSON. Source slots contain only that task's block
references and their exact URLs. For example, a MIX task's slot configuration is:

```json
{
  "slot": 0,
  "inputs": ["BLOCK:pool_jpsi_CSCO_g:source9:000003"],
  "mode": "normal",
  "rng_seed": 12345,
  "target_hepmc_events": 100,
  "max_lhe_events": 1000
}
```

The allocator freezes the seed; workers do not derive it from scratch paths.
Source-slot HepMC is scratch-only. MIX publishes the actual accepted count, not
the requested target. MiniAOD and merge use actual `edmFileUtil` counts. Native
HELAC diagnostic stageouts are disabled in the new adapter; only declared
products are published by the core transfer layer.

## Storage and recovery

The default CERN staging budget is 100 decimal GB with a 10 GB reserve. A wave
reservation must include its original LHE, split blocks, and enforced mixed-HepMC
output ceilings. File sizes may inform byte reservations, never event counts.
Already resident sources are not reserved again. Replayed reclamation cannot
subtract its reservation twice.

Exact storage roots remain in `common/node_config_defaults.json`. CERN EOS is
the short-term storage through mixed HepMC; large MiniAOD products and upstream
archives go to IHEP. No worker searches alternate storage layouts.

Compressed upstream members are stored in uncompressed tar archives, without
recompressing their gzip payloads. The archive worker verifies component MiniAOD
readiness and creates the archive; it does not itself delete staging. The
controller records archive verification, reclamation, and continuation as
separate lifecycle transitions. Live storage validation is still required before
production use.

Handoff export reads the coordinator's explicit `merge_groups.json` index. It
checks component MiniAODs, not the future merge output, and carries actual event
counts into the IHEP task. Batch preparation uses job-private proxy and scratch
paths and distinct checkpoint directories. Submission attempts are persisted
before contacting HepJob; ambiguous or interrupted attempts require exact
reconciliation and are not automatically duplicated.

## Remaining integration

The refactored CLI is the supported interface for new work. Prepared pilots,
live CERN/IHEP scheduler reconciliation, full analyzer execution, and the
CMSSW15 fallback still require environment validation before production cutover.
The old graph scaffold remains a retained compatibility path. The new runtime
contracts should be checked in an unsubmitted generated DAG before any
production cutover.

Offline checks for the current contracts:

```bash
PYTHONPATH=src python3 -m unittest \
  tests.test_core_workers tests.test_core_ihep tests.test_core_analysis \
  tests.test_core_planning tests.test_core_planner_local tests.test_core_handoff \
  tests.test_core_workspace
```
