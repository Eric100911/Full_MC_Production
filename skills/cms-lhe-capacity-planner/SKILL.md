---
name: cms-lhe-capacity-planner
description: Plan CMS LHE and shower-source capacity from authoritative event inventories and pilot efficiency measurements. Use when choosing normal-to-phi source ratios, source files, per-source LHE budgets, block sizes, processing-node counts, or storage limits; explaining unavoidable unused LHE or HepMC events; reviewing report-and-truncate behavior; or estimating whether a Full_MC_Production campaign fits the IHEP disk budget.
---

# CMS LHE Capacity Planner

Use counted inventory events as authoritative input. Combine them with measured
pilot shower efficiencies and the campaign's source multiplicities. Optimize
source utilization without turning a target unused fraction into a hard
physics restriction.

Read [`references/capacity-model.md`](references/capacity-model.md) for formulas,
definitions, and the required report.

## Establish evidence

Collect:

1. Exact inventory JSON and its checksum.
2. Selected source files and counted events per pool.
3. Campaign source slots, including repeated use of the same pool.
4. Block size and per-source LHE budgets.
5. Pilot `attempted_lhe_events`, `accepted_hepmc_events`, retries, and wall time
   per shower mode.
6. Configured storage warning and hard limits, retained bytes, and estimated
   bytes per final event.

Do not rescan LHE sizes merely to recover information already present in the
inventory. Do not infer events from file size. Use remote scans only when the
authoritative inventory is missing or explicitly being refreshed.

For a completed pilot, use:

```bash
python3 tools/review_phase2_shower_efficiency.py \
  --pilot-dir generated/<pilot> \
  --cache-dir /tmp/chiw/phase2_shower_review_manifests \
  --json-output /tmp/chiw/phase2_shower_review.json \
  --csv-output /tmp/chiw/phase2_shower_review.csv
```

Add `--fetch-remote` only from an approved normal CERN/IHEP shell with a valid
proxy.

## Model each source separately

For every source slot, report:

- pool and shower mode;
- selected and planned LHE events;
- attempted LHE events;
- accepted HepMC events;
- HepMC events consumed by mixing;
- LHE acceptance efficiency;
- unused LHE and unused accepted-HepMC fractions.

Aggregate both by source slot and by pool. This is essential when a campaign
uses the same pool more than once.

Choose normal and phi budgets so their expected accepted HepMC yields are
similar at the mixer boundary. Repeated source slots multiply demand. Preserve
non-overlapping deterministic blocks within a subprocess.

## Apply policy

- Treat roughly 15% unused events from any source, including phi shower HepMC,
  as an operational planning guideline.
- Use the configured unused-HepMC warning threshold when evaluating a generated
  campaign. In this repository the current default is 0.15; read the generated
  metadata instead of assuming it.
- Do not encode a target unused fraction as a hard stop.
- Use `report-and-truncate` when a usable common mixed-event count can be
  retained safely; emit the source-level shortfall and truncation report.
- Halt on corrupt input, invalid accounting, overlap, provenance failure, or
  inability to form a valid output—not merely because one source has leftovers.

Explain why nonzero waste remains after ratio optimization: stochastic shower
acceptance, integer block boundaries, whole-file selection, repeated pool
slots, common-minimum mixing, retry limits, and pilot-to-production efficiency
variation.

## Check storage

Estimate retained capacity primarily from expected final events and measured or
configured bytes per final event. Include existing retained bytes. MiniAOD is
normally the dominant product, but use measured product breakdowns when
available.

Use the campaign job spec's `warning_bytes` and `hard_limit_bytes` as
authoritative enforcement. Keep the operational design below the approximately
5 TB IHEP T2 budget unless the configured limits explicitly say otherwise.
Never hide a hard-limit exceedance by excluding retained products from the
estimate.

## Deliver a recommendation

Provide:

- inventory checksum and selected-file count;
- event and block totals per pool;
- source multiplicities and proposed LHE budgets;
- expected accepted yield and final mixed events;
- unused fractions per source and why they remain;
- selected processing-node count and unused capacity;
- retained storage estimate with warning/hard-limit status;
- uncertainties derived from pilot sample size and efficiency spread;
- exact campaign parameters to change, without introducing a hard target
  unused-rate parameter.
