# LHE output location and CrystalBall `nmc`: reviewed fix plan

Date: 2026-09-18
Updated: 2026-09-19
Status: **issues confirmed; fixes refined but not implemented.** The main rerun
remains paused. `dag_generator.py` and `lhe_generation/run_helac.sh` are still
unchanged.

This note reviews the two actionable items carried over from the v4_5_full
SPS_CS post-mortem (cluster 12185477 @ bigbird25, 5240/5241 nodes, about 678k
mixed events against a roughly 2M target). It supersedes the provisional
CrystalBall sizing recommendation in
`docs/unwevt_investigation_2026-09-17.md`.

## Review summary

1. The generated-LHE path bug is real. Generation and its downstream planner
   both use the read-only existing-inventory resolver. They must use one shared
   generated-output resolver derived from `storage.generated_lhe_base`.
2. That two-call change prevents writes into `MC_Production_v3/LHE_pool`, but
   it does **not** by itself enforce the stronger policy that no new product is
   written anywhere below `MC_Production_v3`: without `--target-base-url`, the
   current write default is still v3.
3. `source_inventory_sha256` authenticates the inventory JSON file, not the
   remote LHE contents. It cannot detect an in-place overwrite at the same URL.
4. The 5,316-event CrystalBall result at `nmc=500,000` is a real counted result
   for the current configuration. It must not be dismissed as a low-yield
   artifact merely because historical `nmc=5,000,000` files are about ten
   times larger.
5. The two 5,316-event outputs are identical because the CrystalBall addon is
   not seeded from the job's `MY_SEED`. The main-path `run_config.ho` seed is
   never consumed by the addon branch, and the addon's `MC_VEGAS` RNG starts
   from a compiled constant.
6. `ceil(B / 5316)` allocates about ten times **more** seeds than
   `ceil(B / 53600)`. The previous text called that “under-sizing”; the direction
   was backwards. Neither formula is ready for production until the intended
   `nmc` and multi-seed behavior are validated.
7. The first CrystalBall repair candidate is now the HELAC-Onia author's stock
   `RANDA_init.inc` recipe. Patch the addon, provide `seed.input`, force one
   clean addon-2 rebuild, and run correlation acceptance experiments before
   publishing the package or considering direct `DSEED` assignment.
8. The production-core pilot's exact 153,293-event planner match validates an
   event-count equality gate, but not the remote contents behind an inventory
   URL. The contaminated-pool recovery requirements therefore remain.
9. The production-core 5/5 source budgets produced 9 rather than 10 merged
   events. Tiny acceptance pilots need explicit source headroom; budget equal
   to target is not a robust plan.
10. A fixed HELAC package does not clear the production-core end-to-end gate:
    ntuple package staging, HepJob status/recovery, schedd selection, persistent
    diagnostics, and cross-workflow output namespacing remain separate fixes.

---

## Item 1 — separate generated output from immutable inventory

> “The LHE files should never end up in the MC_Production_v3. Make it go to
> v4_5_full instead.”

### Confirmed defect

Two generated-LHE paths call `existing_lhe_pool_dir()`:

| Role | Current behavior |
|---|---|
| LHE generation node `output_dir` | Resolves to an exact `lhe_pool_directories.<pool>.path`, normally under `MC_Production_v3/LHE_pool/...` |
| `_resolve_lhe_path()` remote branch | Makes the planner read the same inventory path |

`existing_lhe_pool_dir()` is a read-side resolver. With no
`--existing-lhe-base`, it returns an exact configured immutable-pool path such
as:

```text
root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO
```

Because both writer and reader make the same mistake, a newly generated file
is accepted by the planner after it overwrites the old file. The inventory JSON
still has its original checksum, so the job-spec checksum comparison continues
to pass. That checksum comparison proves only that the selected JSON is the
expected JSON; it does not re-count or fingerprint the remote files.

The intended write layout is already used elsewhere:

```text
<target-base>/lhe_pools/<pool-storage-name>/
```

`run_helac.sh` defaults to `EOS_GENERATED_LHE_BASE/<pool>`, `GEN:` processing
inputs resolve through the generated-LHE base, and block output already derives
from `node_storage_config(...)["generated_lhe_base"]`.

### Recommended code change

Add a write-side resolver whose name and arguments make it impossible to
confuse with the inventory resolver:

```python
def generated_lhe_pool_dir(pool_name: str, target_eos_base: str) -> str:
    base = str(node_storage_config(target_eos_base)["generated_lhe_base"])
    return f"{base.rstrip('/')}/{pool_storage_name(pool_name)}"
```

Use it in both places, with the same `self.options.target_base_url`:

```python
"output_dir": generated_lhe_pool_dir(
    pool.name, self.options.target_base_url
)
```

and:

```python
return (
    f"{generated_lhe_pool_dir(pool_name, self.options.target_base_url)}/"
    f"sample_{storage}_{seed}.lhe.gz"
)
```

Do not pass `existing_lhe_base` to either generated path. Keep
`_resolve_existing_lhe_path()`, `_discover_existing_lhe_files()`, `EOS:` input
resolution, and exact inventory paths unchanged.

The helper should not hide local-storage behavior. The local worker prepends
`LOCAL_OUTPUT_BASE` to `output_dir`, while `_resolve_lhe_path()` currently
expects `<local-output-base>/lhe_pools/<storage>/...`. Add a local regression
test and retain that layout instead of blindly emitting an absolute local path
that the worker would prepend a second time.

### Required write-default decision

The helper fixes the destructive collision with `LHE_pool`, but an omitted
`--target-base-url` still produces:

```text
root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/lhe_pools/...
```

That violates the stronger “no new products in v3” policy. Do not solve this by
changing the existing-inventory base: reads and writes need independent
defaults.

Recommended policy for this branch:

- keep the exact v3 `lhe_pool_directories` as read-only inventory locations;
- require an explicit `--target-base-url` for any remote DAG that can write LHE,
  blocks, processing output, merge output, or ntuples; and
- fail during DAG generation, before runtime bundles or node configs are
  emitted, when a remote write target is absent or resolves to the protected v3
  base.

An explicit configured `default_target_eos_base` pointing to v4_5_full would
also work, but silently reusing `EOS_BASE` cannot express the read/write split.
Requiring the CLI argument is the smaller safety change and matches the current
v4_5_full production commands.

As a second guardrail, reject a generated `output_dir` that equals or is below
any configured exact `lhe_pool_directories.*.path`. This protects the immutable
inventory even if future defaults change.

### XRootD URL acceptance issue

Generated JSON correctly preserves the required triple-slash URL, but the
actual 2026-09-16 worker log prints its `xrdcp` target as:

```text
root://cceos.ihep.ac.cn:1094//store/...
```

`run_helac.sh:resolve_remote_target()` currently reconstructs the URL with one
separator before an already absolute path. The fix must also verify the actual
worker-side `xrdcp` URL, not only the generated config, and preserve:

```text
root://cceos.ihep.ac.cn:1094///store/...
```

This normalization defect is not what caused the overwrite, but it is on the
same stageout path and violates the repository's IHEP URL contract.

### Regression tests required before submission

1. Fresh remote generation with a v4_5_full target:
   LHE `output_dir`, planner `lhe_paths`, and `block_output_dir` all resolve
   under the same target `lhe_pools/<storage>` tree.
2. Existing-LHE mode: planner inputs remain the exact v3
   `LHE_pool/<configured-subdir>` URLs.
3. Supplying `--existing-lhe-base` never changes a generated-LHE destination.
4. A remote write workflow without an approved target fails early under the
   selected policy.
5. Local generation and local planner paths agree exactly.
6. The worker's final `xrdcp` URL retains the endpoint and triple slash.
7. A generated destination equal to or below an inventory directory is
   rejected.

Run the repository-required Python, shell-syntax, planner, inventory, and
generated-DAG checks after adding the focused regression test.

### Inventory damage and recovery

The 2026-09-17 SPS_CS run wrote seeds 60100–61409 into
`MC_Production_v3/LHE_pool/SPS-JpsiJpsi-LO`, including logs and uncompressed
shuffle-split blocks. The campaign spec selects seeds 60100–61099 from the
original counted inventory.

Do not “repair” this by merely recounting the contaminated directory and
updating `source_inventory_sha256`. That would bless an accidental replacement
without establishing physics equivalence.

Recovery order:

1. Freeze the affected pool for new planning.
2. Preserve the original inventory
   `generated/lhe_inventory_jjp_20260717_184812.json` and checksum
   `9c393a70...` as provenance evidence.
3. Restore the overwritten files from a verified immutable copy if one exists;
   otherwise regenerate into a new versioned target directory, never in place.
4. Count `<event>` records for every replacement file and run the applicable
   longitudinal-symmetry/physics validation.
5. Create a new exact inventory and update a new campaign-job-spec version only
   after the replacement set is accepted.
6. Before using a counted inventory, make the planner compare its observed
   `total_input_events` with supplied `lhe_event_counts`; currently those counts
   are carried into provenance but are not enforced as a runtime equality.

Repeat the same protection check for the SPS_G, DPS2/TPS, and DPS1/TPS pools
before any paused campaign is resumed.

---

## Production-core integration constraints

The 2026-09-18 production-core pilot consumed an existing counted inventory; it
did not exercise LHE generation. Its findings refine how the fixes in this note
are packaged and accepted, but they do not replace the generated-LHE resolver
or CrystalBall source repair.

| Pilot finding | Consequence for this plan |
|---|---|
| Counted input matched 153,293 exactly | Add the same observed-versus-declared event-count equality to the legacy planner. Keep remote-content recovery separate because URL plus inventory checksum does not fingerprint the remote file. |
| F1: IHEP ntuple cannot see `common/packages` | Do not call an LHE-only or merge-only success an end-to-end acceptance. Materialize the runtime-manifest-selected ntuple bundle under the native path expected by `run_chain.sh`, and test both prebuilt-runtime and source-fallback cases. |
| F2/F5: `hep_q` was called incorrectly and finished jobs disappear | Query `hep_q -i <id>`; parse recognized job rows rather than error text. Unparseable or absent results are `unknown`, then reconciled against checkpoints/products. They are never automatic evidence for `batch resume`. |
| F3/F6: schedd discovery and `myschedd bump` disagree | Resolve/pin `SCHEDD_HOST` once in the run record and use that exact schedd for submit, monitoring, and recovery. |
| F4: diagnostics lived only in node-local `/tmp` | Persist the compile log, addon stdout, and per-stage failure diagnostics in the run directory before scratch cleanup. |
| F7/F8: dirty sources and one-shot directories | Import and verify the fixed package before preparing a clean immutable runtime. Use a new release/run directory for retries instead of deleting accepted evidence. |
| F9: legacy worktree package symlinks are archived as dangling links | Transfer the accepted HELAC package as a regular file. Do not reuse the production-core worktree's `common/packages/*.tar.gz` symlinks in a legacy bundle. |
| F13: 5/5 budgets yielded a 5+4 event merge | For an exact tiny-pilot target, provision per-source budget above the target using measured efficiency and reserve. Record produced and consumed events per source; do not silently relabel 9 as a 10-event acceptance. |
| F15: both systems emit `JOB000000_BLOCK000000` | Put legacy and production-core runs in distinct path namespaces and record implementation, run ID, runtime revision, and package checksum in manifests. The internal block ID alone is not globally unique. |

The HELAC artifact should therefore be repaired and accepted first, copied as
the same regular versioned package into both workspaces, and only then adapted
to the production-core runtime. The package repair and the refactor's IHEP
ntuple/runtime repair are independent changes with independent tests.

---

## Item 2 — measure the CrystalBall `nmc` response correctly

> “Run an LHE-only HELAC pilot to see the true effect of altering `nmc` along
> with the configuration input file in the addon directory. Upgrade that
> configuration architecture only later.”

### What remains valid from the earlier investigation

In `addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90`:

- `unwevt` is read into `nwri_tot`, but the addon unweighting writer never
  checks that value as a stop condition;
- `nmc` becomes `ncalm` and controls the doubling schedule;
- `itmax` is forced to one; and
- the final accepted-event count must be measured from the output LHE.

Therefore `unwevt` cannot promise a CrystalBall output count. The current
pilot's 5,316 events at `nmc=500,000` are an exact count and a legitimate point
for that exact runtime/card/cut combination.

### Exact inventory comparison

The size-derived estimates in the first draft are replaced here by counted
inventories.

The percentiles below use the sorted lower order statistic; the median is the
mean of the two middle counts.

| Inventory | SHA-256 | Files | Total events | p0 | p25 | median | p75 | p95 | p100 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Original authoritative inventory | `9c393a70...` | 2000 | 141,412,548 | 6,084 | 50,329 | 53,644.5 | 84,682 | 154,009 | 393,965 |
| 2026-09-17 recount after seeds 100/101 were replaced | `dc771d21...` | 2000 | 141,212,030 | 5,316 | 50,320 | 53,632.5 | 84,342 | 154,009 | 393,965 |

The only changed event counts in that recount are:

| Seed | Original count | Recounted count |
|---:|---:|---:|
| 100 | 134,791 | 5,316 |
| 101 | 76,359 | 5,316 |

The two new files were also reported byte-identical after decompression. This
shows that nominal job seeds 100 and 101 did not vary the addon RNG stream.
It does **not** show that 5,316 is the wrong response at
`nmc=500,000`.

The later five-leg check is stronger: at `nmc=2,000,000`, nominal seeds
100, 101, 500, 5000, and a repeat of 100 all produced 12,926 events,
8,867,505 uncompressed bytes, and the same uncompressed SHA-256
`b59fd0fa...`. Their compressed hashes differed only because gzip recorded
different header metadata. Payload checks in this plan must therefore hash the
decompressed LHE.

The historical median is compatible with the historical production default
`nmc=5,000,000` being about ten times the pilot setting, but that is a
hypothesis, not a controlled response curve: the historical package, cuts,
cards, and seed mechanism must be treated as potential confounders.

### Root cause of the identical nominal seeds

The earlier note said that the job seed was plumbed through `run_config.ho`.
That is true only for the main `ho_cluster` branch.

For a CrystalBall pool, `run_helac.sh` instead:

1. prepares `addon/pp_psiX_CrystalBall/input/user.inp`;
2. copies `state.inp` and `crystalball.inp`; and
3. executes `HO_pp_psiX_CrystalBall` directly.

It never creates or feeds `run_config.ho` for that branch. More importantly,
the addon uses `MC_VEGAS:RANDA`, whose `DSEED` is initialized to the compiled
constant `1234567d0`; this RNG does not read `input/seed.input`. Merely copying
or editing `seed.input`, or setting `ranhel`, will not seed the VEGAS stream.

Consequences:

- the current `--seed` changes filenames and shuffle seeds but not the addon
  integration/unweighting stream;
- a “three different seeds” pilot before fixing addon seeding would repeat the
  same physics sample; and
- seed-varying CrystalBall production files are blocked on a minimal RNG-seed
  plumbing fix, even if the broader addon-configuration refactor remains
  deferred.

### Refined pilot sequence

#### Phase A — source repair and one-time addon-2 package rebuild

Patch the HELAC-Onia source so the CrystalBall addon reads `seed.input` and
executes the stock `RANDA_init.inc` before its first VEGAS call. This follows
the author's recipe and is the first candidate to test.

The source insertion has this shape:

```fortran
    INTEGER::beam1_pdf,beam2_pdf
    INCLUDE "../../../src/RANDA_init.inc"
    WRITE(*,*)'                     THE BEGINNING OF HELAC-Onia'
```

The include advances the fixed Park-Miller stream by the integer stored in
`seed.input`. Adjacent values therefore start one scalar draw apart. In
CrystalBall those values are regrouped into multidimensional VEGAS points,
passed through adaptive grids and cuts, and followed by conditional
unweighting draws. That can decorrelate final observables, but it is an
empirical property to test rather than a mathematical guarantee. The earlier
claim that adjacent values necessarily produce near-duplicate LHE samples was
too strong.

In a clean staging tree, remove both old executable paths:

```bash
rm -f "${helac_dir}/bin/HO_pp_psiX_CrystalBall"
rm -f "${helac_dir}/addon/pp_psiX_CrystalBall/bin/HO_pp_psiX_CrystalBall"
```

Then run a small `ho_cluster` command file containing:

```text
set seed = 31001
set nmc = 10000
set unwevt = 20
generate addon 2
launch
exit
```

`link_addon()` must now compile addon 2 from the patched source. Retain the
compile log, normalize the addon's binary link to a relative link, and save the
patched source plus worker-compatible executable as one immutable versioned
candidate package. Do not publish it into the production workspaces until the
acceptance experiments below pass.

The current bare-addon branch still needs a small bridge until it is refactored:

```bash
printf '%s\n' "${MY_SEED}" > "${addon_dir}/input/seed.input"
```

The new source reads this as the number of initial `RANDA` draws to discard.
The bridge is removed once `ho_cluster:set_seed()` owns the production
invocation.

This smoke proves rebuild and seed-reader execution. It does not establish
production yield or acceptable decorrelation.

#### Phase B — deterministic `nmc` response with the accepted package

Run one LHE-only CrystalBall job for each:

```text
nmc = 250000, 500000, 1000000, 2000000
```

Hold the accepted fixed package and RANDA skip seed,
`crystalball_jpsi.inp`, `state.inp`, all `user.inp` values, cuts, and
compiler/build constant. This measures the response to `nmc`; it is not a
seed-spread study.

The addon rounds behavior through its doubling schedule, so record the actual
final `NCALL` and every integration step from stdout, not just the requested
`nmc`. For the current source, the four requested points are expected to end
with final unweighting `NCALL` values of 160k, 320k, 640k, and 1.28M,
respectively; confirm rather than assume those values from stdout. Repeat one
point once as a deterministic reproducibility check.

Each fixed-seed/`nmc` leg must use a unique output directory. The normal output
name is only `sample_<pool>_<seed>.lhe.gz`; reusing a directory for a fixed-seed
sweep would overwrite one leg with the next.

Before calling the package accepted, run E0-E3 from
`docs/crystalball_addon_seed_fix_plan_2026-09-18.md`:

- same seed + same inputs twice gives the same uncompressed LHE checksum;
- adjacent, moderately separated, and widely separated skip seeds give
  different uncompressed event streams;
- stdout records the requested RANDA skip seed;
- the package contains the patched source and rebuilt binary with recorded
  checksums; and
- repeated adjacent-pair and wide-pair comparisons detect no operationally
  material correlation at a predeclared sensitivity. Different hashes alone
  are insufficient.

If the upstream recipe fails the correlation gate, test direct `DSEED`
initialization with the same experiments. If the generator period or measured
draw consumption remains inadequate, replace the RNG or use fewer longer runs.
After a candidate passes, publish it immutably, copy the package bytes—not a
symlink—into both workspaces, and verify the same checksum before any refactor.

#### Phase C — accepted multi-seed yield and CPU sample

At the selected `nmc`, run multiple seeds accepted by the decorrelation gate.
Count events exactly, record CPU/wall time, and report the minimum, median,
range, and yield per CPU hour. Do not label the streams mathematically
independent, and do not use Poisson `sqrt(N)` alone as the uncertainty on
seed-to-seed yield.

For an event budget `B`, define a documented conservative planning yield
`Y_plan` from this accepted multi-seed sample, then use:

```text
initial CrystalBall seeds = ceil(B / Y_plan)
```

Add reserve or incremental submission based on counted outputs. Do not use the
single deterministic 5,316 count or the historical 53.6k median as the final
production denominator.

If this yield is used in a tiny downstream mixing pilot, choose each source
budget so its expected accepted yield exceeds the requested mixed target by a
documented margin. Report planned, attempted, accepted, and mixer-consumed
events per source slot. The production-core 5/5-budget pilot's 5+4 result is the
counterexample to using `budget == target`.

#### Phase D — CrystalBall card and cut comparisons

After the baseline curve, vary one input family at a time:

1. CrystalBall card contents;
2. addon-local `user.inp` patches; and
3. CrystalBall-relevant `minpt1c`, rapidity, and `absoluterap` settings.

Use paired runs at the same RANDA skip seed and `nmc`. Do not change the
card and cuts together and attribute the difference to one of them. Preserve
the exact effective `user.inp`, `state.inp`, `crystalball.inp`, package/source
revision, and their checksums with every result.

The paired test separating restored rapidity cuts from
`minmqqp`/`minmqbeam` belongs to the main-path `pool_2jpsi_g`/NLOstar
investigation. Keep it as a separate experiment; it is not a CrystalBall
`nmc`-sweep leg and should not delay the deterministic addon curve.

### Pilot acceptance record

For every leg retain:

- fixed-package release name, package/source/binary checksums, compiler/build
  identity, and implementation/run namespace;
- requested `nmc`, effective integration schedule/final `NCALL`, and `unwevt`;
- requested workflow seed and effective RANDA skip seed;
- exact LHE event count and uncompressed checksum;
- wall and CPU time;
- integration result/uncertainty printed by the addon;
- exact cuts and copies/checksums of all effective addon inputs; and
- a unique v4_5_full output URL using the canonical triple-slash form.

No phase may write to `MC_Production_v3` or reuse the canonical inventory
filenames there.

---

## Ordered work before resuming production

1. Implement the generated-LHE resolver and write-target guardrails.
2. Fix and test worker-side triple-slash URL reconstruction.
3. Quarantine and restore/regenerate the contaminated SPS-JpsiJpsi-LO
   inventory; do not merely re-hash it.
4. Add the stock `RANDA_init.inc`, add the direct-runner seed-file bridge,
   remove the old CrystalBall binary in a clean staging tree, and force one
   small addon-2 rebuild.
5. Build a versioned candidate package and run the reproducibility,
   seed-sensitivity, and observable-correlation experiments.
6. Accept the upstream recipe or escalate to direct `DSEED` initialization or
   RNG replacement according to the measured result.
7. Publish one immutable accepted package, then copy the regular file with the
   same checksum into both workspaces.
8. Run the fixed-seed CrystalBall `nmc` sweep and accepted multi-seed yield
   sample in unique v4_5_full locations.
9. Freeze the DPS1/TPS seed allocation only after defining `Y_plan` and source
   headroom from those measurements.
10. Separately fix the production-core ntuple package view, HepJob
   reconciliation, schedd pinning, diagnostics retention, and output namespace;
   then rerun its end-to-end pilot.

Nothing in this review authorizes submission or resumption of the paused main
rerun.

## Related evidence

- `docs/unwevt_investigation_2026-09-17.md`
- `docs/crystalball_seed_issues_and_options_2026-09-18.md`
- `docs/crystalball_addon_seed_fix_plan_2026-09-18.md`
- `.worktrees/production-core/docs/mcprod_ihep_pilot_findings_2026-09-18.md`
- `docs/helac_absolute_rapidity_fix.md`
- `docs/v44_dps1_good_rerun_summary.md`
- `generated/lhe_inventory_jjp_20260717_184812.json`
- `generated/lhe_inventory_jjp_20260717_184812_recounted.json`
- `generated/v4_5_pilot_dps1_20260916_130904/`
- `common/campaign_job_specs/jjp_sps_cs_full_v2.json`
