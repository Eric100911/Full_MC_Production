# CrystalBall seed fix — refactor catch-up

Date: 2026-09-19
Status: fix committed (`931c933`) and package published in the **legacy** workflow.
This note tells the `.worktrees/production-core` refactor what to port so it does
not silently regenerate unseeded CrystalBall LHE.

## What was fixed

The CrystalBall addons (`pp_psiX_CrystalBall`, used by `pool_jpsi_CSCO_g` and
`pool_upsilon_CSCO_g`) never read the job seed: their RNG started from a compiled
`DSEED` constant, so every seed produced the identical physics sample. The fix
adopts the upstream `RANDA_init.inc` idiom — the addon reads `seed.input` and
burns `iseed` RANDA draws before integrating.

## Changes (legacy workflow) that the refactor must mirror

1. **`tools/repack_helac_crystalball_seed.py`** — idempotent repack/patch tool. It
   inserts `INCLUDE "../../../src/RANDA_init.inc"` after the unique
   `INTEGER::beam1_pdf,beam2_pdf` anchor in `addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90`,
   re-tars the nested HELAC + outer delivery package, and writes a sha256 manifest.
   `--check` verifies the packaged source.
2. **`lhe_generation/run_helac.sh`** — `prepare_crystalball_addon_input()` writes
   `input/seed.input` from `MY_SEED` (the "skip seed"); `normalize_prebuilt_helac_links()`
   now also normalizes the `HO_pp_psiX_CrystalBall` symlink to a relative link.
3. **`dag_generator.py`** — generated LHE resolves through `generated_lhe_pool_dir()`
   (write target `<target-base>/lhe_pools/<pool>`) instead of `existing_lhe_pool_dir()`
   (read-only inventory), and `assert_generated_dir_not_inventory()` rejects any
   output dir that overlaps an immutable inventory path.

## Published package

`common/packages/helac_package.tar.gz` is now the **patched** package
(sha256 `407a62f3bdc29774f9537a8326a69e6665dd1c17a02d5c952e242a7d3cd55760`).
Provenance (upstream `RANDA_init.inc` sha256 `820898de…`, patched source `1f2d67e6…`,
input package `addacf1a…`) is in `common/packages/helac_package_cbseed_v1.manifest.json`.
The unpatched original is backed up as `helac_package.tar.gz.pre-cbseed`. The
versioned release `helac_package_cbseed_v1.tar.gz` is **tracked in git** (force-added,
commit `e124496`); the mutable runtime name `helac_package.tar.gz` remains gitignored
(a copy of the same bytes).

## Validation already done (E0–E3 + nmc sweep)

- E0 build+plumbing, E1 determinism, E2 seed sensitivity, E3 decorrelation
  (0 exact overlap across 3 adjacent pairs; closest-direction ΔR matches the
  within-sample reference) — all pass.
- Cross-section stable ~144–145k nb across all seeds.
- Per-seed unweighted yield is ~80× variable (204–16,486 events at `nmc=500k`) and
  **non-monotonic in `nmc`** — `nmc` is not a yield knob.

## What the refactor must port

1. **Use the patched package** — the tracked `common/packages/helac_package_cbseed_v1.tar.gz`
   (sha256 `407a62f3…`) checks out via git; copy it to the runtime name
   `helac_package.tar.gz`, or run `tools/repack_helac_crystalball_seed.py` against its
   own copy. Do not ship the stale unpatched tarball.
2. **Seed bridge** — if the refactor worker runs the CrystalBall addon directly,
   write `MY_SEED` into `addon/pp_psiX_CrystalBall/input/seed.input` before launch.
3. **Write-side resolver** — generated LHE must go to `<target-base>/lhe_pools/<pool>`,
   never the immutable inventory; guard against inventory-overlapping output dirs.

## References

- `docs/crystalball_addon_seed_fix_plan_2026-09-18.md` — acceptance plan (E0–E4).
- `docs/crystalball_seed_issues_and_options_2026-09-18.md` — defect mechanism + option space.
- `docs/lhe_output_location_and_crystalball_nmc_2026-09-18.md` — resolver/guardrail/`nmc`.
- `docs/helac_build_and_tooling_lessons_2026-09-19.md` — el7 build recipe + gotchas.
