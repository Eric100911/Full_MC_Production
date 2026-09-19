# HELAC el7 build, tooling, and operating lessons

Date: 2026-09-19
Status: living reference captured from the CrystalBall seed-fix missions
(multi-agent implementation + E0 addon-2 rebuild smoke). Net-new knowledge not
covered by the seed-defect or LHE-location docs.

Companions:

- `docs/crystalball_addon_seed_fix_plan_2026-09-18.md` — acceptance plan (E0–E4).
- `docs/crystalball_seed_issues_and_options_2026-09-18.md` — defect mechanism + option space.
- `docs/lhe_output_location_and_crystalball_nmc_2026-09-18.md` — resolver/guardrail/`nmc`.

---

## 1. HELAC `el7` build & run recipe

The faithful worker-equivalent build runs in the `cmssw/el7` container with the
LCG_88b toolchain. This is reusable for E1–E3 and any future HELAC work.

```bash
singularity exec -B /tmp/chiw,/cvmfs,/afs \
  /cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el7:x86_64 \
  bash <script>
```

Inside the container (mirrors `run_helac.sh:setup_build_env`):

```bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/sft.cern.ch/lcg/views/LCG_88b/x86_64-centos7-gcc62-opt/setup.sh
export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/releases/LCG_88b/Boost/1.62.0/x86_64-centos7-gcc62-opt/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/lib64:/opt/rh/gcc-toolset-12/root/usr/lib64:$LD_LIBRARY_PATH
export PATH=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2.0/x86_64-centos7-gcc62-opt/bin:/opt/rh/gcc-toolset-12/root/usr/bin:$PATH
export LANG=C LC_ALL=C
```

Build order:

1. HepMC2: `tar -xzf hepmc2.06.11.tgz` → `./configure --with-momentum=GEV --with-length=MM` → `make -j 2` → `make install`.
2. HELAC main: `tar -xzf HELAC-Onia-2.7.6.tar.gz` → `sed` the two worker fixes (disable `hepmc_path`; strip `HEPTopTagger::` in the fjcore interface) → `./config`.
3. CrystalBall addon: `mkdir -p …/output …/tmp` then `make -C addon/pp_psiX_CrystalBall -f makefile_pp_psiX_CrystalBall HODIR=<helac_dir> FC=gfortran all`.

Key facts:

- **`./config` is a full build**, not just a configure. It writes the makefile,
  runs `make` (config:730 — builds `libpdf/libqedps/libminuit/libhbook` + fjcore),
  and then creates the `ho_cluster` / `bin/ho_cluster` symlinks. There is no
  separate top-level `make`; `ensure_helac` in `run_helac.sh` calls only `./config`.
- **The addon needs `output/` and `tmp/` at runtime.** It opens `./tmp/temp.inp`
  and writes `./output/*.lhe`. Omitting `mkdir -p output tmp` yields
  `Fortran runtime error: Cannot open file './tmp/temp.inp'`.
- The addon writes `addon/pp_psiX_CrystalBall/output/sample_pp_psiX_crystalball.lhe`.
- **`nmc=10000` produces 0 events** (header-only LHE). E0 (plumbing) is fine at
  tiny `nmc`; E1–E3 need `nmc ≥ ~500000` to yield events.

---

## 2. LCG/cmssw shell-environment gotchas

- **Never name a shell variable `BASE`** when LCG is sourced.
  `contrib/gcc/6.2/x86_64-centos7/setup.sh:4` sets
  `BASE=/cvmfs/sft.cern.ch/lcg/releases/gcc/6.2.0-b9934/x86_64-centos7`, and
  `source` runs in your shell, so your `BASE` is silently clobbered — a later
  `rm -rf "$BASE"` then deletes the read-only cvmfs gcc tree. Use `WORKDIR`,
  `BASE_DIR`, or a unique name. (`run_helac.sh` already uses `WORKDIR`/`BASE_DIR`.)
- **`set -u` breaks the LCG setup.** `setup.sh:14` references `$COMPILER` when
  unset → `COMPILER: unbound variable`. Match the worker: `set -e` (optionally
  `-o pipefail`), never `set -u` when sourcing LCG/cmsset.
- **`exec >>log 2>&1` misorders the log.** stdout is block-buffered, stderr is
  unbuffered, so a `set -e` abort flushes stdout markers *after* the stderr errors
  that caused it. When diagnosing a build log, filter noise
  (`grep -v 'rm: cannot remove'`) and trust the last non-noise line, not visual order.

---

## 3. Multi-agent workflow (Workflow tool)

- **JS template literals interpolate `${…}`.** Literal shell variables
  (`${MY_SEED}`, `${addon_dir}`) inside a backtick-delimited prompt string are
  treated as JS interpolation and throw at parse time (`addon_dir is not defined`)
  before any agent runs. Pass such text as single-quoted JS strings with `\n`,
  or escape as `\${…}`.

---

## 4. Packaging / repack

- **Idempotent patch-tool pattern** (see `tools/repack_helac_crystalball_seed.py`):
  anchor match must be exactly 1; fail-closed on a displaced include; sha256
  manifest written atomically beside the package; a `--check` mode that never
  re-inserts; never overwrite the input package in place.
- **gzip is not byte-reproducible.** The mtime in the gzip header makes the
  outer/nested tarball sha256 vary per build even when the payload is identical.
  `--check` must therefore compare against the sibling manifest, and byte-stable
  "immutable" publication needs gzip-mtime normalization.

---

## 5. Operating rules (consolidated)

- **Verify a seed reaches the RNG, not just filenames.** The CrystalBall addon
  was born unseeded: the job seed changed filenames and shuffle seeds but never
  the VEGAS stream. A "three different seeds" test before fixing seeding would
  have silently repeated one physics sample.
- **Hash decompressed payloads.** `.gz` sha256 differs for identical uncompressed
  LHE (gzip metadata). All "identical file" conclusions must be drawn on the
  decompressed bytes.
- **Read vs write resolvers must be distinct.** `existing_lhe_pool_dir` resolves
  the immutable inventory; `generated_lhe_pool_dir` resolves the write target.
  Using the read-side resolver for writes is how generated LHE landed in
  `MC_Production_v3`.
- **A generated file ≠ a loaded file.** Config/log presence proves generation,
  not that a running process consumed it — always confirm in the live worker log.
