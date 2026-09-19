# CrystalBall RNG seeding — issues and candidate solutions

Date: 2026-09-18
Updated: 2026-09-19
Status: **evidence and option record. On 2026-09-19 the stock
`RANDA_init.inc` path was selected as the first candidate for acceptance
experiments; it is not yet accepted for production.**
Scope: JJP CrystalBall pools only (`pool_jpsi_CSCO_g`, `pool_upsilon_CSCO_g`).

Evidence base: the shipped `common/packages/helac_package.tar.gz`
(`HELAC-Onia-2.7.6.tar.gz`, unpacked to `/tmp/chiw/helac_shipped`), the
`run_helac.sh` git history, and measurements against the live IHEP pool. **No
HELAC source was modified to produce this document.**

A separate file, `docs/crystalball_addon_seed_fix_plan_2026-09-18.md`, contains
the worked implementation, acceptance, and publication sequence for the first
candidate. This document retains the broader option record. Where that plan is
one of the options below it is cross-referenced, and it should be read as an
instance of Option family A + B, not as the conclusion.

> **Notation.** `[M]` = measured in this investigation. `[E]` = estimate or
> inference from code, not measured. `[U]` = unresolved / open.

---

## 0. What is actually wrong, in one paragraph

The CrystalBall addon's random-number stream starts from a **compiled-in
constant** and nothing in the workflow can move it. Every job therefore produces
the same physics sample, and the job `--seed` reaches only the output filename,
log labels and the shuffle-split seed. Measured directly: five legs at
`nmc=2000000`, `unwevt=2000`, seeds 100 / 101 / 500 / 5000 / 100-repeat all
produced **12,926 events, 8,867,505 bytes and one shared uncompressed SHA-256**
`b59fd0fa…`, with the worker log confirming each leg ran under its intended seed
`[M]`. The compressed `.lhe.gz` files gave five *different* hashes (gzip header
mtime only) — a false-negative that has already misled one reading of this data.

Consequence: a CrystalBall production budget cannot be sized from repeated
measurement, because repeated measurement returns the same sample.

---

## 1. Issues

### I1 — The seed never reaches the addon (two independent breaks)

**I1a — the shell never writes it.** `prepare_runtime_user_inp`
(`lhe_generation/run_helac.sh:854`) rewrites the Monte-Carlo block of `user.inp`
with a fixed substitution list (`:868-884`):
`cmass bmass minptq minptconia minptbonia minmqqp minmqbeam preunw unwevt nmc
nopt nopt_step noptlim gener ranhel` — fifteen keys, no `seed`. On the
CrystalBall branch `prepare_crystalball_addon_input` (`:947`) calls it with the
**addon's** directory (`:958`), so it is the addon's `user.inp` being rewritten.

**I1b — `set seed` is unreachable.** The only writer of `set seed = ${MY_SEED}`
is `run_config.ho` (`:1392` heredoc, `:1414`), which lives inside the `else`
branch at `:1388` and is consumed by `./ho_cluster` at `:1445`. The CrystalBall
branch (`:1371-1379`) never creates `run_config.ho`.

**I1c — the addon could not read it anyway.** `grep -rni seed` over
`addon/pp_psiX_CrystalBall/` (64 files, source + makefile + `.inp`) returns **zero
matches**. Its only RNG is `RANDA` from `src/MC_VEGAS.f90`, driven by
`DSEED=1234567d0` (`:44`), and an exhaustive tree-wide `DSEED` grep shows it is
written in exactly one place — the LCG step at `:63-64`. Nothing assigns it from
input. The addon's `input/` directory ships no `seed.input`.

Age: the CrystalBall branch was introduced 2026-05-25 in `9e13f89`, and at that
same commit `set seed` already sat inside the other branch. `set seed` has
appeared exactly once in `run_helac.sh` in every commit back to `20a0c9d`
(2025-12-23). **The feature was born with the defect; there was never a working
version.** That is ~116 days as of this document.

### I2 — The invocation chain bypasses the unified interface

The framework has a first-class addon mechanism. `addon/addon_process.dat`
registers the CrystalBall addon as **ID 2** (`pp_psiX_CrystalBall`), and the
unified chain is:

```
run_config.ho `set seed = N`
  -> helaconia_run_interface.py:100  options['seed'] default '7'
  -> :2586 set_seed() writes $(HODIR)/input/seed.input = N
  -> :2156 link_addon() builds the addon to $(HODIR)/bin/HO_pp_psiX_CrystalBall
  -> create_subdir.sh $subdir HO_pp_psiX_CrystalBall
       copies $(HODIR)/input/{process,default,user,seed}.inp -> $subdir/input/
       ln -s $(HODIR)/bin/HO_pp_psiX_CrystalBall  $subdir/Helac-Onia
  -> CWD=$subdir  =>  input_dir="./input/" = $subdir/input/
```

`input_dir` is a compile-time constant, `CHARACTER(len=24),PARAMETER::input_dir="./input/"`
(`src/Helac_Global.f90:224`), resolved against the process CWD. `create_subdir`
copies `seed.input` among its list (`cluster/create_subdir.inc:143`), and the
addon is symlinked **as** `Helac-Onia`, never invoked bare.

Our path instead runs `cd addon/pp_psiX_CrystalBall && ./bin/HO_pp_psiX_CrystalBall`
(`run_helac.sh:1376`), so `input_dir` resolves to the addon's own `input/`.
`prepare_crystalball_addon_input` writes `user.inp`/`state.inp`/`crystalball.inp`
there, so the layout is internally consistent — but there is no `seed.input`, and
nothing in the framework populates one.

**This is why I1a and I2 interact:** adding the missing `INCLUDE` (see S3) without
addressing the invocation would open a non-existent `seed.input` at
`status='OLD'`. The upstream fix for `pp_NOnia_MPS` works precisely because that
addon runs under the unified chain: its own `input/` also ships no `seed.input`,
yet it reads `$subdir/input/seed.input`.

### I3 — The upstream seeding idiom has its own weakness

`src/RANDA_init.inc` — the idiom used by `Colliders_PSI3.f90` (×4) and by
`pp_psicc_DPS2`, `pp_psipsi_DPS`, `pp_oniumjet_NLO`, `pp_psipsi_DPS2`,
`pp_psipsipsi_DPS` (×2), and inlined verbatim in `pp_psiY_SPS.f90:297-307` —
**burns `iseed` draws** rather than assigning `DSEED`:

```fortran
OPEN(777,FILE=TRIM(input_dir)//'seed.input',status='OLD')
READ(777,*)iseed
WRITE(*,*)'Random number seed',iseed
CLOSE(777)
IF(iseed.GT.0)THEN
   DO ililil=1,iseed
      CALL RANDA(1,ranrrr)
   ENDDO
ENDIF
```

So `seed.input = N` starts a run **N draws into a fixed stream**. Two seeds
differing by 1 (e.g. the generator's native 100 and 101) would produce runs whose
streams differ by **one draw** — sharing essentially every random number.

Because the two runs' configurations differ only in the seed, their *outputs*
can still differ in bytes, event count and hash. In CrystalBall, VEGAS groups
several consecutive scalar draws into one multidimensional point, adapts its
grid, and later consumes conditional unweighting draws. That machinery may
substantially decorrelate final observables even though the input scalar
streams overlap. The source alone therefore supports neither “near-duplicate
physics” nor “guaranteed independence”; the upstream recipe needs an
observable-correlation acceptance experiment.

Note also that the recipe's insertion point is addon-specific: `pp_NOnia_MPS.f90`
has its first executable statement at line 35, so "insert after line 34" is
correct there; `pp_psiX_cb.f90` has a further declaration (`INTEGER::beam1_pdf,beam2_pdf`,
line 37) that `pp_NOnia_MPS.f90` lacks, so the equivalent insertion is **after
line 37**. Because `RANDA_init.inc` ends in executable statements, inserting
one line early is a compile error, not a silent no-op.

### I4 — The generator's period may cap the number of independent samples `[E]`

The only RNG is a Park–Miller LCG: `DSEED = DMOD(16807*DSEED, 2147483647)`,
period `2^31-2 ≈ 2.1e9` states. A production run at `nmc=5000000` consumes on the
order of `nmc × varnum` draws `[E]` (order `1e7`–`1e8`), so the number of
**non-overlapping** streams available is order tens `[E]`, not thousands.

If that estimate holds, a 2000-file independent pool is not attainable with this
generator by any seeding arrangement, and the constraint applies to the
production budget rather than to the plumbing. **This is an estimate from code
reading, not a measurement** — see Q3.

### I5 — Delivering and keeping a source fix

- `common/packages/helac_package.tar.gz` is **untracked in git**; its nested
  `HELAC-Onia-2.7.6.tar.gz` is the source of truth unpacked by every worker.
  `common/packages/README.md:24` documents a manual rebuild procedure.
- `run_helac.sh` today patches **no** Fortran at runtime; it patches only input
  cards. There is no existing mechanism to ship a source edit.
- `inspect_helac_package` (`dag_generator.py:2953`) validates only that the two
  nested tarballs are present. A tarball rebuilt from upstream would silently
  drop the fix.
- `build_crystalball_addon_if_needed` (`:990`) short-circuits on an existing
  binary, and the addon makefile's `all : clean $(exe)` always cleans first.
  The addon is currently rebuilt on every worker (the shipped `addon/.../bin/` is
  empty), but that is incidental, not enforced.

### I6 — Nothing would have caught this, and nothing will catch a regression

The pipeline has no positive assertion that a seed reached the addon, and no
assertion that two seeds produced different samples. The failure is silent: every
job exits 0, every file is well-formed, `unwevt` targets are met, and DAGMan
reports success. The only symptom is that the physics does not vary.

The natural check is also booby-trapped: comparing `.lhe.gz` hashes gives
**false negatives**, because the gzip header carries mtime. `[M]` Identical
payloads produced five distinct compressed hashes in the seed sweep.

### I7 — Pool data integrity and provenance

`root://cceos.ihep.ac.cn:1094///store/user/chiw/MC_Production_v3/LHE_pool/SPS-Jpsi`,
2000 files, seeds 100..2099.

- **`[M]` Seeds 100 and 101, written 2026-09-16, are byte-identical** — 5,316
  events each, uncompressed SHA-256 `6b405bd4…`, with differing `.gz` hashes. The
  run's `log_manifest` records `nmc=500000 unwevt=2000 preunw=50000`. This is the
  defect's footprint in live production data.
- **`[M]` The 2026-06-09 bulk (1998 files) does not carry the same signature.**
  All 1998 have distinct sizes; sampled event counts span 6,084 / 50,329 / 53,662 /
  84,342 / 224,997; eight consecutive seeds (1628–1662) share an identical `<init>`
  configuration yet scatter independently in integral (3944.9–4009.8) and error
  (10.38–14.78). All were written 2026-06-09 12:16:13–12:45:45 at **0.89 s/file**,
  i.e. a stage-out, not generation.
- **`[E]` That bulk cannot have come from `run_helac.sh` as it stood that day:**
  `1c4d165` hardcodes production `NMC=5000000 UNWEVT=100000 PREUNW=500000`
  (constant config), and constant config plus a fixed RNG forces identical files.
- `[U]` Its actual provenance is **unresolved**. Both "per-job-varying RNG" and
  "per-job-varying `nmc`" fit the observations; the two cannot be separated from
  outside the pool.

Consequence: **the v3 `SPS-Jpsi` pool is not known to be homogeneous**, and
2026-09-16 CrystalBall-addon samples were written into it.

---

## 2. Candidate solutions

Grouped by the issue they address. Options within a group are alternatives;
options across groups compose (see §3). Pros and cons are stated as trade-offs,
not as a ranking.

### S1 — How the seed reaches the addon

| | Option | Mechanism | Pros | Cons |
|---|---|---|---|---|
| **S1-a** | **Keep bare invocation; write `seed.input` locally** | `prepare_crystalball_addon_input` writes `addon/pp_psiX_CrystalBall/input/seed.input`, since `input_dir` already resolves there | Smallest diff; no change to output layout or harvesting; fully inside the existing branch | Diverges further from the unified interface; the repo owns seeding forever; a second place must know about `input_dir` semantics |
| **S1-b** | **Route through `./ho_cluster`** | Replace `:1371-1379` with the main-path shape; select the addon via its registered ID 2 | Uses the framework's own `set_seed()` and `create_subdir.sh`; deletes the bespoke writer; removes the bare invocation | Largest change; output lands under run dirs, not `addon/.../output/`; requires resolving the spikes in §4 |
| **S1-c** | **Hybrid / staged** | S1-a to unblock measurement, S1-b later | Separates "make it work" from "make it right"; each stage independently verifiable | Two migrations; S1-a code is throwaway; interim state is a third configuration to reason about |

### S2 — What value goes into `seed.input`

| | Option | Mechanism | Pros | Cons |
|---|---|---|---|---|
| **S2-a** | **Burn, seeded with the raw job seed** (literal upstream recipe) | `seed.input = MY_SEED` | Matches upstream byte-for-byte; the multidimensional/adaptive calculation may decorrelate final observables | Adjacent scalar streams overlap almost completely; different output does not prove acceptable decorrelation, so S5-c is required |
| **S2-b** | **Burn, with a strided value** | `seed.input = f(MY_SEED)` with stride ≫ per-run consumption | Keeps the upstream `.inc` unmodified; one-line change in the shell | Burn cost scales with the value; the achievable stride may be capped by burn time, which may in turn be smaller than per-run consumption (I4) |
| **S2-c** | **Assign `DSEED` directly** | Repo-owned `.inc` sets `DSEED = f(seed)` instead of burning | Exact, zero-cost control of the initial state; `DSEED` is `PUBLIC` and already `USE`d at `pp_psiX_cb.f90:6`, so no `MC_VEGAS.f90` edit | Still selects positions on one Park-Miller cycle; deviates from the upstream idiom and needs the same correlation tests |
| **S2-d** | **Replace the RNG** | Substitute a period-appropriate generator | Removes I4 as a constraint | Largest possible change; touches physics validation; scope well beyond this defect |

### S3 — Making the addon able to read a seed

| | Option | Mechanism | Pros | Cons |
|---|---|---|---|---|
| **S3-a** | **Include the stock `../../../src/RANDA_init.inc` after the final declaration in `pp_psiX_cb.f90`** | The upstream fix, using the package's existing include | Matches the `pp_NOnia_MPS` / `pp_psiY_SPS` precedent; keeps one authoritative upstream include | Requires `seed.input` to exist in whatever `input_dir` resolves to (couples to S1); the relative path and insertion anchor are addon-specific (I3) |
| **S3-b** | **Own `.inc` in the repo** | Ship a repo-owned include (e.g. revised for S2-c) rather than upstream's | Lets S2-c be expressed without patching `MC_VEGAS.f90`; file is reviewable in git | Not the upstream artifact; divergence to track |

### S4 — Delivering and defending the patch

| | Option | Mechanism | Pros | Cons |
|---|---|---|---|---|
| **S4-a** | **Tracked rebuild script + repacked tarball** | A script extracts, applies, repacks `helac_package.tar.gz`; `--check` mode | Change is reviewable in git; the 57 MB blob stays untracked; idempotent | The blob itself remains untracked, so anyone rebuilding it from upstream silently loses the fix unless the script is run |
| **S4-b** | **Patch at runtime in `run_helac.sh`** | Apply the edit after unpacking | Deterministic, reviewable, impossible to lose | Introduces Fortran source editing at runtime, which the repo has never done; fragile against upstream text drift; adds work to every job |
| **S4-c** | **Ship a prebuilt HELAC runtime** | Include a compiled `HELAC-Onia-2.7.6/ho_cluster` + built addon | `inspect_helac_package` already understands a prebuilt runtime; removes per-job rebuild | Larger blob; binary provenance is harder to review than a diff; ABI coupling to the worker image |

### S5 — Detection and regression prevention

| | Option | Mechanism | Pros | Cons |
|---|---|---|---|---|
| **S5-a** | **Extend `inspect_helac_package`** | Assert the packaged addon source contains the include | Fails at `validate` time instead of silently producing a one-sample pool | Presence check only — proves the text is there, not that seeding works |
| **S5-b** | **Add a two-seed difference gate** | Generate two seeds, compare **uncompressed** payloads | Directly tests the property that matters | Costs a pilot; must use uncompressed hashes (I6) |
| **S5-c** | **Add an observable-correlation study** | Repeated adjacent seed pairs compared with widely separated controls; measure yield/integral and fixed kinematic-bin covariance as well as exact shared events | Directly tests whether S2-a is operationally adequate for CrystalBall | Requires multiple pairs, predeclared statistics, and a material-correlation threshold |
| **S5-d** | **Assert the addon logs its seed** | Require `Random number seed <N>` in `wrapper.stdout` | Cheap; catches "the INCLUDE never ran" | Still passes under S2-a; a log line is not a physics property |

### S6 — Pool integrity

| | Option | Mechanism | Pros | Cons |
|---|---|---|---|---|
| **S6-a** | **Treat `SPS-Jpsi` as not-homogeneous until proven otherwise** | Record the caveat; consume it only with provenance checks | No destructive action; honest about `[U]` | Does not fix the pool; blocks some downstream use |
| **S6-b** | **Re-derive provenance** | Reproduce the production config of 2026-06-09 and test whether a file reproduces byte-for-byte | Would settle `[U]` definitively | Requires reconstructing an exact configuration from `1c4d165`; effortful, may not converge |
| **S6-c** | **Quarantine / re-generate the affected seeds** | Separate the 2026-09-16 overwrites (at minimum 100/101) from the rest | Removes the known-defective files from the canonical pool | Irreversible; the 2026-09-16 files were themselves a repair of earlier overwrites; interacts with inventory checksums |
| **S6-d** | **Restrict the regeneration target** | Ensure no new write lands in `MC_Production_v3` | Prevents recurrence | Orthogonal to repairing what is already there |

---

## 3. How the options interact

Not every combination is coherent:

- **S3-a requires a matching S1.** The `INCLUDE` opens `input_dir//'seed.input'`;
  with S1-a the file must be written into the addon's `input/`, with S1-b it is
  supplied by `create_subdir.sh`. S3-a with neither is a runtime crash at
  `status='OLD'`.
- **S2-a is unvalidated under every S1 and S3.** It can pass S5-b and S5-d
  while retaining strongly overlapping scalar streams. The adaptive
  CrystalBall calculation may or may not remove material observable
  correlation, so every combination containing S2-a needs S5-c.
- **S2-c interacts with S3.** Expressing it as a repo-owned `.inc` (S3-b) keeps
  `MC_VEGAS.f90` untouched; expressing it as an edit to `RANDA_init.inc` inside a
  repack (S4-a) is also possible but then diverges the upstream file.
- **S2-b's achievable stride depends on I4's answer** and therefore on Q3.
- **S1-c is a sequence, not a state**: it composes S1-a then S1-b, and the S2/S3
  choices must hold across both.
- **S4 is orthogonal to S1-S3** but determines whether the fix survives a
  package rebuild — with S4-a alone the fix is lost by anyone who follows
  `common/packages/README.md` literally.

---

## 4. Spikes required before S1-b can be scoped

These are open questions, not constraints:

1. The `run_config.ho` file-form directive for selecting a registered addon
   (`generate addon 2` in the interactive interface, `helaconia_interface.py:225`).
2. Under the unified chain the addon reads `$subdir/input/user.inp`, copied from
   `$(HODIR)/input/user.inp` — the **main** `user.inp`, not the addon's. The
   CrystalBall keys the addon reads (`minpt1c maxpt1c miny1c maxy1c absoluterap`,
   set today at `run_helac.sh:978-982`) would have to be merged there, or
   `create_subdir.sh` taught to copy the addon's cards. **This is the largest
   unknown.**
3. How `state.inp` / `crystalball.inp` selection is expressed through the
   unified chain.
4. Whether `create_subdir.sh`'s `read local x` prompts can block a batch job
   with no stdin.
5. Whether `link_addon()`'s `misc.compile` helper works in the worker container
   (CentOS 7 / LCG_88b).
6. Output layout and harvesting: LHE would land under the run directory rather
   than `addon/.../output/`, affecting `run_helac.sh:1380-1386` and downstream
   shuffle-split / stage-out.

---

## 5. Open questions requiring measurement or a decision

| | Question | Why it matters | How it would be settled |
|---|---|---|---|
| **Q1** | Does a candidate fix produce distinct, operationally decorrelated physics samples rather than only different bytes? | S2-a can pass byte-level checks while its scalar streams overlap | Use repeated adjacent pairs and wide-pair controls; compare uncompressed payloads, exact shared events, integral/yield covariance, and fixed kinematic bins |
| **Q2** | Does the seed source survive a package rebuild? | S4-a loses the fix if `README.md` is followed blindly | Rebuild from the documented procedure and re-run S5-a |
| **Q3** | What is the actual per-run RANDA draw consumption, i.e. the real stride ceiling (I4)? | Bounds the achievable stride (S2-b) and possibly the whole pool strategy | Instrument one production-config run, or find the burn value at which two runs decorrelate |
| **Q4** | Is the 2000-file pool attainable at all with this RNG? | Determines whether S2-d is in scope and whether the budget is expressible | Follows from Q3 with the period `2^31-2` |
| **Q5** | What is the provenance of the 2026-06-09 pool bulk? | The pool is not known to be homogeneous | S6-b, or accept S6-a |
| **Q6** | Which option family is tried first? | Determines the first package and experiment | Decided 2026-09-19: stock include with the current direct-runner bridge first; `ho_cluster` migration later |

---

## 6. Findings that bound any solution

- The defect is **~116 days old and was never not present**; there is no earlier
  working behaviour to restore. `[M]`
- The fix cannot be a pure shell change: the addon binary contains no seed
  handling at all, so **HELAC/addon source must change** in every option above.
  This is the one thing all options share.
- No option should write to `MC_Production_v3` or reuse its filenames. `[M]`
- Sizing CrystalBall production from a single measured count is not sound under
  any option; that conclusion is independent of which option is chosen.

---

## Appendix — primary evidence

Measurements `[M]`:

| What | Result |
|---|---|
| Seed sweep, 5 legs, `nmc=2000000 unwevt=2000` | 12,926 events, 8,867,505 bytes, one uncompressed SHA-256 `b59fd0fa…` across seeds 100/101/500/5000/100-repeat; 5 distinct `.gz` hashes |
| v3 pool seeds 100, 101 (2026-09-16) | 5,316 events each, uncompressed SHA-256 `6b405bd4…` for both; `.gz` hashes differ |
| v3 pool 2026-06-09 batch | 1998 files, seeds 102–2099, 1998 distinct sizes, written 12:16:13–12:45:45 (0.89 s/file) |
| v3 pool same-config scatter | seeds 1628–1662: identical `<init>` config; integral 3944.9–4009.8; error 10.38–14.78 |

Code anchors: `run_helac.sh:854, 868-884, 947, 958, 1371-1379, 1388, 1392, 1414,
1445`; `MC_VEGAS.f90:44, 63-64`; `Helac_Global.f90:224`; `create_subdir.inc:143`;
`create_subdir.sh`; `helaconia_run_interface.py:100, 2156, 2586, 2595`;
`addon/addon_process.dat`; `pp_psiY_SPS.f90:297-307`; `pp_NOnia_MPS.f90:34`;
`pp_psiX_cb.f90:35-38`; `dag_generator.py:2953`.
