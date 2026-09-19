# CrystalBall addon RNG-seed fix, acceptance, and package plan

Date: 2026-09-19
Status: **revised implementation and acceptance plan; no package or production
output has been changed.**
Scope: JJP CrystalBall pools (`pool_jpsi_CSCO_g` and
`pool_upsilon_CSCO_g`).

This revision takes the HELAC-Onia author's recommendation as the first
candidate: add the stock `RANDA_init.inc` to the CrystalBall addon, ensure
`seed.input` reaches it, force one clean addon-2 rebuild, and test the resulting
physics behavior. Direct `DSEED` assignment is now a fallback, not the default.

Companions:

- `docs/crystalball_seed_issues_and_options_2026-09-18.md` records the original
  evidence and option space;
- `docs/lhe_output_location_and_crystalball_nmc_2026-09-18.md` places this
  work in the LHE recovery and production-resumption plan; and
- `.worktrees/production-core/docs/mcprod_ihep_pilot_findings_2026-09-18.md`
  supplies the package-transfer and end-to-end pilot constraints.

---

## 1. Decision

Use this sequence:

1. Add the upstream `RANDA_init.inc` call to `pp_psiX_CrystalBall`.
2. Supply `input/seed.input` through both supported invocation paths:
   `ho_cluster:set_seed()` for addon 2, and a narrow bridge in the current
   direct wrapper.
3. Remove the old CrystalBall executable from a clean staging tree and run a
   small `generate addon 2` job so the patched source must compile.
4. Run the acceptance experiments in section 5 against that candidate build.
5. If the upstream recipe is operationally acceptable, publish the patched
   source and rebuilt executable as one immutable package and copy the same
   regular file into both workspaces before refactoring.
6. Escalate to direct state assignment or RNG replacement only if the measured
   correlations or generator-period limits reject the upstream recipe.

This separates four claims that must not be conflated:

- **plumbing:** the requested seed reaches the addon;
- **sensitivity:** different seeds produce different output;
- **reproducibility:** the same seed reproduces the same output; and
- **decorrelation:** different runs are sufficiently uncorrelated for the
  intended production use.

The first three can be tested directly. The stock include does not
mathematically guarantee the fourth; the acceptance experiments decide whether
its practical behavior is adequate for CrystalBall.

---

## 2. What the author's recipe does in CrystalBall

### 2.1 Current defect

The current CrystalBall branch directly runs:

```bash
cd addon/pp_psiX_CrystalBall
./bin/HO_pp_psiX_CrystalBall
```

The addon contains no seed initialization, while `MC_VEGAS.f90` starts from:

```fortran
REAL(KIND(1d0)),PUBLIC::DSEED=1234567d0
```

The workflow seed currently changes filenames and later shuffle seeds, but not
the CrystalBall integration or unweighting stream.

### 2.2 Upstream initialization

The stock `src/RANDA_init.inc` reads `seed.input` and advances the fixed
Park-Miller stream:

```fortran
INTEGER::iseed,ililil
REAL(KIND(1d0)),DIMENSION(1)::ranrrr
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

If the underlying scalar sequence is `u1,u2,...`, seed 100 begins at `u101`
and seed 101 begins at `u102`. The scalar streams therefore overlap almost
completely.

That fact does not prove that the final events are near-duplicates. In the
usual unweighted CrystalBall configuration, `varnum` is at least four and
VEGAS obtains a vector for each integration point:

```fortran
CALL RANDA(NDIM,RAND)
```

Adjacent seeds initially group the values differently:

```text
seed 100: (u101, u102, u103, u104)
seed 101: (u102, u103, u104, u105)
```

Those components feed different coordinates, adaptive VEGAS grids, cuts,
weights, and finally a conditional accept/reject draw:

```fortran
CALL RANDA(1,ranr)
IF(umax*ranr(1).LT.w1)llwri=.TRUE.
```

The nonlinear and adaptive calculation can therefore decorrelate observable
outputs substantially. It cannot create a formal independence guarantee from
overlapping inputs. The earlier plan's statement that adjacent seeds would
necessarily produce near-duplicate physics samples was too strong; the
author's practical account is plausible and must be tested.

### 2.3 Source insertion

Insert the stock include after the final declaration and before the first
executable statement in `calc_pp_psiX_cb`:

```fortran
    LOGICAL::lexist
    SAVE lunwei,lhewgt,icase
    INTEGER::beam1_pdf,beam2_pdf
    INCLUDE "../../../src/RANDA_init.inc"
    WRITE(*,*)'                     THE BEGINNING OF HELAC-Onia'
```

Anchor the patch on the unique `INTEGER::beam1_pdf,beam2_pdf` declaration, not
on a line number. The include contains declarations followed by executable
statements, so placing it before the addon's final declaration is a compile
error.

### 2.4 Seed-file bridge for the current direct runner

Under `ho_cluster`, `set seed = N` calls `set_seed()`, and
`create_subdir.sh` copies `seed.input` into the addon run directory. The
current bare-addon branch bypasses that machinery. The include alone would
therefore try to open a missing file.

Until the invocation is refactored, add this to
`prepare_crystalball_addon_input()`:

```bash
printf '%s\n' "${MY_SEED}" > "${addon_dir}/input/seed.input"
msg_info "CrystalBall RANDA skip seed: ${MY_SEED}"
```

The log deliberately calls this a **skip seed**. With the stock include, it is
the number of initial `RANDA` draws discarded, not a direct assignment to
`DSEED`.

Remove the bridge when the production path moves to `ho_cluster`, which
already owns creation and distribution of `seed.input`.

### 2.5 Reproducible source delivery

Do not make the nested tarball the only record of the change. Add a tracked,
idempotent patch/repack script that:

- extracts `common/packages/helac_package.tar.gz` and its nested
  `HELAC-Onia-2.7.6.tar.gz` into a clean staging directory;
- inserts the include at the unique anchor;
- fails if the anchor count is not exactly one;
- has a `--check` mode that verifies the packaged CrystalBall source; and
- records the unmodified upstream `RANDA_init.inc` checksum in the package
  manifest.

---

## 3. Force one clean addon-2 rebuild

### 3.1 Remove the stale executable in staging

`link_addon()` returns immediately when
`bin/HO_pp_psiX_CrystalBall` already exists. Remove both executable paths in
the disposable staging tree:

```bash
helac_dir=/path/to/staging/HELAC-Onia-2.7.6
rm -f "${helac_dir}/bin/HO_pp_psiX_CrystalBall"
rm -f "${helac_dir}/addon/pp_psiX_CrystalBall/bin/HO_pp_psiX_CrystalBall"
```

Do not delete an installed or published artifact in place. The addon makefile
declares `all : clean $(exe)`, so forcing `link_addon()` past its executable
check also removes stale addon objects and modules.

### 3.2 Addon-2 compile smoke

Run in the same `helac-el7` build environment used by workers:

```text
set seed = 31001
set preunw = 1000
set unwevt = 20
set nmc = 10000
set nopt = 1000
set nopt_step = 1000
set noptlim = 10000
set gener = 0
set minptconia = 0d0
set maxrapconia = 2.4
generate addon 2
launch
exit
```

Before launch, stage the intended J/psi `state.inp`, `crystalball.inp`, and
CrystalBall-specific `user.inp` values into the inputs copied by the unified
driver. Use a fresh run directory so `create_subdir.sh` cannot prompt about
overwriting old contents.

```bash
cd "${helac_dir}"
./ho_cluster < addon2_rebuild.ho > addon2_rebuild.log 2>&1
test -x bin/HO_pp_psiX_CrystalBall
grep -F 'Random number seed' addon2_rebuild.log
```

The smoke passes only when the compile log shows the patched
`pp_psiX_cb.f90` being compiled and stdout reports `Random number seed
31001`. An executable mtime alone is not rebuild evidence.

---

## 4. Candidate package and immutable publication

The experiments should run from a versioned candidate package, not from an
unrecorded build directory. It must contain:

- the patched CrystalBall source;
- the unchanged upstream `src/RANDA_init.inc`;
- the rebuilt `bin/HO_pp_psiX_CrystalBall`;
- compiler and `helac-el7` build identity;
- source, binary, and candidate-package checksums; and
- the addon-2 smoke control file and build log.

The addon makefile creates an absolute addon-local symlink from `$(exe)`.
Normalize it before packaging:

```bash
cd "${helac_dir}/addon/pp_psiX_CrystalBall/bin"
ln -sfn ../../../bin/HO_pp_psiX_CrystalBall HO_pp_psiX_CrystalBall
```

Archive validation must reject dangling or absolute symlinks.

After the experiments accept the candidate, publish it under an immutable
release name and copy its bytes into each workspace under the runtime filename:

```bash
install -m 0644 /path/to/releases/helac_package_cbseed_v1.tar.gz \
  /path/to/workspace/common/packages/helac_package.tar.gz
test -f /path/to/workspace/common/packages/helac_package.tar.gz
test ! -L /path/to/workspace/common/packages/helac_package.tar.gz
sha256sum /path/to/workspace/common/packages/helac_package.tar.gz
```

Use regular files in both workspaces. The legacy bundler can archive package
symlinks as dangling absolute links, while the production-core runtime builder
rejects source symlinks.

---

## 5. Next step: acceptance experiments

All legs use the same candidate package, cards, cuts, compiler/build, and a
unique v4_5 output directory. Compare decompressed LHE payloads; gzip headers
make compressed-file hashes unsuitable.

### E0 — build and plumbing

1. Run the small addon-2 smoke with seed 31001.
2. Confirm the old executable was absent before launch.
3. Confirm the patched source compiled and the new executable was produced.
4. Confirm stdout prints the requested `Random number seed`.
5. Run the current direct wrapper once and confirm its bridge writes the same
   seed into the exact addon-local `input/seed.input`.

Failure at E0 is a plumbing/build failure, not a physics result.

### E1 — deterministic reproducibility

Run the same seed twice with identical inputs at a modest representative
`nmc`, initially 500,000 to match the existing 5,316-event observation.

Require:

- identical decompressed LHE checksum and event count;
- identical integration result/error and effective schedule; and
- identical relevant stdout apart from paths and timestamps.

This proves reproducibility, not independence.

### E2 — seed sensitivity screen

Use at least these seed relationships:

```text
adjacent:       100, 101
moderate gap:   500, 5000
large allowed:  100, 90000
```

The current wrapper accepts seeds 11..99999. Record the skip value exactly.
Require different seeds to produce different decompressed payloads. Also record:

- exact event count;
- integration estimate and printed uncertainty;
- final `NCALL` and wall/CPU time;
- canonicalized full-event duplicate fraction;
- J/psi pT and rapidity histograms; and
- unweighting acceptance/yield.

Different hashes are necessary but are not evidence of independence.

### E3 — observable-correlation study

The author's explanation is accepted operationally only if adjacent skip seeds
show no material correlation beyond the predeclared tolerance for the intended
use. One adjacent pair cannot establish that.

Use repeated adjacent pairs `(N,N+1)` across the allowed seed range and a
matched control set of widely separated pairs. Before launching, freeze:

- the number of pairs;
- primary statistics: integral, accepted count, and fixed pT/rapidity bins;
- the correlation or covariance estimator;
- the confidence interval; and
- the largest correlation considered operationally harmful.

Compare adjacent-pair correlations with the wide-pair control. The acceptance
statement must be “no material observable correlation detected at the stated
sensitivity,” not “the streams are mathematically independent.”

Use a reduced but representative `nmc` for the multi-pair screen if necessary,
then confirm any conclusion with selected pairs at the intended production
`nmc`. Do not extrapolate a low-`nmc` decorrelation result without that
confirmation.

### E4 — package and regression

- Rebuild from the documented source procedure and reproduce E0/E1.
- Confirm the package contains the include and the rebuilt executable.
- Confirm the addon-local link is relative and resolves after unpacking.
- Confirm both workspaces receive a regular file with the accepted checksum.
- Confirm no output path reaches `MC_Production_v3`.

---

## 6. Decision after the experiments

### Accept the upstream recipe when

- E0 and E1 pass;
- different seeds produce different event streams;
- integration estimates and physics distributions remain statistically
  compatible with the same CrystalBall configuration; and
- E3 detects no operationally material adjacent-seed correlation at its stated
  sensitivity.

In that case, retain the stock include and document that the result is an
empirically accepted decorrelation scheme, not a formal independent-substream
construction.

### Escalate when it fails

If the seed changes output but E3 finds material correlation, test direct
`DSEED` initialization as a second candidate and repeat E1-E3. Direct state
assignment avoids the known one-draw skip relationship for adjacent workflow
seeds, but it still selects positions on the same Park-Miller cycle and must
not be accepted without the same measurements.

If the `2^31-2` period or measured draw consumption cannot support the
intended number and length of production runs, use fewer longer runs or replace
`RANDA` with a generator that provides explicit, validated substreams.

---

## 7. Production-core integration constraints

The fixed HELAC package does not clear the separate production-core pilot
blockers:

- its IHEP ntuple native tree still needs a runtime-manifest-selected package
  view under the path expected by `run_chain.sh`;
- `hep_q -i <id>` parsing, absent-job reconciliation, and recovery eligibility
  must be corrected before `batch resume`;
- the exact schedd must be pinned for planning, submission, monitoring, and
  recovery;
- worker failure diagnostics must survive node-local scratch cleanup; and
- legacy and refactor outputs need distinct run/path namespaces.

Prepare the immutable production-core runtime only after the accepted HELAC
package is copied into a clean source workspace.

---

## 8. Ordered work

1. Add the anchored stock include and the direct-runner `seed.input` bridge.
2. Add package `--check` validation for the source insertion.
3. Remove both old executable paths in a clean staging tree.
4. Run the addon-2 compile smoke and create a versioned candidate package.
5. Run E0-E3 and record the predeclared acceptance criteria and results.
6. Accept the stock include or escalate according to section 6.
7. Publish the accepted immutable package and copy the same regular file into
   both workspaces.
8. Run the fixed-seed `nmc` response curve, followed by the accepted
   multi-seed yield sample used to define `Y_plan`.
9. Refactor invocation/package plumbing only after the accepted behavior
   remains reproducible.
10. Rerun production-core end to end only after its separate blockers are
    fixed.

Nothing in this plan authorizes a scheduler submission, a write to
`MC_Production_v3`, or resumption of the paused production campaign.
