# mcprod (production-core refactor) pilot — findings

Pilot date: **2026-09-18**. Branch `refactor/production-core`, worktree
`.worktrees/production-core`, core revision `6c88a9e`.

This documents defects and operational hazards observed while running the first
**end-to-end pilot of the refactor** (`mcprod` / `src/full_mc/`), covering the
CERN half (plan → mix → miniaod → archive) and the IHEP half (merge → ntuple →
selection → efficiency).

Scope of the pilot: `JJP_DPS1`, pool `pool_jpsi_CSCO_g` used twice,
detector profile `run3_2022_legacy_premix_v1`, 20 planned source events,
5 events/block, 5 target mixed events, 5/5 source budgets.

## Run identity

| | |
|---|---|
| CERN schedd / root cluster | `bigbird17.cern.ch` / `9599533` (DAG status 0) |
| CERN run dir | `/afs/cern.ch/user/c/chiw/condor/mcprod-pilot/runs/jjp_dps1_pilot/cern` |
| IHEP HepJob cluster | `81400157` on `ams017.ihep.ac.cn` |
| IHEP runtime | `/scratchfs2/cms/wangchi/mcprod-runtimes/production-v2/ihep` |
| IHEP batch dir | `/scratchfs2/cms/wangchi/mcprod-pilot/runs/jjp_dps1_pilot/ihep` |
| Runtime revision | `6c88a9e`, `source_dirty: false`, analyzer `89a3782` |

CERN half result: topology `plan → mix ×2 → miniaod ×2 → archive`, 4 blocks,
**153,293 counted input events matched exactly**, `.complete` written.

IHEP half result: **merge succeeded (9 events), ntuple FAILED.** See F1.
`selected/` and `efficiency/` were never reached.

---

## Summary

| # | Severity | Area | Finding |
|---|----------|------|---------|
| **F1** | **Blocking** | `workers/chain.py` | `native/common/packages` is never staged → **ntuple can never run**, on any site |
| **F2** | **Dangerous** | `backends/hepjob.py` | `batch status` reports a *live* job as `failed`; `batch resume` would **double-submit** |
| **F3** | **Blocking (CERN)** | `backends/condor.py` | `_schedd()` reads `SCHEDD_NAME`, not `SCHEDD_HOST` → no default schedd on lxplus |
| F4 | High (ops) | hepjob wrapper | Worker diagnostics live in node-local `/tmp`; **unrecoverable after the job ends** |
| F5 | High (ops) | HepJob semantics | Finished jobs leave the queue entirely; "absent" ≠ success |
| F6 | Medium | `myschedd` | `bump` can re-select the schedd between `plan` and `submit` |
| F7 | Medium | `runtime/packages.py` | Dirty-source guard trips on **any** untracked file, including docs |
| F8 | Medium | CLI | `runtime prepare` / `run plan` are one-shot per directory |
| F9 | Medium | legacy only | Worktree `common/packages/*.tar.gz` are symlinks; legacy `build_bundle` does not dereference |
| F10 | Low | `handoff.py` | `handoff transfer` has no default destination |
| F11 | Low (ops) | IHEP env | No proxy on IHEP; CERN AFS unreadable from IHEP |
| F12 | Low | docs | Plan named `lxlogin005`, which does not resolve |
| F13 | Low (physics) | planning | 9 vs 10 mixed events — budget/target leaves no headroom |
| F14 | Cosmetic | container | `WARN No valid VOMS proxy` inside `run_chain.sh` |
| F15 | Low (forensics) | both | Legacy and refactor share the `JOB000000_BLOCK000000` ID namespace |

---

## F1 — BLOCKING: ntuple stage cannot run; `common/packages` is never staged

**Symptom.** The IHEP batch job failed at the ntuple stage:

```
ERROR: cmssw15-ntuple-el9 command failed (1);
  diagnostics: /tmp/chiw/mcprod-ihep.z1oJv7/worker/ntuple/commands.log
```

with, inside `commands.log`:

```
-- Step 7: Ntuple (JJP) --
[ERROR] Packages directory missing:
  /scratchfs2/.../repro/worker/ntuple/native/common/packages
```

**Causal chain.**

1. `src/full_mc/workers/chain.py:26` builds the "native tree" and links the
   common directory as a **symlink into the runtime core**:

   ```python
   (root / "common").symlink_to(core_path(task, "common"), target_is_directory=True)
   ```

2. The runtime's `controller/core/common/` contains
   `campaigns, cmssw_configs, compression_helpers.sh, compression_util.py,
   node_config_defaults.json, octet_pdg.py, profiles` — and **no `packages/`**.

3. `processing/run_chain.sh:57` sets `PACKAGES_DIR="${COMMON_DIR}/packages"`
   and `run_chain.sh:1257-1258` hard-fails when it is absent.

4. The runtime *does* ship the payloads, but at the **runtime root**
   (`cmssw15_tpsonia2mumu_runtime.tar.gz`, `analyzer.tar.gz`), addressed by
   profile name via `runtime.json`. Nothing bridges those to `common/packages/`.

**Verified** on the IHEP runtime: `controller/core/common/` has no `packages/`,
and the symlink resolves to exactly that directory.

**Second naming mismatch.** `run_chain.sh:1255` wants
`tpsonia2mumu_code.tar.gz`, and `run_chain.sh:1301` wants
`cmssw15_tpsonia2mumu_runtime.tar.gz`. In `src/full_mc/runtime/packages.py`,
`CMSSW15_SOURCE_PACKAGE = "tpsonia2mumu_code.tar.gz"` (line 17) is produced
**only** in the submodule-fallback branch (line 205); the prebuilt path stages
the runtime as `cmssw15_tpsonia2mumu_runtime.tar.gz` at the runtime root
(line 199). With the prebuilt tarball — our case — `tpsonia2mumu_code.tar.gz`
does not exist anywhere.

**Why the CERN half did not catch it.** CERN site profiles are only
`helac-el7, utilities-el9, pythia-el8, cmssw12-sim-el8`. Ntuple runs exclusively
on IHEP, so this path had never been executed.

**Consequence.** The acceptance criterion "one merged MiniAOD + one ntuple" is
**not met**. Merge is fine; everything downstream of ntuple (selection,
efficiency) is unreachable.

**Fix direction.** `native_tree()` must materialize a real
`native/common/packages/` — not a symlink to the core's `common` — containing
the profile-resolved bundles under the names `run_chain.sh` expects.

**Resolution status (2026-09-24).** The clean `e1fdf48` runtime bootstrap now
creates `core/common/packages/` and places the CMSSW15 profile bundle there
under its descriptor basename. A runtime prepared from that revision passes
the package/archive validation. The older `production-v2` runtime recorded
above is unchanged and should not be reused for the cutover pilot.

**Reproduce (no scheduler submission, merge checkpoint is resumed):**

```bash
RT=/scratchfs2/cms/wangchi/mcprod-runtimes/production-v2/ihep
WS=/scratchfs2/cms/wangchi/mcprod-pilot/runs/jjp_dps1_pilot/ihep/batch
export X509_USER_PROXY=/scratchfs2/cms/wangchi/mcprod-pilot/proxy/x509up
python3 $RT/controller/core/mcprod batch worker \
  --task $WS/tasks/0000.json \
  --work-dir /scratchfs2/cms/wangchi/mcprod-pilot/repro/worker
```

`src/full_mc/application/resume.py:79-83` resumes a stage whose checkpoint
identity matches, so merge is skipped and the failure reproduces at ntuple.

---

## F2 — DANGEROUS: `batch status` reports a live job as `failed`

`mcprod batch status` returned, for HepJob `81400157`:

```
state=failed  scheduler_state=failed  recovery_eligible=true
```

while the job was in fact **idle and alive**:

```
81400157.0  wangchi  ...  I  0  0.0  0000.sh
```

**Causal chain.**

1. `src/full_mc/backends/hepjob.py:256` builds `["hep_q", str(job_id)]`.
   This `hep_q` requires `-i <id>`; a bare positional argument makes argparse
   reject it.
2. The rejection exits **rc=0** and writes the usage text to **stderr**, so the
   `returncode != 0` branch in `_hep_q` (line 250) does not fire.
3. `_hep_q` concatenates stdout+stderr, finds non-empty text, and returns it as
   though it were a valid scheduler response.
4. `_hep_state` (line 274) regex-matches against `_FAILED_STATES`
   (line 30: `{"failed", "error", "removed", "aborted", "killed"}`) and matches
   `\berror\b` inside `hep_q.py: error: unrecognized arguments` → `"failed"`.
5. `status_batch` sets `state="failed"`, `recovery_eligible=True`.

**Captured verbatim** in `batch_manifest.json`:

```json
"scheduler_query": ["hep_q", "81400157"],
"scheduler_state": "failed",
"state": "failed",
"recovery_eligible": true,
"scheduler_output": "usage: hep_q.py [-h] [-u [USER]] ... [-slurm]\nhep_q.py: error: unrecognized arguments: 81400157",
```

**Why this is dangerous.** `resume_batch` resubmits anything
`failed && recovery_eligible` — so a routine `batch status` followed by
`batch resume` would **resubmit a live job**, duplicating the allocation. This
is the exact hazard the module's comments claim to guard against.

**Mitigation applied.** `batch resume` was **not** run; monitoring used
`hep_q -i <id>` directly.

**Resolution status (2026-09-24).** The current development worktree passes
`-i`, keeps stderr diagnostics out of scheduler-state parsing, and treats
`usage:` / `error:` output as `unknown`. Regression tests cover diagnostics on
either stream and verify the job remains submitted, not recovery-eligible.
This change is not in the clean `e1fdf48` runtime deployed for this pilot. Until
it is included in a new clean release, do not use that runtime's `batch status`
or `batch resume`; query the exact ID with `hep_q -i <id>` and treat empty or
unparseable output as unknown.

---

## F3 — BLOCKING (CERN): `_schedd()` reads the wrong condor variable

`src/full_mc/backends/condor.py:100` runs `condor_config_val SCHEDD_NAME`, which
is undefined on lxplus (rc=1), while CERN's `myschedd` sets `SCHEDD_HOST`
(e.g. `bigbird17.cern.ch`). `mcprod run submit` therefore cannot resolve a
default schedd here; submission is impossible without pinning
`"condor": {"schedd": "bigbirdNN.cern.ch"}` in the run spec.

---

## F4 — High (ops): worker diagnostics are unrecoverable after the job

The HepJob wrapper creates scratch under **node-local** tmp:

```bash
mkdir -p /tmp/chiw
job_scratch=$(mktemp -d /tmp/chiw/mcprod-ihep.XXXXXX)
```

and the worker work-dir is `"$job_scratch/worker"`. When the job ends, that
directory dies with the node. The error message therefore points at a path
nobody can read — the `commands.log` named in the failure did not exist on any
reachable host. `0000.err` retained only 115 bytes (one line); `0000.out` was
empty.

The failure was only diagnosable by **re-running the task manually with
`--work-dir` on persistent scratchfs2**. Fix direction: put the work-dir (or at
minimum `commands.log` and per-stage stderr) under the batch dir on shared
storage, or stage diagnostics out on failure.

---

## F5 — High (ops): a finished HepJob leaves the queue entirely

`hep_q -i 81400157` later showed `0 jobs; 0 completed, 0 removed, 0 idle, ...`.
HepJob removes finished jobs, so **absence from the queue is ambiguous**: it can
mean success or death. Any watcher must combine queue state with checkpoint /
product inspection.

The pilot watcher (`/tmp/chiw/ihep_watch_81400157.sh`) broke only on a `C` /
removed / held row; a vanishing job matched none of them, so it looped until its
timeout. Worth fixing in the skill's monitoring guidance.

---

## F6 — `myschedd bump` can re-select the schedd

`myschedd bump` returned `bigbird12` at plan time and `bigbird17` at submit
time. Since the schedd is frozen into the generated run spec, the plan had to be
redone. Bump immediately before submitting and re-pin to match.

---

## F7 — Dirty-source guard trips on any untracked file

`prepare_runtime` snapshots `git status --porcelain --untracked-files=normal`;
**any** untracked file sets `source_dirty`, which lands in `run.json` as
`runtime_source_dirty`, and `condor.py:_assert_submittable` then raises
`refusing scheduler submission from a dirty runtime source`. An untracked
*planning document* blocked the first submission attempt. Workaround: stash it,
rebuild (destination must not exist), restore.

**This document is itself an untracked file in this worktree** and will trip the
same guard until it is committed or stashed.

---

## F8 — `prepare_runtime` and `run plan` are one-shot per directory

Redoing either requires removing the destination / emptying the run dir first.
Combined with F6, a changed schedd means `rm -rf` and a full re-plan.

---

## F9 — Worktree package symlinks (legacy only; refactor verified immune)

In this worktree `common/packages/*.tar.gz` are symlinks to the main checkout,
and the legacy `dag_generator.py:build_bundle` does not pass `dereference=True`,
so archives embed **dangling absolute symlinks**. This produced a broken ntuple
runtime bundle on workers and cost three clusters (`15949978`, `15950115`,
`15950288`) before being removed.

**The refactor is immune** — `runtime/packages.py` rejects symlinks in the
source payload and copies with `shutil.copy2` (dereferences). Verified directly:
the prepared IHEP runtime contains **0 symlinks**, and the CMSSW15 payload is
byte-identical to the source (10,595,396 B, `gzip -t` OK). This hypothesis for
the ntuple failure was tested and **excluded**.

---

## F10 — `handoff transfer` has no default destination

`transfer_handoff` requires `site_config.destination`; there is no fallback.
A site config had to be written by hand
(`/afs/cern.ch/user/c/chiw/condor/mcprod-pilot/sites/ihep.json`).

---

## F11 — IHEP has no usable proxy

IHEP had no proxy, `~/.globus/` was empty, and CERN AFS is not readable from
IHEP. The CERN proxy was copied to
`/scratchfs2/cms/wangchi/mcprod-pilot/proxy/x509up` (mode 600) and used by both
the batch wrapper and interactive queries.

---

## F12 — `lxlogin005` does not resolve

The plan named `lxlogin005`; it did not resolve. Work went through the `ssh
ihep` alias (`lxlogin.ihep.ac.cn`, ControlMaster), landing on `lxlogin003`.

---

## F13 — 9 vs 10 mixed events

Source budgets `5` / target `5` leave no headroom: source B mixed 4 events, so
2 MiniAOD components carried 5 + 4 = **9** events against a merge target of 10.
Independently confirmed:

```
edmFileUtil -j .../merged/JJP_DPS1_run3_2022_legacy_premix_v1_MERGE000000.root
{"runs":1,"lumis":2,"events":9,"bytes":3325783}
```

Not a gate — `workers/merge.py` validates `merged == sum(components)` (9), not
`merge_events` (10) — but it makes the "10-event merge" acceptance target
unreachable. Same pattern as the legacy pilot.

---

## F14 — VOMS warning inside the container

`run_chain.sh` logs `WARN No valid VOMS proxy detected. Set X509_USER_PROXY ...`
when run under the container. Harmless here: the proxy **was** exported and
staging to IHEP succeeded, so the warning reflects the container environment not
inheriting the variable rather than a missing credential. Still noise worth
silencing or explaining.

---

## F15 — Legacy and refactor share an output ID namespace

Both systems emit `JOB000000_BLOCK000000`. During the pilot this made output
forensics genuinely ambiguous: two different runs wrote the same job IDs, and
which artifact belonged to which system was only resolvable by path and mtime.
Worth a distinct prefix in one of the two systems.

---

## Cleared (tested, not a problem)

- **Runtime symlink trap** — 0 symlinks in the prepared IHEP runtime.
- **Payload integrity** — CMSSW15 tarball identical on both sides, `gzip -t` OK.
- **Inventory count guard** — `plan` matched 153,293 counted input events exactly.
- **Merge** — succeeded; `merge.json` status ok; artifact on EOS.
- **Block identity** — merges are not in the CERN DAG, as designed.

## Outstanding

- Ntuple / selection / efficiency **unvalidated** — blocked by F1.
- Selection and efficiency workers (`analyzer-el9`) never exercised end-to-end.
