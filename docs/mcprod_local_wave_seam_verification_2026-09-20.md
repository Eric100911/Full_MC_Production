# mcprod local wave seam: implementation record and open verification targets

Date: 2026-09-20
Status: **implemented in the working tree; committed but not merged**
Scope: `production-core` (`mcprod`) wave controller only. Legacy `dag_generator.py` is untouched.

This document exists so an independent session can try to **falsify** the claims
below. Every claim is tagged with how it was established:

- **[PROVEN]** — I ran something and observed the result. Repro command included.
- **[READ]** — established by reading code plus reasoning. Not executed.
- **[ASSUMED]** — I believe it but did not check. Prime falsification target.

If you find an [ASSUMED] that is false, or a [PROVEN] that does not reproduce,
that is the valuable outcome. Section 7 lists the specific things I could not
settle.

**Read `docs/mcprod_local_wave_seam_refactor_plan_2026-09-20.md` first** — it is
the reviewed plan this implements. This document is the *after* picture.

---

## 0. Environment trap you will hit immediately

On AFS, an edit that keeps the file size the same and lands in the same mtime
second does **not** invalidate the `.pyc`. `+ 1` → `+ 2` is exactly such an edit.
Python then loads stale bytecode, and `inspect.getsource()` reads the *file*, so
it prints the new text and lies to you.

```bash
find . -path ./tests/generated -prune -o -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null; true
```

`python3 -B` only stops *writing* `.pyc`; it does not stop *reading* a stale one.
Run the purge before every test invocation, and **between every step** of any
before/after proof. My first such proof in this session was invalid because of
this; see §5.

Tests are run as `PYTHONPATH=src python3 -m unittest tests.<module>` from the
repository root. Three modules (`test_core_input_records`, `test_core_workspace`,
`test_core_simulation_profiles`) fail to import without `PYTHONPATH=src` — that
predates this change.

---

## 1. Why this refactor exists

On 2026-09-19 the `v5_0_pre1` JJP pilot completed its first wave, then aborted:
the DAGMan PRE script failed with status 1 after 79 s. `DAGMAN_VERBOSITY=3` does
not record SCRIPT stderr, so the exception text is unrecoverable — the root cause
was never proven.

What is known: the seam issued **511 container-wrapped `xrdfs stat` calls**
serially from the submit host, one per staged block, and any single non-zero
return aborted the whole DAG. The same call site had *wedged* (status 124) two
days earlier on a different host. Full incident record:
`docs/mcprod_seam_eos_access_plan_2026-09-20.md`.

The plan's conclusion: the seam should read **local records only** and never
contact EOS. That is the whole point of this change.

---

## 2. What was built

| Change | Where |
|---|---|
| Seam performs no remote I/O at all | `src/full_mc/application/production.py` |
| Immutable ledger replaces growing `state.json` fields | `src/full_mc/application/ledger.py` (new), `io.py` |
| Deletion moved from submit host to a worker | `src/full_mc/workers/reclaim.py`, `storage/reclaim.py` (new) |
| Consumer-side size + per-block LHE count checks | `storage/transfer.py`, `workers/chain.py` |
| PRE stdout/stderr captured per step | `planning/graph.py` → `logs/advance_<NNNNNN>.log` |
| Handoff reads committed ledger records | `application/handoff.py` |

State schema v2: `state.json` keeps only `{state_schema, status, next_step,
pending, pool_cursors, job_offsets, merge_buffers, merge_group_counts}`.
Everything that grows with the run moved to `ledger/`. A v1 state is **refused**,
not migrated.

### The headline claim

**[PROVEN]** `production.py` contains no remote machinery:

```bash
grep -n "TransferClient\|artifact_ready\|validated_record\|run_profile\|xrdfs\|xrdcp\|client\." \
  src/full_mc/application/production.py     # -> no matches
```

The only `storage.transfer` import is `is_remote` (`production.py:34`), a pure
`str.startswith("root://")` predicate. **[READ]** therefore `advance_wave()` and
everything it calls perform zero EOS operations.

**[PROVEN]** The acceptance suite asserts the same thing by patching every
`TransferClient` method to raise and driving a whole run to completion:
`tests/test_core_seam_acceptance.py::test_no_transfer_operation_is_reachable_from_a_whole_run`.

---

## 3. Verification tiers

| # | Claim | Tier | How |
|---|---|---|---|
| 3.1 | Seam has no remote calls | PROVEN | grep + patched-client whole-run test |
| 3.2 | Terminal reclaim needs a 2nd trailing wave slot | PROVEN | `tests/test_core_wave_budget.py`, §5 |
| 3.3 | Completion short-circuit fixes re-entry wedging | PROVEN | `tests/test_core_seam_recovery.py`, §5 |
| 3.4 | Handoff fails closed on a v1 state | PROVEN | same module |
| 3.5 | Allocation/job IDs/RNG seeds unchanged by the refactor | READ | `test_50k_blocks_allocate_identically_and_keep_state_bounded` |
| 3.6 | The completing wave is skipped, not failed, by DAGMan | **READ** | §7 item 1 — the biggest open one |
| 3.7 | Reclaim is safe against every ordering | **ASSUMED** | §7 items 3–4 |

---

## 4. Findings from three adversarial reviewers

The workflow ended with three independent reviewers (seam locality, allocation
identity, deletion safety). They reported 8 findings: 0 critical, 4 major,
4 minor. My disposition of each:

### 4.1 Fixed

**Completion is not terminal** (their #1 and #4, major, same defect).
`advance_wave` never consulted `state["status"]`. It decided whether the
terminal reclaim had already run from `state["pending"]` — a value the commit
recording that fold had already cleared. So re-entry after completion
re-derived leftover blocks, prepared a **second** terminal reclaim wave, and
flipped `status` from `complete` back to `running`. The new wave has no
successor to fold it, so the run wedged with `.complete` never written.

Two triggers, both real: a crash in the window between the completion commit and
the sentinel write; and simply running `mcprod internal advance-wave` on a
finished run, which is a documented recovery entry point.

The reviewed plan states the required behaviour verbatim ("the next PRE
invocation sees status == complete, recreates the sentinel, and returns the
normal skip code") — it was simply not implemented.

**Handoff silently destroyed data** (their #2 and #7). `export_handoff` had no
state-schema guard. Pointed at a pre-refactor run it returned zero tasks **and
overwrote the operator's `merge_groups.json` snapshot with an empty index**.
`advance_wave` already fails closed on v1 state; handoff now does too.

### 4.2 Rejected

**"The completing wave has no sub-DAG, so DAGMan fails the node"** (their #6,
major). They built a DAG whose PRE "touches `.complete` and exits 0", ran real
`condor_dagman`, and got `EXITING WITH STATUS 1`. But that is not the generated
script. **[PROVEN]**, driving the declared waves:

```
WAVE_000003  phase=complete  skip=True  -> PRE exit 99  steps/000003.dag exists=False
```

`advance_wave` returns `skip=True` on completion; `cli.py:192` turns that into
exit 99; `advance.sh` propagates it through `exec timeout`; `PRE_SKIP WAVE_000003
99` marks the node **skipped-as-successful**, so DAGMan never opens the absent
file. Their harness tested itself, not the artifact.

**Caveat, and it is a real one:** I verified the exit-code chain and PRE_SKIP
semantics from source. I did **not** run a live DAGMan. PRE_SKIP is load-bearing
for the entire lazy-wave design (every unused slot relies on it), but see §7.1.

### 4.3 Left open — see §6

Their #3, #5, #8. Two underlying issues, both real, both with non-obvious fixes.

---

## 5. Two bugs I found myself, with regression proofs

### 5.1 Wave budget off by one

`plan_run` computed `steps = len(sources) + sum(max_processing_nodes) + 1`.
But a wave's PRE folds wave *i-1* and prepares wave *i*, so even the last real
wave needs a successor to fold it — and the terminal reclaim wave is itself
prepared into a slot needing that successor. Two trailing slots are structural.

When one short, DAGMan runs every wave it declares, **reports success**, and
`.complete` is never written: a silent non-completion with no failing log line.
The trigger is `(n_src − ⌈n_src/spf⌉) + (nodes − ⌈nodes/npw⌉) < 1`; since
`⌈1/x⌉ = 1`, **`n_src == 1 && nodes == 1`** — the pilot/smoke shape — always
triggers it. Large production has slack.

Fixed to `+ 2`. **[PROVEN]** `tests/test_core_wave_budget.py` drives *only the
waves `run.dag` declares* (earlier tests called `advance_wave(run, 3)` past the
declared boundary, which hid the bug):

```
+2 -> OK      +1 -> AssertionError: 'running' != 'complete'      +2 -> OK
```

### 5.2 Both fixes have proven regression tests

Each fix was reverted, the test observed to fail, then restored:

```
revert completion short-circuit -> AssertionError: None is not true      FAILED
revert handoff schema guard     -> AssertionError: RuntimeError not raised  FAILED
```

Every step purged `__pycache__` first (§0).

---

## 6. The two open problems, explained

### 6.1 Reclaim is gated on URL spelling

`_remote_staging_refs` (`production.py:245`) filters the deletable set through
`is_remote`, a `str.startswith("root://")` test. When empty, `_process_phase`
(`production.py:409-415`) emits **no reclaim task and no cursor gate at all**.

Its docstring claims "The run spec always pins `cern_staging` to an EOS URL".
**That is false.** `load_run_spec` (`settings.py:165-171`) does not reject a
local path — it silently rewrites it:

```python
if not value.startswith("root://"):
    storage[key] = _path(value, path.parent)     # becomes an absolute local path
```

`_validate_generated_staging` only checks overlap with the immutable LHE
inventory, which such a path does not have. So the spec is accepted and
reclamation never happens.

**The sharper half is the byte accounting.** `_staging_view`
(`production.py:189-191`) counts a block as resident only if its index is **at or
after** the pool cursor, encoding the invariant "cursor past a block ⇒ that
block has been reclaimed and is gone". `_used_bytes` (`:215`), `_allocation`
(`:207`) and `_check_bytes` (`:219`) all rest on it. If reclamation never runs,
the files stay on disk while the accounting reports them gone: **the guard
designed to prevent staging overflow reads false free space**, `fitting` stays
large, and the run allocates waves until the filesystem itself fills.

The filter was introduced to keep a fixture green: `tests/test_core_production.py:78`
pins `cern_staging` to a local path and never writes a reclaim result. That
fixture's assertion was inverted to accommodate it — it now asserts the staged
block **still exists**, commented "the seam no longer deletes staged products".

Not hypothetical spelling-wise: `cern_staging` has already been misconfigured in
this project (`/eos/home-c/` vs the `/eos/user/` that `eosuser.cern.ch` serves).

Two candidate fixes, both requiring fixture changes:

1. **Drop the scheme filter** — reclaim covers local and remote (the worker's
   `remove_many` already `unlink`s local paths). Matches the plan, and is the
   only option that also repairs the accounting.
2. **Enforce `root://` for `cern_staging` at plan time** — fail fast. Does not
   repair the accounting if local staging is ever legitimately needed.

### 6.2 Archive member size validation is vacuous

The HepMC reference is built at plan time (`production.py:359`):

```python
hepmc = _ref("HEPMC_" + identity, _staging(spec) + "/mixed/" + identity + ".hepmc.gz", "hepmc")
```

`_ref` sets no `size_bytes`, and cannot: the file does not exist yet — MIX
produces it later. Those refs become the archive task's inputs.

At HEAD the seam compared the staged object's real size against the archived
size (`client.size(...) != int(item["size_bytes"])`) — that remote stat was
exactly what the refactor set out to remove, but it was **the only thing covering
these members**. The replacement is guarded by `"size_bytes" in reference`, which
is false for HepMC members, so it is skipped. The consumer-side check added by
this refactor (`transfer.py materialize_input`) is guarded the same way and
skips identically. Hydration does not fill the gap either: `workers/__init__.py:9`
hydrates `{miniaod, merge, ntuple, selection, efficiency}` — **`archive` is not
in the set**.

Net: a short HepMC download is tarred and recorded as contract-verified.

Blast radius is bounded and nastier for it. `create_archive` verifies the tar
against *local* files, so it cannot catch a short download — the local file *is*
the short one. MiniAOD is unaffected (it hydrates from the sidecar). So the
corruption is confined to **the preservation archive**, whose entire purpose is
to be the durable copy after staging is reclaimed. It surfaces only at restore
time.

**The naive fix breaks archive.** Adding `"archive"` to `_HYDRATE_ALL` looks
obvious but `archive_inputs` also contains the retained LHE originals, whose refs
point at canonical pool URLs with no `complete: true` sidecar. `validated_record`
would return `None`, `_hydrate_inputs` would raise `ValueError("input artifact is
not ready")`, and every archive task would fail. The fix must be **selective** —
hydrate only members that actually have a sidecar.

---

## 7. Open verification targets

These are the things I could not settle. Each is stated so it can be falsified.

1. **Does a live DAGMan really skip the completing wave?** (§4.2)
   I claim yes, from the exit-code chain plus `PRE_SKIP`. Build the real
   generated DAG and run `condor_dagman` on it. If DAGMan reports the node as
   `STATUS_ERROR` or aborts, my rejection of reviewer finding #6 is wrong and
   the fix is to write a real (possibly empty) sub-DAG for the completing wave.
   **This needs a Condor submission, which I did not make.**

2. **Is `steps = ... + 2` sufficient in every configuration?**
   My argument is `ceil(x/y) <= x` for positive `x`, so two extra slots always
   cover the terminal reclaim and its fold. Check the edge cases: a run where
   `_source_phase` prepares fewer sources than `source_files_per_step` allows; a
   run that raises `"no additional source fits the available CERN staging
   budget"` mid-way; a campaign whose `max_processing_nodes` is reached in a
   partial wave. `tests/test_core_wave_budget.py` only covers the 1-source,
   1-node shape.

3. **Can the per-wave reclaim delete an original before ARCHIVE consumed it?**
   The DAG orders `ARCHIVE -> RECLAIM`, and the archive worker materializes
   members before writing the tar. But the archive also validates its gates via
   `artifact_ready`, which is remote. I did not construct an interleaving that
   breaks it. **[ASSUMED]**

4. **Can `terminal_reclaim_refs` pass its guard while the original is not
   actually archived?** Its rule is "an unused block may not be reclaimed before
   its original appears in a committed archive receipt". Receipts are committed
   only below `next_step`. Try to construct a state where the guard passes but
   the receipt is not yet committed, or where an archive's member list omits the
   original. **[ASSUMED]**

5. **Is the 50,000-block identity claim real?**
   `test_50k_blocks_allocate_identically_and_keep_state_bounded` claims identical
   allocation, job IDs and RNG seeds. Verify it actually compares against the
   pre-refactor allocator (`git show HEAD~1:src/full_mc/application/production.py`)
   rather than only against itself. **[READ]**

6. **Does `state.json` really stay bounded?** The claim is that its size does
   not grow with block count. Check the assertion is a real bound and not a
   tautology.

7. **Is `merge_groups.json` still consistent?** It is now a *derived* snapshot
   written by both the seam and handoff export. Check no reader treats it as a
   source of truth, and that the two writers cannot disagree.

---

## 8. Reproducing everything

```bash
cd <repo>/.worktrees/production-core
PURGE='find . -path ./tests/generated -prune -o -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null; true'

# whole suite, cache purged per module (22 modules, 147 tests)
for t in test_core_production test_core_seam_acceptance test_core_seam_recovery \
         test_core_wave_budget test_core_reclaim_wiring test_core_reclaim \
         test_core_ledger test_core_pre_logging test_core_input_validation \
         test_core_handoff test_core_planning test_core_workers \
         test_core_storage_safety test_core_input_records test_core_plan_original \
         test_core_simulation_profiles test_core_credentials test_core_workspace \
         test_core_ihep test_core_condor test_core_analysis test_core_planner_local; do
  eval "$PURGE"
  printf "%-32s " "$t"
  PYTHONPATH=src python3 -m unittest tests.$t 2>&1 | grep -E "^OK|^FAILED" | head -1
done
```

Result at the time of writing: **147 tests, 0 failing modules** (20 modules
before this change).

Independent probes written during the session and kept out of the tree
(`/tmp/chiw/`): `root_dag_budget.py` (drives only the declared waves),
`wave_budget_probe.py` (same against the local-staging fixture).

---

## 9. Not done

- **No production submission.** Plan implementation step 7
  (`mcprod runtime prepare` + a new pilot) was not executed. It requires a
  proxy and submits real Condor jobs.
- **No live DAGMan run** (see §7.1).
- §6.1 and §6.2 are **unfixed** — they need a decision, because both "obvious"
  fixes break existing fixtures or the archive path.
