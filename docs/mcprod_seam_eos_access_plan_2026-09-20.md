# mcprod wave-seam EOS access: failure analysis and refactor plan

Date: 2026-09-20
Status: **proposal for review — no code has been changed**
Author: session analysing the `JpsiJpsiPhi_MC_Production_v5_0_pre1_lhepilot1` failure
Scope: `production-core` (`mcprod`) wave planning / seam reconciliation only.
The legacy `dag_generator.py` path is explicitly out of scope (it is a separate entry point).

Line numbers below refer to the runtime snapshot at
`generated/JpsiJpsiPhi_MC_Production_v5_0_pre1/runtime-cern-v5/controller/core`
(`runtime.json` `git_revision = c577d61d0ec8314fcac720af969ef2fd56696cb8`).

---

## 1. Summary

On 2026-09-19 the `v5_0_pre1` JJP pilot completed its first wave correctly and then
aborted. The root cause has **not been proven** — the PRE script's stderr was never
captured — but the surviving suspect is narrow: **511 container-wrapped `xrdfs stat`
calls executed serially by the PRE script on the DAGMan submit host**, one of which
returned non-zero, which aborted the entire DAG.

This is not an EOS reliability problem. Every check that could be performed locally
passed: the proxy is valid, all 511 staged blocks exist with matching sizes, and all
result sidecars satisfy their contracts. The problem is the **access pattern** —
position, granularity, and context — and the block count amplifies it.

The plan proposes four layers, ordered by risk. The user has stated that the next
production round will be **tens of thousands of blocks**, which makes L4 mandatory
rather than optional.

---

## 2. Background: the wave architecture

This section is self-contained; a reviewer should not need the originating conversation.

### 2.1 Static declaration, runtime filling

`write_root_dag` (`planning/graph.py:103`) writes `run.dag` **once** at planning time.
It declares a fixed number of wave slots:

```
steps = len(spec["sources"]) + sum(c.max_processing_nodes for c in campaigns) + 1
```
— `application/production.py:126`. For this pilot: `4 + 6*1 + 1 = 11`.

Each slot emits three lines (`planning/graph.py:130-136`):

```
SUBDAG EXTERNAL WAVE_000001 <root>/steps/000001.dag
SCRIPT PRE      WAVE_000001 <root>/advance.sh 1
PRE_SKIP        WAVE_000001 99
PARENT WAVE_000000 CHILD WAVE_000001
```

**`steps/000001.dag` does not exist when `run.dag` is written.** It is produced later,
by the PRE script. `PRE_SKIP ... 99` marks a slot skipped-as-successful when
`advance.sh` exits 99 (used for unused slots and for termination via `.complete`).

This is an upper bound on the slot count; unused slots are skipped.

### 2.2 The seam

For slot *i*, DAGMan:

1. runs `SCRIPT PRE WAVE_i` → `advance.sh i` → `mcprod internal advance-wave --step i`
   **on the submit host, inside the DAGMan process** (`graph.py:111` comment:
   *"SCRIPT PRE runs in the DAGMan process, not in a worker .sub job."*).
   `advance.sh` explicitly exports `X509_USER_PROXY` / `X509_CERT_DIR` (`graph.py:112-116`)
   and refuses (exit 2) if the proxy is missing/unreadable.
2. on exit 0, opens `steps/00000i.dag` and submits it as an external SubDAG;
3. that SubDAG submits its worker jobs; on completion the cycle repeats.

`advance_wave` (`production.py:576`) is the seam body:

- `:578` read `run.json` + `state.json`
- `:579` `_write_groups` → `merge_groups.json`
- `:589` `_finish_pending(...:523)` — **settle**: consume the previous wave's results,
  update `blocks` / `resident` / `originals` / `pool_cursors` / `job_offsets` / `archives`
- `:592` completion test: `all(job_offsets[c] >= max_processing_nodes[c])`
- `:602` `_allocation(spec, state)`
- `:603-606` branch: `_process_phase` if any campaign has allocatable nodes, else `_source_phase`
- `:608` `atomic_json(state.json)`

Two phases exist: `_source_phase` (`:166`) generates LHE + plans blocks;
`_process_phase` (`:237`) builds MIX → MINIAOD (→ ntup) nodes plus an ARCHIVE node.

### 2.3 Two kinds of "sidecar"

| | Task result | Product sidecar |
|---|---|---|
| File | `result.json` | `<product>.json` |
| Location | **AFS** `<root>/results/<task>.json` | **CERN EOS**, beside the product |
| Written by | worker, at job end | worker, after writing the product |
| Content | `{task_id, stage, status, runtime_release, outputs:[...], originals:[...]}` | `{artifact_id, complete:true, status, events, size_bytes, url, kind, runtime_release, ...}` |
| Read by | seam `_result()` (`:312`) — **local, zero remote calls** | `validated_record()` (`storage/transfer.py:190`) |

The AFS path is created by a fixed remap in a **per-stage** submit file
(`planning/graph.py:53-55`):

```
transfer_output_files  = result.json,diagnostics.tar.gz
transfer_output_remaps = "result.json=<root>/results/$(task).json;..."
```

`validated_record` reads the EOS sidecar and cross-checks `status`, `artifact_id`, `url`,
`complete`, and `events`/`size_bytes`/`kind`/`runtime_release`/`count_source` against the
local ref, then does one `client.size()` as a backstop. It is used for the archive
(`production.py:383`) and MiniAOD (`:551`) — **but not for blocks**.

### 2.4 state.json

`atomic_json` (`io.py:10`) is `mkstemp` + `os.replace` — a single atomic rename. Under a
single writer, readers are therefore already safe (they see the old or the new file,
never a partial one).

---

## 3. Incident 2026-09-19

Root DAGMan `13477642` @ `bigbird27.cern.ch`, run dir
`generated/JpsiJpsiPhi_MC_Production_v5_0_pre1/cern-v5-lhepilot1`.

```
18:55:30  LHE_000002 succeeded (pool_2jpsi_g, 833 events)
18:57:08  PLAN_000002 wrote its manifest
18:57:10  WAVE_000000 subdag controller (13477643) terminates successfully
18:57:14  DAGMan: "Running PRE script of Node WAVE_000001..."
          tally: Done=1 Pre=1 Un-Ready=9
18:58:33  "PRE Script of Node WAVE_000001 failed with status 1"
18:58:34  DAG_STATUS_NODE_FAILED → run.dag.rescue002 → condor_rm → EXITING WITH STATUS 1
```

`state.json` was **not modified** (`next_step` still `0`, `pending` still step 0), which
places the failure inside `_finish_pending` before `atomic_json` at `:573`.

Earlier the same day, two attempts (16:14:57, 16:17:43) died at WAVE_000000 on a
`helac_package.tar.gz` basename mismatch; the current runtime's `bootstrap.py` carries an
explicit fallback for it keyed on the literal name `helac_package_cbseed_v1.tar.gz`.
**That fallback is fragile if the package is renamed again** — noted, not part of this plan.

History: this is a recurrence. On bigbird15 (2026-09-17) the *same* call site
**wedged** (killed by its own `timeout 3600`, status 124). Here it **failed fast** (79 s,
status 1). Same code path, opposite symptoms — "PRE returned quickly" is not evidence of health.

---

## 4. Evidence ledger

### 4.1 Verified

| Claim | How verified |
|---|---|
| PRE proxy valid | `voms-proxy-info --file /afs/cern.ch/user/c/chiw/condor/x509up` → 141 h left; authenticates to `xrdfs root://eosuser.cern.ch` |
| All 511 blocks present, sizes matching `PLAN_*.json` | `xrdfs ls -l` on each pool's `blocks/` dir vs the sidecar refs — **0 missing, 0 mismatches** |
| All 8 result sidecars satisfy `_result` | `task_id` / `status=="ok"` / `stage` / `runtime_release` all match |
| `_validate_planner_result` data contracts hold | block ids, per-block events, partial-tail rule, `originals` artifact_id/url/kind/events/size — all match the frozen tasks |
| `_check_bytes` far from limit | usable = 100 GB − 10 GB reserve vs ~1.5 MB of blocks |
| PRE stderr is not captured | `dagman.out` records only `PRE Script of Node WAVE_000001 failed with status 1`; `DAGMAN_VERBOSITY setting: 3` |
| `ssh bigbird27` unusable | connection closed on port 22 |

### 4.2 NOT verified — the critical gap

**The actual exception text is unrecoverable.** Everything above passed, so the failure
must lie in the 511 remote calls themselves (network, container start, or a single
transient), but **this is a strong hypothesis, not a proven root cause**.

Consequence for this plan: **the first work item is observability, not refactoring.**
Without it, a second failure will be equally undiagnosable.

### 4.3 The blocking call, located

`_validate_planner_result` (`production.py:321`) → `client.size(ref["url"])` (`:355`) and
the same for the retained original (`:371`).

`TransferClient.size` (`storage/transfer.py:57`) → `_run(["xrdfs", endpoint, "stat", lfn])`
→ `run_profile("utilities-el9", ...)` (`:28-55`), scratch hardcoded to `/tmp/chiw/mcprod-transfer`.
So **each call starts a container to run one `xrdfs stat`**. 79 s ÷ 511 ≈ 0.15 s each.

---

## 5. Two measured numbers that shape the plan

### 5.1 511 blocks staged, 11 ever consumed

Wave 0 produced 10,093 LHE events in **511 blocks** (`block_events = 20`; 265+101+43+102).

Total consumption over the whole production:

```
6 campaigns × max_processing_nodes = 1        →  6 processing nodes
per-slot lhe_budget = 20 = exactly 1 block
total slots = 1+1+2+2+2+3 = 11                →  11 blocks consumed
```

The seam verifies **100 % of the inventory to serve 2 % of it**.

Note this ratio is pilot-specific (`block_events=20`, `lhe_budget=20`, one node per
campaign). At production scale the *absolute* counts grow; the ratio depends on the
chosen block size and budgets. The structural point stands: verification is
inventory-scoped while consumption is budget-scoped.

### 5.2 state.json growth == the PLAN fragments

```
PLAN_000000.json  96,785
PLAN_000001.json  36,025
PLAN_000002.json  15,750
PLAN_000003.json  33,241
                 ───────
                 181,801 B ≈ 182 KB     ≈ state.json's projected growth
```

The same block inventory is stored four times: the PLAN fragment, `state.blocks`,
`state.resident` (a duplicate of `blocks`), and per-block product sidecars on EOS.

**Therefore folding + deleting the fragments will not shrink `state.json`.** At tens of
thousands of blocks this file becomes tens of MB and is **rewritten in full at every seam**.

---

## 6. Proposal

### L1 — Granularity: N remote calls → 1

**Now:** the seam does one `xrdfs stat` per staged artifact.

**Proposed:** a per-wave aggregation task collects the product sidecars into one
`wave_manifest.json` on EOS; the seam reads exactly one object.

Benefits beyond cost: the aggregator is a **worker** node, so it has `.out`/`.err` —
directly repairing the diagnosability gap. Individual failures become retryable inside
that node instead of aborting the DAG.

Open item: reconciliation policy when the manifest and the actual inventory disagree.

### L2 — Position: take remote I/O off the submit host

**Now:** the PRE does authenticated remote I/O from the DAGMan AP — the only component
that is not sandboxed, has no `.out`/`.err`, and has historically been the fragile host.

**Proposed:** validate at the point of consumption (each job checks its own inputs).

**Caveat (raised by the user and confirmed in CLAUDE.md):** a source block may be reused
across subprocess classes, so point-of-use validation is **re-executed** per consumer.
It is not a clean amortization win.

**Hard prerequisite — this is the safety question:** the consumer must assert
*"events actually read == events declared"* for each input. **That assertion does not
currently exist.** What exists today:

- `workers/mix.py` records `actual_mixed_hepmc_events` and per-slot `actual_hepmc_events`
  (from each `shower_N_manifest.json`) into the task result — but **compares nothing
  against the declared LHE block event count**.
- `workers/miniaod.py:16-24` compares ROOT events against the **mixed-HepMC** count
  (`expected = task["inputs"][0]["events"]`), sets `partial`, and records
  `expected_events` / `partial` in the published metadata.
- `miniaod.py:18-19`: a shortfall raises **only if** `normal_shortfall_policy !=
  "report-and-truncate"`. The default **is** `report-and-truncate`
  (`settings.py:184`).

So a truncated-but-readable block produces **fewer mixed events, recorded as a shortfall,
without failing**. It is recorded rather than silent, but it is not an error. And because
`job_offsets` advances at wave-construction time (`:567`), with completion keyed on
`job_offsets` reaching the target (`:592`), a systematic shortfall can reach `.complete`
under-produced.

**Conclusion:** L2 is only safe if a per-slot "LHE read == declared" assertion is added.
L2 should not replace seam verification until that exists.

### L3 — Ledger: fragments + fold (mostly already implemented)

The fragment mechanism already exists: `results/<task>.json` is the per-job fragment,
written by the remap at `graph.py:53-55`, read once by `_finish_pending`, and **never
deleted**. `_finish_pending` *is* the fold.

What is actually missing — three small things:

1. **Completeness check.** `state["pending"]["task_ids"]` already lists every task the
   wave should produce. Compare the `results/` directory against it and fail loudly on a
   mismatch. Zero new state.
2. **Cleanup.** `mv` folded fragments to `applied/wave_NNNNNN/` rather than deleting —
   same cost, preserves the only per-job forensic record (this investigation depended on
   exactly such files).
3. **Idempotency key.** Atomicity moves from one `os.replace` to
   fold → commit → archive. A crash between commit and archive would re-apply fragments.
   Since a fold handles exactly one wave, a scalar suffices: `"applied_wave": N`, with the
   fold guarded by `applied_wave < N` and the archive step made repeatable. No per-fragment
   ids needed, because the writer is unique.

### L4 — Scale: get the inventory out of state.json

**Required, given tens of thousands of blocks.**

- Move the block inventory to `inventory/<pool>.json`; keep `state.json` to scalars
  (`status`, `next_step`, `applied_wave`, `pool_cursors`, `job_offsets`, `pending`) —
  currently ~697 B.
- Drop `resident`: it duplicates `blocks` and is only used for `_used_bytes` summation and
  identity lookups.
- The fold's target becomes the per-pool inventory file, so "fold" and "shrink" are the
  same operation.

This is the only change that alters the state layout, and the only one needing its own
validation and rollback plan.

---

## 7. Suggested order

1. **Observability first** — capture PRE stderr (raise `DAGMAN_VERBOSITY`, or `tee` from
   `advance.sh`). Zero risk, and without it the next failure is equally opaque.
2. **L3's three items** — low risk, no layout change.
3. **L1** — the aggregation node; the actual fix for the 511-call fan-out.
4. **L2** — only after adding the per-slot events-read assertion.
5. **L4** — separately, with rollback.

---

## 8. Open questions for reviewers

Please try to falsify these specifically:

1. **Is the root cause really the 511 remote calls?** Everything verifiable passed. The
   direct evidence was destroyed by `DAGMAN_VERBOSITY=3`. Is there another candidate
   inside `_finish_pending` consistent with (a) failing after ~79 s, (b) leaving
   `state.json` untouched?
2. **Is 79 s consistent with 511 sequential container-wrapped stats?** If a single
   `run_profile("utilities-el9", ...)` call is much faster or slower than ~0.15 s, the
   arithmetic that motivated L1 needs revisiting.
3. **Does L1 actually reduce remote round-trips?** Writing one manifest requires the
   aggregator to *read* N sidecars (N reads) unless the worker emits the manifest itself
   or the sidecars are packed. Confirm the accounting before treating L1 as a cost win
   rather than a robustness win.
4. **Is the `events-read == declared` gap real?** Read `workers/mix.py` and the mixer's
   `mixed_manifest.json` producer. If some layer already enforces it, L2's prerequisite
   is satisfied and L2 becomes viable sooner.
5. **Is L3's `applied_wave` scalar sufficient?** Consider a crash between the commit and
   the archive with a *resubmitted* DAG that re-runs an earlier step, and DAGMan rescue
   semantics.
6. **Does anything outside the seam read `state.blocks` / `state.resident`?** L4's blast
   radius depends on it — check `_blocks`, `_used_bytes`, `_allocation`, and the review
   tooling under `tools/`.

---

## 9. Reproduction notes

The verification in §4.1 was performed read-only from `lxplus975`:

```bash
# proxy used by the PRE script
voms-proxy-info --file /afs/cern.ch/user/c/chiw/condor/x509up

# every block's size vs its sidecar claim (511 blocks, 0 mismatch)
xrdfs root://eosuser.cern.ch ls -l <staging>/sources/<pool>/<source_id>/blocks

# sidecar contracts
python3 -c "..."   # compare results/PLAN_*.json against tasks/PLAN_*.json

# the DAG's own verdict
tail -40 <run_dir>/run.dag.dagman.out
```

Note `condor_q -constraint` returns **0 rows** on bigbird27 even for `Owner=="chiw"`;
every query must use explicit cluster IDs. For liveness, use the **startd slot `LoadAvg`**,
not the schedd's `CumulativeRemoteUserCpu` (which updates in batches and makes healthy jobs
look stalled).
