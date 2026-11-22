# v06_00 campaign-shard scheduling and staging plan

**Status:** canonical implementation and operator plan; design only. No CERN lock proof, live pilot, release tag, or production rollout is asserted here. This document is authoritative for the v06_00 work. [The Chinese reader](v06_00_reading_zh.md) is a shorter explanation; when details differ, follow this plan.

**Frozen base:** commit `6c62e05`, in a separate worktree. Keep existing runs, runtime releases, tags, EOS/IHEP data, and the main checkout untouched. Create immutable `v06_00_pre01`, `pre02`, … tags only after each corresponding gate is passed and the exact commit is reviewed. `v06_00` is reserved for the accepted release. No tag or live operation is authorized by this document.

## Product contract

The planner freezes deterministic campaign/shard allocations from complete, counted BLOCK inventories. A BLOCK may be reused by distinct campaigns, which must remain explicit in provenance; no campaign may consume the same BLOCK twice, including across repeated source slots that reference one pool. All source slots in one campaign/pool share a stream and advance it in sequence.

Each `(campaign, pool)` stream has immutable `stride` and `offset`, plus separate `allocation_cursor`, `safe_cursor`, and `closed` fields. The default is stride 1, offset 0. For the agreed 400:80 pool-demand example, DPS1 uses indices `0,1,…,399`, while TPS uses the fixed sequence `0,5,…,395`. This selects the first index of each five-position group; there is no randomized cohort selection. A stream takes whole BLOCKs until its slot's actual LHE event budget is met. Event counts come from accepted inventory manifests, never filenames or size estimates.

`allocation_cursor` is the next pool index available for a new frozen assignment. `safe_cursor` is the contiguous index frontier whose earlier consumer work has completed and met the existing archive requirements. It cannot jump over an unfinished earlier shard and cannot be copied from the allocation cursor while work is active. Mark a stream closed only after it has no future assignments and all assigned consumers are safely complete.

Each source/shard completion trigger runs an ordinary pool-cleanup `JOB`. That job checks possible future or unfinished demand, durable archive prerequisites, accepted consumer completion, run ownership, and exact artifact URLs. If nothing is eligible at that trigger, PRE returns `PRE_SKIP=99` immediately; it does not wait for later refs. With DPS1 `safe_cursor=80` and TPS `safe_cursor=40`, `[40,80)` retains indices `40,45,50,55,60,65,70,75` for TPS. This arithmetic alone does not prove a file is safe to delete. Reclaim tasks name explicit accepted refs; no broad path deletion or EOS lookup runs from PRE.

## AFS 本地文件分桶

新建 `campaign_shards` run 会冻结空文件标记 `.bucketed-v1`，读写双方据此
选择相同布局。没有标记的已有 run 继续使用原来的扁平路径；不迁移已有
文件，也不为恢复操作重写旧 DAG。

任务、结果、已接受结果的完成凭据，以及 source/shard/cleanup 清单按
`<namespace>/<index // 1000>/` 分桶，每个桶最多 1000 个 JSON 文件。
`namespace` 保留阶段、campaign 和 detector profile 等身份前缀，`index`
取既有任务 ID 的末尾数字。例如 `PLAN_001000` 的任务文件是
`tasks/PLAN_/000001/PLAN_001000.json`。桶号使用整除，随索引增加产生新桶。

每个 worker 通常有 `.out`、`.err`、`.tar.gz` 三个日志文件，因此日志使用
`index // 333`，每桶最多 999 个文件。共享的 `logs/events.log` 和
`logs/controller.log` 保持原位置和写入方式。

SubDAG 使用 `subdags/<namespace>/<index // 1000>/<identity>/<identity>.dag`。
例如 `subdags/SOURCE_/000001/SOURCE_001000/SOURCE_001000.dag`；每个 DAG
单独占一个目录，DAGMan 的原生日志、lock 和 rescue 文件留在该目录内，
每个上层桶最多 1000 个子目录。根 DAG 仍为 `run.dag`，公共 submit 文件
仍放在 `submit/`。分桶只改变提交端本地路径，不改变远端产物 URL、任务
ID、随机数种子、物理参数或暂存容量的记账方式。

## Frozen pilot and planning configuration

The initial pilot uses **one isolated staging-budget scope and one authoritative ledger**. Its capacity proof cannot be combined with another run's budget. A future shared budget would require every participating run to use the same ledger and stable lock; it is outside this pilot.

Planning uses top-level `orchestration: "campaign_shards"` and `limits.shard_nodes=10`, `limits.target_window_nodes=20`. This means each campaign shard contains at most ten processing nodes, and the planner exposes a bounded twenty-node target window. Shard allocation order within a campaign may be serialized to preserve job indices and seeds; worker completion is independent across shards and campaigns.

Illustrative run configuration (names and URLs are resolved from the frozen campaign inventory):

```json
{
  "run_id": "v06_00_pre04_first_site_pilot",
  "core_revision": "<immutable implementation commit used for this runtime>",
  "runtime_release": "<runtime built from that exact implementation commit>",
  "orchestration": "campaign_shards",
  "limits": {
    "shard_nodes": 10,
    "target_window_nodes": 20,
    "source_lhe_bytes": 100000000,
    "source_block_bytes": 100000000,
    "mixed_hepmc_bytes": 64000000
  },
  "consumption_streams": [
    {"campaign": "JJP_DPS1", "pool": "jpsi", "stride": 1, "offset": 0},
    {"campaign": "JJP_TPS", "pool": "jpsi", "stride": 5, "offset": 0}
  ],
  "storage": {
    "staging_bytes": 100000000000,
    "reserve_bytes": 10000000000
  },
  "site_validation": {
    "proof_path": "/afs/.../site-validation/cern-afs-lock-proof.json",
    "isolated_budget_bytes": 90000000000
  }
}
```

Validate config at plan creation: `limits.shard_nodes` and `limits.target_window_nodes` are positive strict integers; stream membership must exist, stream pairs must be unique, `stride` is a positive strict integer, and `0 <= offset < stride`. Any omitted stream receives `(stride=1, offset=0)`. The stride/offset set is frozen before allocation; do not silently change it to compensate for observed source yields.

`site_validation.proof_path` is required before submission of a site-enabled run. `isolated_budget_bytes` must be a positive strict integer exactly equal to `storage.staging_bytes - storage.reserve_bytes`. A mismatch, missing or malformed proof, duplicate/insufficient schedds, missing evidence file, or any false/missing check fails closed before submission.

Required proof format:

```json
{
  "schema": "cern-afs-lock-proof-v1",
  "schedds": ["schedd-a.example", "schedd-b.example"],
  "checks": {
    "mutual_exclusion": true,
    "read_after_unlock": true,
    "crash_recovery": true,
    "pre_defer": true,
    "pre_skip": true,
    "dynamic_subdag": true,
    "autorescue": true
  },
  "evidence": [
    "/afs/.../site-validation/flock-contention.log",
    "/afs/.../site-validation/read-after-unlock.json",
    "/afs/.../site-validation/crash-recovery.log",
    "/afs/.../site-validation/dagman-noop.log"
  ]
}
```

The validator requires a `schedds` list of nonempty strings containing at least two unique names; repeated entries are allowed if two distinct schedds are present. It also requires all seven named checks set to the boolean `true` and a nonempty `evidence` list whose path strings already resolve to local files. Evidence paths need not be relative to one proof directory, and the schema has no per-schedd evidence mapping. The proof is a record written after a manually run, explicitly authorized G3 NOOP validation; planning code only reads and validates it. A malformed or absent proof blocks submission. Local tests cannot manufacture site proof.

### First site-pilot acceptance fixture

Use counted input totals of **800 J/psi events and 100 gg events**, capped at 50 events per BLOCK, yielding 16 J/psi BLOCKs and 2 gg BLOCKs in the fixture. DPS1 and TPS each receive two processing nodes, each targeting 20 mixed events; the merge target is 40 events. Record the inventory manifest and source IDs, actual event counts, stride selection, node inputs, seeds, and EDM IDs. If the 2-node/40-event fixture does not match validated campaign input budgets, stop and correct the fixture before preparing a site run; do not infer compatibility from these illustrative totals.

## State ownership and AFS lock contract

Within the isolated run directory, `state.json` is the sole mutable authority for planner cursors, safe frontiers, target progress, and the current capacity-accounting state. Immutable allocation manifests and verified worker/archive/reclaim receipts preserve history. Any convenient report or snapshot is derived from `state.json` and those records; it cannot overwrite or outrank them.

Use a separate, stable `ledger.lock` inode for cross-DAGMan exclusion. Never unlink, replace, or rotate it while the run is active. Under the short whole-file `flock`, read the latest state, validate and settle only already-verified local receipts, apply one idempotent claim/publication/reclaim transition, then write a complete same-directory temporary state and atomically replace `state.json`. `atomic` replacement prevents readers seeing half a file; it does not by itself prevent stale writers or prove cross-host visibility. Those semantics remain unverified until G3 evidence is accepted.

For the first pilot, the staging scope has one isolated `staging_bytes - reserve_bytes` budget. Count each CERN-staged file once, even when multiple campaigns consume it. An outstanding producer claim reserves its maximum new bytes until a verified publication receipt settles it to actual resident bytes. A verified successful reclaim receipt, including an allowed `already-missing` result, returns credit once. A failed producer retains its claim for DAGMan retry/rescue. MiniAOD on IHEP and archive worker scratch are outside CERN staging accounting.

The AFS critical section reads local records only. It must not invoke `xrdfs`, `xrdcp`, Apptainer, workers, physics code, or remote stat. A missing/corrupt ledger, AFS error, invalid receipt, or claim conflict is a hard failure, never a capacity defer.

## Source preparation, publication, and one-time original archive

Declare one source SubDAG for each frozen source. `prepare_source()` atomically reserves the frozen source bundle with `staging.reserve_bundle()`. For generated sources, that bundle includes both the LHE and PLAN claims before the LHE producer can start; PLAN then follows LHE. For an externally provided LHE, PLAN reserves the combined retained-original and BLOCK ceiling. If PLAN uses an LHE generated by this same source, it reuses that original's URL and does not count or upload the original a second time. The source-owned flow is:

```text
source PRE atomically reserves LHE + PLAN claims
        ↓
source LHE producer (generated input only)
        ↓
PLAN producer (accepted complete BLOCK inventory; owns external-LHE retained-original claim when applicable)
        ↓
verified source sidecars and immutable inventory record
        ↓
one source-owned original-LHE archive
```

The source bundle owns the original LHE archive exactly once, independent of how many campaign shards later select its BLOCKs. Archive output and its receipt must satisfy the current worker contract before original or BLOCK cleanup can proceed. Do not put the original LHE into every campaign archive.

For staged LHE, PLAN, and MIX outputs, the producer must check that the local output and its sidecar fit within the frozen `staging_claim_bytes` ceiling before upload. `_common.publish()` already writes the sidecar locally and checks the combined output/sidecar size against the claim before uploading the data and then the sidecar. Existing result acceptance validates the producer result, exact run-owned staged object list, and existing sidecar contract. Do not add a separate collision/path-escape or remote-upload verification subsystem without evidence that the existing storage and publication contract needs it. PRE remains local-only and does not perform remote checks.

Only fully accepted PLAN manifests contribute stable pool indices. Construct each pool prefix in frozen source order, then source-local block index order. An unknown earlier source boundary prevents later outputs from being inserted ahead of it. Out-of-order source jobs can finish, but cannot renumber already-frozen pool positions.

## Deterministic shard assignment

The domain API is `stream_key(campaign, pool)`, `build_streams(spec)`, and `allocate_shard(campaign, blocks_by_pool, streams, start_job_index, node_limit, exhausted_pools)`. Cursors are **physical pool indices**, not counts of selected blocks. For stride 5 at offset 0, cursor 5 means the next eligible pool index is 5. A new stream starts at its offset. It accepts strict integer stride/cursor configuration and returns a frozen manifest or `None` if temporary inventory is insufficient for any requested slot in the shard. Caller state remains unchanged on `None`.

Within one shard, simulate the complete requested node range using a private cursor copy. For each node, process source slots in declared order; repeated slots with the same campaign/pool consume successive distinct eligible BLOCKs from that shared stream. Each slot takes whole BLOCKs until its actual event budget is met. After selecting pool index `i`, advance the trial cursor to `i + stride`; the resulting cursor is the next eligible physical pool index. Do not commit any cursor unless the entire requested shard can be frozen. Preserve existing node/source record shape, task identity, deterministic seed derivation, and EDM event identity.

```python
def take_event_budget(blocks_by_index, cursor, stride, offset, event_budget):
    i = next_matching(cursor, stride, offset)
    selected, events = [], 0
    while events < event_budget:
        block = blocks_by_index.get(i)
        if block is None:
            return None
        selected.append(block)
        events += block["events"]
        i += stride                 # physical next pool index
    return i, selected
```

An exhausted pool can provide a nonempty final partial event budget; report exact requested/available/missing event counts in `shortfalls` for the existing report-and-truncate behavior. If an exhausted pool cannot provide even one BLOCK to a requested slot, raise `FinalInputShortfall` with campaign, slot, and pool context. A temporary shortage is not an exhausted tail. Processing job indices are bounded by the campaign's configured maximum.

For pool index `i` and one stream, the possible-demand predicate is:

```python
def may_still_need(i, stream):
    return (
        not stream["closed"]
        and i >= stream["safe_cursor"]
        and (i - stream["offset"]) % stream["stride"] == 0
    )
```

An index is a reclaim candidate only when no frozen stream may still need it, assigned consumers have durable completion evidence, original/source archive requirements hold, and the exact owned URL is known. The pool cleanup coordinator emits explicit refs; the existing Reclaim worker and receipt remain the deletion authority.

## DAG topology and ordering

The root DAG is a finite skeleton. It declares source SubDAGs, finite campaign-shard slots, one ordinary cleanup `JOB` per source/shard completion trigger, and one local finalizer before DAGMan starts. Each shard PRE creates or restores its deterministic child DAG from a committed allocation manifest. It may defer while waiting for complete PLAN prefix, its prior **allocation** record, or a permitted target window. It must not wait for the prior shard's worker completion.

```dag
# One source-owned LHE → PLAN SubDAG per frozen source.
SUBDAG EXTERNAL SOURCE_000000 /run/subdags/SOURCE_/000000/SOURCE_000000/SOURCE_000000.dag

# Finite slots; each PRE commits one immutable allocation before rendering DAG.
SUBDAG EXTERNAL SHARD_DPS1_000000 /run/subdags/SHARD_DPS1_/000000/SHARD_DPS1_000000/SHARD_DPS1_000000.dag
SCRIPT DEFER 75 60 PRE SHARD_DPS1_000000 /run/bin/prepare_shard.py DPS1 0
SUBDAG EXTERNAL SHARD_TPS_000000 /run/subdags/SHARD_TPS_/000000/SHARD_TPS_000000/SHARD_TPS_000000.dag
SCRIPT DEFER 75 60 PRE SHARD_TPS_000000 /run/bin/prepare_shard.py TPS 0
# No shard-completion edge between these independent children.

# One ordinary cleanup job per declared source/shard completion trigger.
JOB CLEANUP_JPSI_000 /run/submit/reclaim.sub
SCRIPT DEFER 75 60 PRE CLEANUP_JPSI_000 /run/bin/prepare_cleanup.py jpsi 0
PRE_SKIP CLEANUP_JPSI_000 99
PARENT SHARD_DPS1_000000 CHILD CLEANUP_JPSI_000

JOB FINALIZE /run/submit/finalize.sub
SCRIPT PRE FINALIZE /run/bin/finalize_local.py
PARENT SOURCE_000000 SHARD_DPS1_000000 SHARD_TPS_000000 CLEANUP_JPSI_000 CHILD FINALIZE
```

The real generator must declare every referenced node and use valid DAGMan syntax. Source readiness is tracked by the frozen accepted inventory prefix; do not make every shard depend on completion of every source. A shard that lacks enough prefix defers, while other campaigns or ready shards proceed. For one campaign, shard **allocation** order is deterministic so job indices, task IDs, seeds, and event IDs do not depend on which PRE wins a race. Across campaigns, allocation may reuse BLOCK refs.

Within a campaign child DAG, preserve worker-level dependency edges:

```dag
JOB MIX_000 mix.sub
SCRIPT DEFER 75 60 PRE MIX_000 /run/bin/admit.py MIX_000
JOB MINIAOD_A_000 miniaod.sub
JOB MINIAOD_B_000 miniaod.sub
PARENT MIX_000 CHILD MINIAOD_A_000 MINIAOD_B_000
JOB ARCHIVE_MIX_000 archive.sub
PARENT MINIAOD_A_000 MINIAOD_B_000 CHILD ARCHIVE_MIX_000
```

Archive/Reclaim grouping follows current worker output contracts. A source original has one source-owned archive. Mixed HepMC remains until all detector variants assigned to it have completed their existing archive path. Shared BLOCK cleanup is pool-scoped because a child DAG cannot observe arbitrary sibling child nodes; each completion-triggered cleanup job consumes durable completion/watermark records and submits a normal exact-ref Reclaim task. If no refs are eligible at that trigger, PRE returns `PRE_SKIP=99` immediately; it does not wait for future refs. Cleanup must remain runnable when producer capacity is full.

The local finalizer reads `state.json`, accepted immutable receipts, and finite trigger terminal records. It does not query EOS or infer success from a successful POST exit alone.

## Admission, PRE/POST behavior, and recovery

Every task that can add bytes to CERN staging has a stable logical Claim key and a frozen maximum byte ceiling. Claim ID does not change for a normal retry or rescue. Under the shared AFS lock, an identical existing claim is idempotent; a changed ceiling under the same key is a hard conflict. A confirmed no-capacity or demand-window condition returns the documented defer code. Other failures exit nonzero with diagnostics.

```dag
JOB MIX_001 /run/submit/mix.sub
SCRIPT DEFER 75 60 PRE MIX_001 /run/bin/admit.py MIX_001
RETRY MIX_001 1
```

PRE exit `0` means its durable claim exists and the node may submit. PRE exit `75` means the expected resource or inventory condition is temporary and asks DAGMan to retry PRE after the configured interval. `PRE_SKIP=99` means the declared slot is terminally unnecessary; it is not a wait code. Other PRE failures are errors, not hidden capacity waits.

### Reserve the complete shard MIX group before its first producer runs

The frozen shard manifest contains a separate ceiling and stable Claim ID for each MIX producer. The first MIX node's PRE calls `admit_producer(run_dir, task_id)`, reads that shard's `staging_bundle`, and atomically reserves the bundle's full set of MIX claims under the ledger lock. Later MIX PRE calls see the same claims and remain idempotent. Each successful producer result is still settled against its own Claim and actual output size; claim identity and per-producer accounting do not become shard-wide totals.

This all-at-once reservation is required because Archive/Reclaim currently waits for the full shard's MiniAOD group. If PRE reserves one MIX ceiling at a time, early MIX outputs can occupy the remaining staging space while a later MIX cannot reserve its ceiling. The shard archive then waits for that missing MIX/MiniAOD, and no capacity can be reclaimed until the archive completes. Reserving the entire MIX group before its first producer runs prevents this partial-group capacity deadlock while preserving the existing shard size and physics.

The run-planning capacity floor must fit the largest campaign's complementary source tuple plus one full shard of worst-case MIX output:

```python
per_source = source_lhe_bytes + source_block_bytes
source_tuple = unique_campaign_pools * per_source
full_shard_mix = min(shard_nodes, campaign.max_processing_nodes) * mixed_hepmc_bytes
minimum_usable_bytes = max_over_campaigns(source_tuple + full_shard_mix)
```

Source-admission headroom must cover one full shard MIX group, not a single MIX. Keep the complementary-source allowance as well, so source admission cannot consume bytes needed for the rest of that shard's frozen MIX claims. This headroom is an admission threshold, not a second ledger Claim. Reclaim remains runnable without producer credit.

Worker-result POST accepts a result only when the worker return code is zero and the result passes the existing envelope, task, output, sidecar, and run-owned staging checks. It then writes the immutable acceptance receipt and updates authoritative state; successful campaign-shard completion POST also verifies all child results and advances the contiguous safe cursor. A failed upstream worker causes POST to return nonzero without accepting its result or advancing completion state. Preserve already committed manifests, claims, artifacts, and accepted receipts on POST failure so normal DAGMan Retry/AutoRescue can recover idempotently. Do not enable `DAGMAN_ALWAYS_RUN_POST`; retain the current explicit POST handlers. Preserve native `RETRY` and `-AutoRescue`, and verify nested rescue option handling. Never pass an old root rescue-file option blindly to a new child DAG.

## Demand-aware replenishment

For campaign `c`, keep `N[c]` (configured total nodes), `A[c]` (frozen assigned nodes), `T[c]` (bounded soft target), and `D[c]=max(0,T[c]-A[c])`. These are planning values, not Condor completion counts or physical pool cursors. Advance the finite target window deterministically up to 20 nodes, then estimate each pool's actual event shortfall from campaign slot budgets and accepted BLOCK event counts. Prefer source production for complementary bottleneck inputs; pause overproduced pool admission when it consumes headroom without moving a campaign's target.

Keep explicit planning headroom separate from the capacity ledger. It must reserve enough usable space for one complete shard's MIX claims and the required complementary source tuple. Never create a second claim to represent a target or headroom. Reclaim does not require producer credit, so a full staging budget cannot prevent cleanup. A temporarily unavailable campaign must not block unrelated campaigns or a ready pool.

## Required offline checks and DAG inspection

Run offline checks in the isolated worktree with scratch under `/tmp/chiw`:

```bash
TMPDIR=/tmp/chiw python3 -m unittest tests.test_core_streams
TMPDIR=/tmp/chiw python3 -m unittest tests.test_core_staging
TMPDIR=/tmp/chiw python3 -m unittest tests.test_core_shard_interfaces
```

Run the scoped offline suites using the commands above and retain their output with the implementation evidence. These tests cover local allocation and state behavior; they do not prove AFS locks or live DAGMan behavior. Run only suites that exist at the relevant implementation milestone; do not invent a missing command and report it as passed. G1 must also cover retry/rescue manifest identity, out-of-order PLAN prefix stability, contiguous safe-frontier advancement, and stream closure only after all work finishes. G2 must cover one authoritative state file, idempotent claims and settlements, no double-credit, capacity inequality, publication/reclaim exactly once, retained failed-producer claims, and atomic old-or-new state recovery.

For an offline run-plan fixture, use the documented CLI and keep generated files under scratch:

```bash
TMPDIR=/tmp/chiw ./mcprod run plan \
  --spec /tmp/chiw/v06_00_fixture.json \
  --output-dir /tmp/chiw/v06_00_plan
rg --files /tmp/chiw/v06_00_plan
rg -n 'SUBDAG EXTERNAL|PARENT|SCRIPT DEFER|PRE_SKIP|RETRY' \
  /tmp/chiw/v06_00_plan
```

Use a frozen fixture that the implementation's offline planner accepts; these example paths must be created in scratch first. Inspect generated offline DAGs to prove that root slots are finite; source PLAN can feed a ready shard without a global PLAN barrier; one source archive owns each original; campaign shards have no prior-shard completion chain; each MIX fans out to assigned detector variants; Archive follows those variants; cleanup jobs use exact refs and safe frontiers and return PRE_SKIP 99 immediately when empty; finalization waits on declared completion records; PRE is DEFER 75 for temporary conditions. A synthetic DAGMan run in G4 must include a deliberately slow MiniAOD, unrelated shard progress, capacity defer/re-entry, ordinary RETRY, and AutoRescue. **G3 and G5 remain pending.**

## G0–G5 implementation and validation sequence

**G0 — freeze and design review.** Record `6c62e05`, inspect repository instructions, current code/model interfaces, worktree isolation, exact site configuration files, and existing active-run boundaries read-only. Record a concise Plan Delta with code evidence. Freeze runtime and pilot paths separately. Do not alter the main checkout, current runs, data, or tags.

**G1 — pure allocation.** Implement and run offline stream/allocation tests. Keep `domain/allocation.py` unchanged; new behavior lives behind the new domain API and necessary orchestration caller. Inspect the serialized allocation manifest and generated child DAG deterministically.

**G2 — local byte ledger.** Implement the single-authority `state.json`, separate stable lock path, atomic state replacement, idempotent claims and receipt settlement. Test on an isolated local filesystem; success here does not prove AFS semantics.

**G3 — authorized CERN validation.** Only with explicit user authorization, perform isolated NOOP validation on at least two actual schedds. Test lock mutual exclusion, read-after-unlock visibility, lock-holder crash recovery, AFS ACL/token/runtime environment, `SCRIPT DEFER 75`, `PRE_SKIP 99`, lazy child DAG generation, and AutoRescue behavior. Record the proof JSON and evidence paths. No physics production or real staging deletion belongs in this gate. A missing or failed check blocks the site backend and all live production actions.

**G4 — synthetic DAGMan.** Use finite root slots and local fake producers/workers. Prove unrelated shards advance during one slow MiniAOD; cleanup releases capacity only after accepted receipts; a deferred producer becomes eligible after release; ordinary RETRY and automatic Rescue reuse the same task/Claim; no global source PLAN or shard completion barrier appears.

**G5 — authorized pilot and release.** Only after G0–G4 and explicit approval, prepare the two-campaign counted fixture described above in a new isolated AFS run directory with a frozen runtime. Record schedd and root DAGMan cluster together, input manifest/checksum, strides, source and processing counts, seeds/EDM IDs, byte ceilings, ledger proof, logs, merge target, and IHEP handoff. Verify results via existing receipts and operator procedures. Scale only after measured ledger correctness, source deficits, staging occupancy, queue progress, cleanup, and physics metadata pass review. Apply each immutable `preXX` tag only to its clean, reviewed, gate-passing commit. Do not move an existing tag. `v06_00` requires full acceptance, not a successful compile or one pilot.

### Conditional release milestones

These tag names are milestones, not evidence that a gate has passed:

| Tag | Intended scope | Required evidence before creating the immutable tag |
|---|---|---|
| `v06_00_pre01` | Per-campaign physical-index streams, all-or-none shard manifests, unit tests | G1 scoped allocation acceptance; frozen runs untouched |
| `v06_00_pre02` | Single-run byte ledger and AFS lock validation | G2 plus authorized, passing G3 proof; otherwise hold this tag |
| `v06_00_pre03` | Dynamic source/campaign-shard/cleanup DAG and synthetic behavior | G4 shows no global completion barrier and correct retry/rescue |
| `v06_00_pre04` | Demand-aware refill and pilot candidate | G5 explicitly authorized pilot evidence and reviewed handoff |
| `v06_00_pre05+` | Narrow fixes supported by new evidence | Corresponding tests and reviewed evidence for each immutable tag |
| `v06_00` | Stable release | All acceptance criteria and deployment approval |

Do not create a pre-release tag merely because its code exists. Each tag points to one reviewed commit and is never moved; a pilot uses a separate run ID/directory and a runtime frozen from the tagged revision.

## Environment and unverified site facts

Interactive lxplus may have Kerberos and AFS tokens; DAGMan PRE runs on a schedd access point with a different environment; execute workers use their explicit proxy/runtime; IHEP downstream jobs have their own site/runtime constraints. Do not infer one environment from another. PRE uses absolute pinned launcher paths and a minimal known `PATH`, logs startup early, uses AFS-local JSON only, and does not require EOS credentials. Workers retain the frozen runtime and explicit transfer credentials.

Historical PRE access to AFS is not evidence of cross-schedd `flock` mutual exclusion, post-unlock visibility, or crash recovery. These remain unverified pending G3. G3 and G5 are pending; local unit tests do not establish site behavior or pilot readiness.
