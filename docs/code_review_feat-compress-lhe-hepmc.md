# Code Review — `feat/compress-lhe-hepmc`

> **Archived review snapshot.** Findings describe the branch state on
> 2026-06-22; many were fixed or refuted later. Do not use this file as a current
> defect list or operational guide. Revalidate every finding against `HEAD`.

**Date**: 2026-06-22
**Scope**: 48 files, +6254/−504 lines, 13 commits + uncommitted changes
**Review effort**: max — 5 parallel sub-agents × 9 finder angles + 1-vote verification + gap sweep
**Upstream**: `master`

---

## Findings summary

| Severity | Count |
|----------|-------|
| CRITICAL | 2 |
| HIGH | 8 |
| MEDIUM | 7 |
| LOW | 5 |
| **Total** | **22** |

---

## CRITICAL

### 1. Header content misdetected as `<event>` tag — output corruption

- **File**: `lhe_generation/lhe_shuffle_split.cc:243`
- **Source**: agent-a20b3b2a99a466210 (C++ angle A)

`trimmed.find("<event") == 0` fires even when `in_header` is true. An XML header line like `<event_setup>true</event_setup>` triggers event parsing mode, causing the parser to consume the header, init block, and first real event as a single garbage "event."

**Failure scenario**: Any LHE file with `<event_*>` in its `<header>` block → parser corrupts init block → all output blocks are invalid. HELAC-generated LHE files routinely contain header tags — this triggers in production.

**Fix**: Guard the `<event>` detection with `!in_header`:
```cpp
if (!in_header && !in_event && trimmed.find("<event") == 0 ...)
```

---

### 2. Header content misdetected as `<init>` tag — tool crash

- **File**: `lhe_generation/lhe_shuffle_split.cc:269`
- **Source**: agent-a20b3b2a99a466210 (C++ angle A)

Same class of bug: `trimmed.find("<init") == 0` fires when `in_header` is true. A tag like `<init_params>` or `<initial>` in the header triggers init mode. `parse_init_block` fails on the corrupt text.

**Failure scenario**: Any LHE file with `<init_*>` or `<initial*>` in its header → `[ERROR] Failed to parse <init> block` → tool exits non-zero → planner job fails.

**Fix**: Guard the `<init>` detection with `!in_header`:
```cpp
if (!in_header && !in_init && trimmed.find("<init") == 0 ...)
```

---

## HIGH

### 3. EOS_BASE cross-file default mismatch (chiw vs xcheng)

- **Files**: `dag_generator.py:41-42`, `processing/run_chain.sh:75`, `lhe_generation/run_helac.sh:74`
- **Source**: manual review (angle C), agent-a48f2b10a069ed735

`dag_generator.py` hardcodes chiw's EOS area (`EOS_PATH_BASE = "/eos/ihep/cms/store/user/chiw/..."`) but the runtime scripts default to xcheng's area. The DAG scans/validates one storage area while execution reads/writes another.

**Failure scenario**: `dag_generator.py generate` without `--target-base-url` → DAG points to chiw's pools → `run_chain.sh` executes with xcheng default → LHE file-not-found or output lands in unexpected location.

**Fix**: Either:
- Propagate `TARGET_EOS_BASE` to `run_helac.sh` (like `run_chain.sh` does), or
- Centralize the default in `common/paths.sh` and source it everywhere.

---

### 4. `_list_remote_dir` silently swallows ALL errors → pool regeneration

- **File**: `dag_generator.py:1121-1139`
- **Source**: manual review (angles A, B)

Every exception (`TimeoutExpired`, `OSError`) and every non-zero return code from `xrdfs`/`gfal-ls` returns `[]`. This propagates to `count_lhe_files_on_t2` returning `(0, None)`, making `scan_existing_pools` mark pools as empty — triggering full LHE regeneration.

**Failure scenario**: Transient network hiccup during `xrdfs ls` → count=0 → system believes pool is empty → 10,000+ LHE events unnecessarily regenerated.

**Fix**: Distinguish "directory doesn't exist" (return 0, no error) from "network failure" (return 0 with error string). Let callers decide whether to proceed.

---

### 5. `run_helac.sh` omits `--filename-prefix` — concurrent jobs overwrite each other's blocks

- **File**: `lhe_generation/run_helac.sh:1290`
- **Source**: agent-a20b3b2a99a466210 (angle C cross-file)

Shuffle-split is called without `--filename-prefix`. Block files are named `block_000000.lhe` (no seed qualifier). Multiple concurrent HELAC jobs for the same pool write identically-named blocks to the same `lhe_blocks/` directory. Compare with `plan_lhe_blocks.py:189` which correctly passes `--filename-prefix "{group_id}_"`.

**Failure scenario**: Two HELAC jobs (seeds 100, 200) for the same pool → `block_000000.lhe` from seed 200 overwrites seed 100's version → downstream processing gets mixed-up events.

**Fix**: Add `--filename-prefix "${MY_SEED}_"` to the shuffle-split args in `run_helac.sh`.

---

### 6. Processing resource request: disk reduced 10x → out-of-disk errors

- **File**: `dag_generator.py:2083-2088`
- **Source**: manual review (angle A)

| Mode | Old | New | Reduction |
|------|-----|-----|-----------|
| Premix | 80GB | 8GB | 10× |
| Regular | 50GB | 8GB | 6× |
| Test | 20GB | 4GB | 5× |

**Failure scenario**: `PREMIX_INPUT_MODE=localcache` → job requests 8GB disk → downloads 40GB premix file → disk quota exceeded → job killed → cascading DAG failures.

**Fix**: Restore previous disk values unless there is evidence the old values were unnecessarily high.

---

### 7. Plan manifest: spurious event conservation failure from missing key

- **File**: `tools/plan_lhe_blocks.py:216`
- **Source**: agent-af7032600e695288c (angle A)

`shuffle_manifest.get("total_input_events")` returns `None` if the key is absent from the C++ tool's manifest JSON. `None != n_events` is always `True` → planner exits with spurious "event conservation mismatch" error.

**Failure scenario**: C++ tool rebuilt with different manifest key name → planner falsely reports conservation failure → pipeline halts despite correct split.

**Fix**: `if splitter_events is None or splitter_events != n_events:`

---

### 8. `EOS_PATH_BASE` extraction breaks on single-slash `TARGET_EOS_BASE`

- **File**: `processing/run_chain.sh:76`
- **Source**: agent-a48f2b10a069ed735 (angle D)

`${EOS_BASE#root://${EOS_HOST}/}` assumes double-slash after hostname. A single-slash URL strips the leading `/` from the path.

**Failure scenario**: `TARGET_EOS_BASE='root://cceos.ihep.ac.cn/eos/ihep/...'` → `EOS_PATH_BASE='eos/ihep/...'` (no leading `/`) → xrdfs uses relative path → all remote storage operations fail.

**Fix**: Normalize the double-slash before stripping:
```bash
EOS_BASE_NORMALIZED="${EOS_BASE/\/\///}"
```

---

### 9. Multi-file init compatibility check is dead code

- **File**: `lhe_generation/lhe_shuffle_split.cc:143-190, 852-879`
- **Source**: manual review (angle B), agent-a20b3b2a99a466210

`inits_compatible()` is marked `__attribute__((unused))` and never called. The loop at lines 852-864 that claims to check init compatibility does nothing meaningful — it copies an uninitialized `InitBlock`, reads `nprup==0`, does nothing, then prints a warning. Two LHE files with different beam energies or process lists are silently merged.

**Failure scenario**: Two HELAC runs at different COM energies merged → output init block from first file only, contains incompatible events → downstream uses wrong cross-sections → physics results invalid.

---

### 10. XML declaration misidentified as manifest file

- **File**: `lhe_generation/lhe_shuffle_split.cc:436-452`
- **Source**: agent-a20b3b2a99a466210 (angle A)

`--input-manifest` auto-detection: if the first line lacks "LesHouchesEvents", the file is treated as a manifest listing file paths. An LHE file starting with `<?xml version="1.0"?>` (legal per LHE spec) has no "LesHouchesEvents" on line 1, so it's misclassified.

**Failure scenario**: LHE file with XML declaration → `LesHouchesEvents` (line 2) treated as filename → `[ERROR] Cannot open input file: <LesHouchesEvents>`.

---

## MEDIUM

### 11. `plan_lhe_blocks.py`: hardcoded XRootD hostname in 3 places

- **File**: `tools/plan_lhe_blocks.py:76, 97, 313`
- **Source**: agent-af7032600e695288c (angle C)

`"cceos.ihep.ac.cn"` is hardcoded instead of importing from `dag_generator.EOS_HOST`. If the endpoint changes, all three uses must be manually updated.

---

### 12. `plan_lhe_blocks.py`: xrdcp failure raises uncaught `CalledProcessError`

- **File**: `tools/plan_lhe_blocks.py:139`
- **Source**: agent-af7032600e695288c (angle A)

`subprocess.run(["xrdcp", ...], check=True)` inside a `try` that only catches `ValueError` from `_normalize_inputs`. The `CalledProcessError` propagates past the `finally` block (which cleans up workdir) with a raw traceback.

---

### 13. Command injection in `ensure_dir()` via `system()`

- **File**: `lhe_generation/lhe_shuffle_split.cc:802-804`
- **Source**: manual review (angle D), agent-a20b3b2a99a466210

`system("mkdir -p \"" + path + "\"")` — shell metacharacters in `output_dir` enable arbitrary command execution. Low risk (path comes from trusted DAG VARS), but still a code smell.

---

### 14. Buffer overflow in `snprintf` with long `filename_prefix`

- **File**: `lhe_generation/lhe_shuffle_split.cc:637`
- **Source**: manual review (angle D), agent-a20b3b2a99a466210

`char fname[64]` with variable prefix — the suffix `block_000000.lhe` takes 16 chars, leaving only 47 for the prefix. Longer prefixes are silently truncated, potentially causing filename collisions between blocks.

---

### 15. `write_manifest` does not check `rename()` return value

- **File**: `lhe_generation/lhe_shuffle_split.cc:793`
- **Source**: agent-a20b3b2a99a466210 (angle A)

Unlike `write_lhe_block` (which checks), `write_manifest` ignores rename failure. On cross-filesystem rename (`EXDEV`), the `.tmp` file is left behind and the manifest at the expected path does not exist.

---

### 16. `_resolve_existing_lhe_path` fallback layout inconsistency

- **File**: `dag_generator.py:2412`
- **Source**: manual review (angle A)

Uses `LHE_pool/{subdir}` while `pool_remote_path` uses `lhe_pools/{storage}` when `existing_lhe_base` is empty. Files written via one path won't be found by the other.

---

### 17. `check_remote_file` double-slash path rejected by strict XRootD

- **File**: `processing/run_chain.sh:357`
- **Source**: agent-a48f2b10a069ed735 (angle D)

Path extraction from `root://host//eos/...` produces `//eos/...`. Some XRootD server versions reject paths beginning with `//`.

---

## LOW

### 18. `_ensure_openssl_dev_symlinks` dead code (30 lines)

- **File**: `dag_generator.py:1646-1675`
- **Source**: manual review (angle B)

Function removed from caller `build_cmssw15_runtime_tarball` but the definition and the `LIBRARY_PATH` setup remain. Could cause scram build failures on EL9 minimal installs if the symlinks were load-bearing.

---

### 19. `EFFICIENCY_NTUPLE` default changed from `false` → `true`

- **File**: `processing/run_chain.sh:1505`
- **Source**: manual review (angle B)

Production wrappers always pass `--efficiency-ntuple` explicitly, so production is unaffected. But direct local invocations now default to efficiency mode.

---

### 20. O(n²) stratum count computation can hang on large files

- **File**: `lhe_generation/lhe_shuffle_split.cc:767-771`
- **Source**: agent-a20b3b2a99a466210 (angle E)

Triple-nested loop scanning every event index against every stratum. For 10M events with 100 strata: ~10¹⁰ comparisons. Tool appears to hang for minutes during manifest generation.

---

### 21. `compute_n_strata` uses `stoi()` without error handling

- **File**: `lhe_generation/lhe_shuffle_split.cc:472`
- **Source**: agent-a20b3b2a99a466210 (angle D)

All other integer args use `parse_int`/`parse_u64` with validation. `--n-strata` passes raw string to `stoi()`, which throws `std::invalid_argument` on bad input.

---

### 22. Hardcoded `/tmp/chiw/` in test scripts

- **Files**: `tests/local_chain_test/run_full_chain_test.sh:50`, `tests/check_y_symmetry.py:172`
- **Source**: agent-acbe31479eacb28f1 (angle A)

Fails for any user other than `chiw` or when `/tmp/chiw/` doesn't exist.

---

## Verified correct (false positives eliminated)

| Claim | Verdict | Agent |
|-------|---------|-------|
| `ntuple_jjp_efficiency_cfg.py` deletion breaks efficiency mode | **REFUTED** — new config handles via `analysisMode`; `keepAllSingleObjectCandsInMC=True` preserved | acbe31479eacb28f1 |
| `parse_init_block` 10-field vs 8-field handling broken | **REFUTED** — `tokens[n-2]`/`tokens[n-1]` correctly handles both formats | a20b3b2a99a466210 |
| `per_stratum` integer division causes bias | **REFUTED** — ceiling division with randomized stratum order distributes rounding correctly | a20b3b2a99a466210 |
| Shower binary rngSeed breaks backward compatibility | **REFUTED** — all three use `(argc > N)` guards with safe defaults | acbe31479eacb28f1 |
| HLT filter labels wrong in new ntuple config | **REFUTED** — match documented values (`hltJpsiMuonL3Filtered3p5`, `hltDoubleMu43LowMassL3Filtered`) | acbe31479eacb28f1 |
| `flush_event` lambda lifetime issue | **REFUTED** — captures local references within function scope, no dangling | a20b3b2a99a466210 |

---

## Top fixes before merge

1. **CRITICAL #1, #2** — Guard `<event>` and `<init>` detection with `!in_header` in `lhe_shuffle_split.cc`
2. **HIGH #3** — Unify EOS_BASE defaults across dag_generator, run_chain.sh, and run_helac.sh
3. **HIGH #4** — Don't swallow errors in `_list_remote_dir`; distinguish "not found" from "network failure"
4. **HIGH #5** — Add `--filename-prefix "${MY_SEED}_"` to shuffle-split call in `run_helac.sh`
5. **HIGH #6** — Restore processing disk values or justify the reductions

---

## Docs to update (per user request)

- **CLAUDE.md**: Add documentation for `TARGET_EOS_BASE` override mechanism, `--existing-lhe-base` flag, `EXISTING_LHE_SUBDIR_BY_POOL` mapping, `CHIW_EOS_OUTPUT_BASE` constant, and `NTUPLE_VERSION`
- **Shower `.cc` files** (`shower_normal.cc:13`, `shower_phi.cc:19`, `shower_sps.cc:20`): Update top-of-file usage comments to include `[rngSeed]` positional argument
- **CLAUDE.md**: Note that `ntuple_jjp_efficiency_cfg.py` was merged into `ntuple_jjp_cfg.py`; efficiency mode now controlled via `analysisMode` parameter, not a separate config file
