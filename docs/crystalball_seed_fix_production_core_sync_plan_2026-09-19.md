# CrystalBall seed fix and production-core catch-up plan

Date: 2026-09-19

Status: implementation plan; no production-core fix has been applied yet

Source fixes: `f707564`, `ad2f0ef`, `931c933`, `7ed3da5`, `e124496`,
and `16d30c7` on `feat/cern-ihep-split-workflow`

## Objective

Make the `refactor/production-core` checkout reproduce the seeded CrystalBall
HELAC behavior already validated in the legacy workflow, make its runtime
release self-contained in a clean checkout, and prevent generated LHE from
being directed into immutable source inventories.

The implementation must cover both:

1. the supported `mcprod` production-core path; and
2. the retained `dag_generator.py` compatibility path where the same shared
   HELAC worker remains reachable.

Do not submit a production DAG as part of this change. Offline tests, a clean
runtime-build smoke test, and a deliberately small seed-sensitivity pilot are
the acceptance boundary.

## Verified current state

### End-to-end production-core call path

The relevant execution path is:

```text
load_run_spec()
  -> plan_run()
  -> _source_phase()
  -> LHE task with explicit pool and seed
  -> runtime release transfers helac-el7 asset
  -> bootstrap installs the asset below core/common/packages
  -> workers/lhe.py copies helac_package.tar.gz to scratch
  -> run_helac.sh --pool ... --seed ...
  -> CrystalBall addon
  -> workers/lhe.py publishes the declared source.lhe.gz
  -> plan worker deterministically splits it into blocks
```
The production-core adapter correctly passes the resolved seed:

```python
command = [
    "bash", str(core_path(task, "lhe_generation/run_helac.sh")),
    "--pool", str(p["pool_name"]),
    "--seed", str(p["seed"]),
    # ...
]
```

The defect is below that boundary. `run_helac.sh` writes the main HELAC card's
`set seed`, but the `pp_psiX_CrystalBall` addon does not consume that card. Its
patched Fortran source reads `addon/pp_psiX_CrystalBall/input/seed.input` via
the upstream `RANDA_init.inc` idiom. The refactor worker currently creates no
such file, so distinct task seeds still produce the addon's compiled default
RNG stream.

### Package contents and reproducibility

The published package in the original checkout was inspected directly:

```text
outer package sha256:
  407a62f3bdc29774f9537a8326a69e6665dd1c17a02d5c952e242a7d3cd55760

addon/pp_psiX_CrystalBall/src/pp_psiX_cb.f90:
  37  INTEGER::beam1_pdf,beam2_pdf
  38  INCLUDE "../../../src/RANDA_init.inc"

patched source sha256:
  1f2d67e68e9709f23abd11024dc09aa1fca9d7b62095aaa2f0867af9b65db5e8

upstream src/RANDA_init.inc sha256:
  820898dea874531aaf19ae2294749a07b30b7113359029ff47a31e21dfb61ade
```

`tools/repack_helac_crystalball_seed.py --check` passes against the published
tarball and manifest.

The refactor currently has only an ignored absolute symlink named
`common/packages/helac_package.tar.gz` pointing into the original checkout.
That masks the defect locally but cannot work in a clean clone or independent
worktree. The versioned seeded tarball and its manifest are not tracked on the
refactor branch.

There is also a basename constraint in the runtime design:

- `prepare_runtime()` copies the selected heavy asset to the release and puts
  its basename in `runtime.json`;
- `unpack_runtime()` installs that same basename below
  `core/common/packages/`; and
- `workers/lhe.py` specifically opens
  `core/common/packages/helac_package.tar.gz`.

Therefore merely changing the source path to
`helac_package_cbseed_v1.tar.gz` would install the wrong runtime basename and
break the worker. Runtime preparation must copy the versioned source under the
stable runtime name `helac_package.tar.gz`.

### Generated-LHE storage behavior

Production-core already improves on the legacy layout. Freshly generated LHE
is declared below a run-scoped CERN staging path:

```python
base = f"{_staging(spec)}/sources/{source['pool']}/{source['source_id']}"
original = _ref(
    f"LHE_{index:06d}",
    base + "/source.lhe.gz",
    "lhe",
)
```

Existing source URLs remain read-side inputs, and generated source and block
products use the declared `base` above. This means the legacy
`generated_lhe_pool_dir()` function should not replace production-core's
run-scoped layout.

The missing part is fail-closed validation. `load_run_spec()` accepts storage
overrides but does not reject a `cern_staging` root which resolves into a
configured immutable LHE inventory or the parent directory of a selected
existing source. Defaults are safe; a run-spec override is not guarded.

The legacy guard deliberately discarded the XRootD authority because all of
its compared LHE paths were on one IHEP EOS. Production-core spans CERN and
IHEP. Its port must retain a normalized authority, otherwise identical-looking
LFNs on distinct endpoints would be treated as the same storage namespace.

### Shared worker defects still present

The refactor's retained `lhe_generation/run_helac.sh` also lacks:

- canonical `root://host[:port]///path` reconstruction;
- the CrystalBall prebuilt-binary relative-symlink normalization; and
- the seed-prefixed shuffle-split globs from `ad2f0ef`.

The URL regression test from `931c933` was run against the refactor script and
reported 17 failures. It reconstructed `root://host//store/...` instead of the
repository-required triple-slash form.

Production-core's normal LHE adapter uses local scratch stageout and its plan
worker performs splitting separately, so the URL and inline-split defects are
not on today's primary core path. They are still reachable through the retained
worker interface and compatibility workflow and should be synchronized in the
same change.

### Divergent changes already accounted for

The fixes through legacy commit `82dc952` are already represented by
`21ba91d` before the core refactor. They do not need to be replayed.

Legacy commit `f707564` is not in the refactor. Its exact implementation is
specific to `DAGBuilder`, while production-core already creates one LHE/PLAN
pair per selected source before campaign allocation. Port `f707564` only to
the retained `dag_generator.py` compatibility path.

## Required invariants

The completed change must preserve all of these:

1. A clean checkout contains the exact seeded package as a tracked Git blob.
2. A CERN runtime release installs it as
   `core/common/packages/helac_package.tar.gz` regardless of the source
   artifact's versioned name.
3. The package contains exactly one CrystalBall anchor and the `RANDA_init.inc`
   include immediately after it.
4. The worker writes the validated positive job seed to the addon's
   `input/seed.input` before launching the addon.
5. Identical seeds are deterministic and distinct seeds change the generated
   CrystalBall event stream.
6. Main-path HELAC behavior is unchanged.
7. Generated LHE and generated blocks remain below the run-scoped staging
   tree, never below an immutable source-inventory directory.
8. Existing-LHE inputs remain exact read-only URLs from the authoritative
   inventory.
9. XRootD URLs emitted by the retained worker preserve the explicit authority
   and triple slash.
10. Inline shuffle-split, when explicitly enabled, discovers and stages the
    `${MY_SEED}_block_*.lhe` files that it actually requests from the splitter.

## Implementation plan

### 1. Import and verify the versioned package artifacts

Bring these exact files from the legacy branch into the refactor:

```text
common/packages/helac_package_cbseed_v1.tar.gz
common/packages/helac_package_cbseed_v1.manifest.json
tools/repack_helac_crystalball_seed.py
tests/test_crystalball_seed_repack.py
```

Use `e124496` for the binary, `7ed3da5` for the manifest, and `931c933` for the
tool and test. Do not add the ignored mutable
`common/packages/helac_package.tar.gz` to Git and do not rely on the current
absolute symlink.

Update the repack tool's default/check examples so the versioned package and
versioned manifest are unambiguous in production-core. Preserve its important
properties:

- input is never overwritten in place;
- the Fortran anchor must occur exactly once;
- an include found elsewhere is rejected rather than duplicated;
- `--check` verifies outer, nested, upstream-include, and patched-source
  checksums from the manifest; and
- scratch remains under `/tmp/chiw/`.

### 2. Stage the versioned package under the stable runtime basename

Modify `src/full_mc/runtime/packages.py`. Keep the source artifact identity
separate from the worker-facing runtime name:

```python
HELAC_SOURCE_PACKAGE = "helac_package_cbseed_v1.tar.gz"
HELAC_RUNTIME_PACKAGE = "helac_package.tar.gz"
HELAC_PACKAGE_SHA256 = (
    "407a62f3bdc29774f9537a8326a69e6665dd1c17a02d5c952e242a7d3cd55760"
)
```

The runtime-preparation branch should follow this shape:

```python
if "helac-el7" in selected:
    source = core / "common/packages" / HELAC_SOURCE_PACKAGE
    if not source.is_file() or source.stat().st_size <= 0:
        raise FileNotFoundError(source)
    runtime_asset = staging / HELAC_RUNTIME_PACKAGE
    shutil.copy2(source, runtime_asset)
    bundles["helac-el7"] = HELAC_RUNTIME_PACKAGE
```

At runtime preparation—not per worker—verify the copied artifact against the
published checksum or manifest. A compact standard-library helper is enough:

```python
def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
```

Fail before publishing the immutable runtime directory if either the source or
copied runtime asset differs. Record the HELAC package source name and digest
in `runtime.json`, for example:

```python
record["helac_package"] = {
    "source": HELAC_SOURCE_PACKAGE,
    "runtime_name": HELAC_RUNTIME_PACKAGE,
    "sha256": HELAC_PACKAGE_SHA256,
}
```

Extend `validate_runtime_release()` to validate this metadata once on the
submit host. Do not add package hashing to each worker invocation.

`runtime/bootstrap.py` and `workers/lhe.py` should not require a behavioral
change once the runtime basename remains stable. Add tests that prove the
whole basename chain:

```python
self.assertEqual(descriptor["bundles"]["helac-el7"], "helac_package.tar.gz")
self.assertTrue(
    (unpacked / "common/packages/helac_package.tar.gz").is_file()
)
```

Also assert that runtime preparation fails when the versioned source artifact
is missing or its digest does not match.

### 3. Bridge the task seed into the CrystalBall addon

Port the two relevant `931c933` hunks to
`lhe_generation/run_helac.sh`.

In `prepare_crystalball_addon_input()`, after preparing the addon cards and
before the function returns:

```bash
# RANDA_init.inc interprets this as the number of initial draws to discard.
# It is a skip seed, not a direct replacement for the compiled DSEED value.
printf '%s\n' "${MY_SEED}" > "${addon_dir}/input/seed.input"
msg_info "CrystalBall RANDA skip seed: ${MY_SEED}"
```

This is downstream of the existing seed validation:

```bash
if ! [[ "${MY_SEED}" =~ ^[0-9]+$ ]]; then
    echo "Error: Seed must be a valid integer"
    exit 1
fi
if [ "${MY_SEED}" -le 10 ] || [ "${MY_SEED}" -ge 100000 ]; then
    echo "Error: Seed must be between 11 and 99999"
    exit 1
fi
```

Keep the write inside `prepare_crystalball_addon_input()` so main-path HELAC
does not acquire an unused addon artifact.

In `normalize_prebuilt_helac_links()`, add the conditional normalization:

```bash
if [[ -L addon/pp_psiX_CrystalBall/bin/HO_pp_psiX_CrystalBall ]] && \
   [[ "$(readlink addon/pp_psiX_CrystalBall/bin/HO_pp_psiX_CrystalBall)" = /* ]]; then
    ln -sfn ../../../bin/HO_pp_psiX_CrystalBall \
        addon/pp_psiX_CrystalBall/bin/HO_pp_psiX_CrystalBall
fi
```

The current seeded tarball contains source and builds this addon on demand, so
the symlink branch is conditional rather than part of today's normal package
path. Retain it to keep prebuilt packages relocatable.

Add the imported structural tests to the default test entry point. They must
verify the exact `seed.input` bridge, unique include placement, idempotent
repack behavior, and manifest checksums without performing a HELAC build.

### 4. Preserve canonical XRootD URLs in the shared worker

Port the URL reconstruction from `931c933`:

```bash
EOS_BASE="${TARGET_EOS_BASE:-root://${EOS_REDIRECTOR}///${EOS_LFN_BASE#/}}"
```

and in `resolve_remote_target()`:

```bash
# path is normalized to exactly one leading slash.
url="root://${host#root://}///${path#/}"
```

Import `tests/test_helac_remote_url.sh`, add it to `tests/run_all_tests.sh`, and
exercise all of these forms offline:

- full IHEP URL with explicit `:1094`;
- bare `/store/...` LFN;
- CERN `/eos/...` path;
- relative target below the configured base;
- final LHE and block directory-plus-filename joins; and
- rejection of collapsed double slashes inside the path body.

### 5. Fix retained inline shuffle-split discovery

Port `ad2f0ef` to the shared worker. The splitter is called with:

```bash
--filename-prefix "${MY_SEED}_"
```

so all subsequent globs must use the same prefix:

```bash
BLOCK_COUNT=$(ls "${SHUFFLE_SPLIT_DIR}/${MY_SEED}_block_"*.lhe 2>/dev/null | wc -l)

cp "${SHUFFLE_SPLIT_DIR}/${MY_SEED}_block_"*.lhe \
   "${SHUFFLE_SPLIT_DIR}/shuffle_split_manifest.json" \
   "${BLOCK_LOCAL_DIR}/"

for bf in "${SHUFFLE_SPLIT_DIR}/${MY_SEED}_block_"*.lhe \
          "${SHUFFLE_SPLIT_DIR}/shuffle_split_manifest.json"; do
    # existing stageout body
done
```

Extend the existing splitter test or add a focused shell test which enables
inline splitting and proves count, local copy, and remote-loop selection all
refer to the seed-prefixed names. Production-core's normal plan worker must
continue to omit `--lhe-shuffle-split`; its separate deterministic planner is
unchanged.

### 6. Add an authority-aware inventory-overlap guard to production-core

Put storage-identity normalization in `src/full_mc/storage/layout.py`, where
URL semantics already live. Do not copy the legacy authority-blind helper.
The target API should distinguish XRootD and local namespaces while normalizing
equivalent spellings within a namespace.

Illustrative shape:

```python
import posixpath
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit


def canonical_storage_location(value):
    text = str(value).strip()
    if text.startswith("root://"):
        parsed = urlsplit(text)
        host = (parsed.hostname or "").lower()
        port = parsed.port or 1094
        path = posixpath.normpath("/" + parsed.path.lstrip("/"))
        return ("xrootd", f"{host}:{port}", PurePosixPath(path))

    path = Path(text).expanduser().resolve(strict=False)
    # A local CERN EOS mount and its XRootD spelling are the same namespace.
    if path.as_posix().startswith("/eos/user/"):
        return ("xrootd", "eosuser.cern.ch:1094", PurePosixPath(path.as_posix()))
    return ("file", "", PurePosixPath(path.as_posix()))


def storage_path_is_within(candidate, directory):
    candidate_kind, candidate_authority, candidate_path = \
        canonical_storage_location(candidate)
    directory_kind, directory_authority, directory_path = \
        canonical_storage_location(directory)
    return (
        (candidate_kind, candidate_authority) ==
        (directory_kind, directory_authority)
        and (candidate_path == directory_path or directory_path in candidate_path.parents)
    )
```

Confirm the chosen default-port rule against the repository's explicit IHEP
endpoint. Portless `cceos.ihep.ac.cn` and explicit `:1094` should compare as the
same authority; `eosuser.cern.ch` and `cceos.ihep.ac.cn` must not.

At the end of `load_run_spec()`, after source URLs have been normalized, form
the actual generated run root:

```python
generated_root = (
    str(spec["storage"]["cern_staging"]).rstrip("/")
    + "/"
    + spec["run_id"]
)
```

Build the immutable directory set from:

1. every non-empty `defaults()["lhe_pool_directories"][...]["path"]`; and
2. the parent directory of every selected existing source URL, including
   inventory paths outside the built-in pool catalogue.

Reject `generated_root` when it is equal to or below one of those directories:

```python
for inventory_dir in immutable_directories:
    if storage_path_is_within(generated_root, inventory_dir):
        raise ValueError(
            "generated staging overlaps immutable LHE inventory: "
            + generated_root
        )
```

Do not reject a sibling which only shares a textual prefix. Do not reject the
same path body on a genuinely different XRootD authority. Do not rewrite any
existing source URL; the guard is validation only.

Focused tests should cover:

- default CERN staging versus configured IHEP inventories: accepted;
- exact configured inventory directory: rejected;
- a descendant of the inventory directory: rejected;
- portless and `:1094` spellings: treated as equal;
- `.` and `..` traversal spellings: normalized before comparison;
- a prefix-sharing sibling: accepted;
- the same LFN on different authorities: accepted;
- a local staging directory below a selected local source's parent: rejected;
- a custom existing-source URL outside configured pool directories: guarded by
  its parent; and
- generated-only specs: still checked against configured immutable pools.

Call this validation from `load_run_spec()` so failure occurs before runtime
validation, run-directory creation, DAG writing, or scheduler interaction.

### 7. Synchronize the retained `dag_generator.py` compatibility path

Because repository guidance explicitly retains the old graph scaffold as a
compatibility path, selectively port the legacy-only portions too:

- `generated_lhe_pool_dir()` and its call sites from `931c933`;
- `assert_generated_dir_not_inventory()` for the single-IHEP legacy namespace;
- generated LHE extension selection based on `compress_lhe`;
- the missing local `lhe_pools/` component in block output paths;
- block-output guarding in skip-LHE mode; and
- one generated planner per pool/job index from `f707564`.

Keep this compatibility implementation separate from production-core's
authority-aware guard. The legacy helper's authority-blind comparison is
correct only because the legacy configured LHE pools all name the same IHEP
storage namespace.

Import `tests/test_generated_lhe_output_location.py`. Its generated-DAG
inspection should continue to prove:

- fresh output uses `<target>/lhe_pools/<pool>`;
- existing-LHE mode reads the exact immutable inventory URL;
- `--existing-lhe-base` cannot redirect fresh output;
- compressed and uncompressed filenames match the worker flag;
- local generation and planning paths agree; and
- inventory-equal or inventory-descendant destinations are rejected.

### 8. Documentation and default test registration

Add the focused tests to `tests/run_all_tests.sh` near the existing HELAC and
storage checks:

```bash
python3 "${SCRIPT_DIR}/test_crystalball_seed_repack.py"
bash "${SCRIPT_DIR}/test_helac_remote_url.sh"
python3 "${SCRIPT_DIR}/test_generated_lhe_output_location.py"
```

Add the new production-core storage/runtime unit-test module to the documented
core test command. Update `common/packages/README.md` so it names the versioned
seeded package as the source artifact and the stable basename as a runtime-only
installation name.

The new plan supersedes the need to copy the legacy catch-up note verbatim,
but the manifest and code should retain the original provenance hashes and
commit references.

## Validation sequence

### A. Static and focused offline checks

Run from `.worktrees/production-core`:

```bash
bash -n lhe_generation/run_helac.sh tests/test_helac_remote_url.sh
python3 -m py_compile \
  tools/repack_helac_crystalball_seed.py \
  src/full_mc/runtime/packages.py \
  src/full_mc/storage/layout.py \
  src/full_mc/settings.py

python3 tools/repack_helac_crystalball_seed.py \
  --check \
  --package common/packages/helac_package_cbseed_v1.tar.gz \
  --manifest common/packages/helac_package_cbseed_v1.manifest.json

python3 tests/test_crystalball_seed_repack.py
bash tests/test_helac_remote_url.sh
python3 tests/test_generated_lhe_output_location.py
PYTHONPATH=src python3 -m unittest \
  tests.test_core_workers \
  tests.test_core_workspace \
  tests.test_core_production \
  tests.test_core_storage_safety
```

Then run the repository-required checks from `AGENTS.md` and
`./tests/run_all_tests.sh`.

### B. Clean-checkout runtime release test

The test must not use the current absolute package symlink.

1. Confirm `git ls-files` contains the versioned tarball and manifest.
2. Prepare a fresh CERN runtime release with `helac-el7` selected.
3. Inspect `runtime.json` and require:

   ```json
   {
     "bundles": {
       "helac-el7": "helac_package.tar.gz"
     }
   }
   ```

4. Verify the release's `helac_package.tar.gz` has the published SHA-256.
5. Exercise `unpack_runtime()` and verify the stable package path beneath the
   extracted core.

### C. Small CrystalBall execution acceptance

Use an isolated scratch/output location and the prepared runtime. Do not write
to the configured immutable `LHE_pool` directories.

Run three small jobs:

```text
seed S      run A
seed S      run B
seed S + 1  run C
```

Acceptance:

- logs report `CrystalBall RANDA skip seed: S` or `S + 1`;
- A and B have identical event content after excluding non-physics timestamps
  or packaging metadata;
- C differs from A/B in generated event content;
- each output is published under the declared run-scoped CERN staging URL;
- the planner consumes that declared URL and produces non-overlapping blocks;
- no file appears beneath a configured immutable inventory directory; and
- a deliberately overlapping run spec is rejected before a run directory or
  DAG is created.

The original E0-E3 study already validates the exact published package's
physics behavior. This smoke test validates that production-core transports
the same bytes and supplies the task seed through its own adapter.

## Commit structure

A reviewable sequence is:

1. `:package: Import the seeded CrystalBall HELAC artifact`
2. `:bug: Install the seeded HELAC package in production runtimes`
3. `:bug: Bridge production seeds into the CrystalBall addon`
4. `:lock: Isolate generated LHE from immutable inventories`
5. `:bug: Catch up the retained HELAC compatibility path`
6. `:white_check_mark: Cover CrystalBall seed and storage regressions`

Do not mechanically cherry-pick `931c933` as one commit: its
`dag_generator.py` code is legacy-specific, while production-core requires the
authority-aware storage adaptation above.

## Multi-agent implementation prompt

Use the following prompt from the repository root when assigning the
implementation to a multi-agent coding session:

```text
Implement the plan in
.worktrees/production-core/docs/crystalball_seed_fix_production_core_sync_plan_2026-09-19.md
inside /afs/cern.ch/user/c/chiw/condor/Full_MC_Production.

First read the worktree's AGENTS.md and the entire plan. Work only in
.worktrees/production-core. Preserve all pre-existing untracked or unrelated
changes, especially:
  docs/mcprod_ihep_pilot_findings_2026-09-18.md
  docs/production_core_simulation_profiles_refactor_plan.md

Use multiple agents with non-overlapping file ownership. All agents share the
same filesystem, so coordinate before editing and do not have two agents modify
the same file.

Suggested division for four available slots:

1. Lead/integrator:
   - inspect status and coordinate file ownership;
   - implement the retained dag_generator.py compatibility changes from
     f707564 and 931c933;
   - integrate all changes, resolve cross-area contracts, run the full test
     matrix, and report the final diff and validation evidence.

2. Package/runtime agent:
   - import the tracked seeded tarball and manifest from legacy commits
     e124496 and 7ed3da5 without relying on the ignored mutable symlink;
   - import/adapt tools/repack_helac_crystalball_seed.py;
   - update src/full_mc/runtime/packages.py so the versioned source is verified
     once and installed as helac_package.tar.gz;
   - add focused runtime-package/bootstrap tests and update package docs;
   - do not edit run_helac.sh or storage code.

3. HELAC worker agent:
   - port the seed.input bridge, CrystalBall relative-symlink normalization,
     triple-slash URL reconstruction, and ad2f0ef seed-prefixed block globs to
     lhe_generation/run_helac.sh;
   - import/adapt tests/test_crystalball_seed_repack.py and
     tests/test_helac_remote_url.sh;
   - add a focused inline-split regression test if existing coverage does not
     exercise the globs;
   - do not edit runtime packaging, settings, or dag_generator.py.

4. Storage-safety agent:
   - implement authority-aware canonical storage identity in
     src/full_mc/storage/layout.py;
   - validate the actual run-scoped generated root during load_run_spec();
   - guard configured inventories and selected custom existing-source parents;
   - add tests for authorities, default ports, traversal, local paths, siblings,
     and failure before filesystem/DAG creation;
   - do not edit run_helac.sh, runtime packaging, or dag_generator.py.

The lead owns shared registration files such as tests/run_all_tests.sh unless
it explicitly delegates them after the worker agents finish. Agents must send
the lead a concise summary of files changed, decisions made, and tests run.

Required behavior:
  - a clean Git checkout carries the exact seeded package with SHA-256
    407a62f3bdc29774f9537a8326a69e6665dd1c17a02d5c952e242a7d3cd55760;
  - runtime preparation copies the versioned source to the stable asset name
    helac_package.tar.gz and records/verifies provenance;
  - CrystalBall writes MY_SEED to input/seed.input before addon launch;
  - main-path HELAC remains unchanged;
  - generated production-core LHE stays below run-scoped CERN staging and
    cannot resolve below immutable inventory directories;
  - storage comparison retains normalized XRootD authority because CERN and
    IHEP are distinct namespaces;
  - retained remote URLs use root://host[:port]///path;
  - retained inline splitting uses ${MY_SEED}_block_*.lhe;
  - the compatibility DAG builder uses write-side generated paths and shares
    one generated planner per pool/job index.

Do not submit production jobs or mutate remote storage. Use apply_patch for
text edits. Use /tmp/chiw for scratch. Do not commit unless explicitly asked.

Run the plan's focused validation, the AGENTS.md-required checks, and
./tests/run_all_tests.sh. Also prepare and inspect one fresh local CERN runtime
release without using the ignored absolute symlink. If environmental limits
prevent the small CrystalBall execution acceptance, complete every offline
check and report the exact remaining command and prerequisite; do not weaken
the acceptance criterion.

Finish with:
  - the implemented invariant set;
  - files changed;
  - exact commands and pass/fail results;
  - any remaining live/small-pilot validation;
  - confirmation that unrelated user files were preserved.
```
