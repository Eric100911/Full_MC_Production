# LHE kinematics audit

`tools/lhe_kinematics_audit.py` streams each selected LHE file with pylhe, processes
chunks of 10,000 events using awkward jagged arrays and NumPy, and writes one JSON
report containing per-file and cohort summaries. It supports plain and gzip LHE
files, local paths, and `root://` URLs. A remote file is staged individually with
`/usr/bin/xrdcp --nopbar --force` under `/tmp/chiw` and removed after parsing.
The system `xrdcp` runs with `LD_LIBRARY_PATH` and `PYTHONPATH` removed from
its subprocess environment because the LCG-selected `xrdcp` crashed with
SIGSEGV (exit 139) on the same input. Other environment variables, including
the proxy and XRootD settings, are preserved; the Python analysis keeps its
LCG environment.

## Environment and run

Use the established LCG environment (no package installation is needed):

```bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_109a/x86_64-el9-gcc14-opt/setup.sh
python3 tools/lhe_kinematics_audit.py --config audit.json --output audit.json.out
```

Example configuration using the consolidated planning inventory:

```json
{
  "inventory": "generated/lhe-inventory-consolidated-20261023/planning-inventory.json",
  "sample_seed": 81,
  "lfn_endpoint": "root://cceos.ihep.ac.cn:1094",
  "cohorts": {
    "old": {"pool": "pool_2jpsi_cs", "sample_fraction": 0.01},
    "new": {"pool": "pool_2jpsi_cs", "namespace": ["/store/user/example/new"], "sample_count": 20}
  },
  "comparisons": [{"left": "old", "right": "new"}]
}
```

Each cohort selects rows whose inventory `status` is `ok`. `namespace` is a list
of exact `source_namespace` values; `seed_min` and `seed_max` are inclusive.
`lfn_endpoint` may be set at the top level or overridden per cohort. When set,
only inventory paths starting with `/store/` become XRootD URLs; the example
produces `root://cceos.ihep.ac.cn:1094///store/...` with three slashes before
`store`. Use `null` in a cohort to disable a top-level endpoint. No alternate
endpoint is inferred or probed. `base` is prepended only to relative paths,
making it useful for inventories whose paths are relative to a local staging
directory. Other absolute paths and existing `root://` URLs are used as written.
With no endpoint, `/store/...` is treated as a local absolute path. A fraction
or count samples files deterministically
using `sample_seed`; inventory `actual_events` does not alter sampling. File
quotas across source namespaces use largest-remainder apportionment and sum
exactly to the requested sample size (ties are resolved by namespace name).
Results describe selected files.

## Definitions and reading the report

Only final-state (`status == 1`) PDG 443 particles count as J/psi. The report
includes their transverse momentum and rapidity, and for events with exactly two
such particles it includes pair transverse momentum, invariant mass, and absolute
rapidity separation. Events with zero or a non-two multiplicity are counted;
pair observables are unavailable for them. Rapidity is calculated only where
`E > |pz|`; non-finite values are omitted from distributions. A_FB diagnostics
count positive, negative, and exactly zero single-J/psi rapidities.

The fixed edges are emitted with every histogram. Bins are lower-inclusive and
upper-exclusive; underflow and overflow are reported together. Comparisons
normalize in-range unit-weight bin counts and report the largest cumulative
histogram difference (`histogram_ks_d`) for common fixed bins. This is a
descriptive, binned diagnostic, not a hypothesis test. Small sample sizes,
different processes, or different cuts can make comparisons misleading; compare
like-for-like cohorts and inspect counts alongside the statistic.

Distributions use unit event/particle counts. Where LHE event weights are
provided, the report separately records available weight count, sum, and positive,
negative, and zero signs; weighted histograms are intentionally not inferred.
This avoids presenting negative-weight samples as ordinary probability
distributions. Cohort moments and 10th/50th/90th percentiles summarize finite
values only.

Each parsed event gets a SHA256 fingerprint from sorted final-state particle
IDs, statuses, and four-vectors. Per-file repeated fingerprints indicate exact
kinematic repeats within the file. `kinematic_stream_sha256` combines these
fingerprints in event order, with a length prefix for each fingerprint and the
total event count. Cohort `matching_kinematic_streams` and top-level
`cross_cohort_kinematic_stream_matches` list matching nonempty complete streams
with their `source_id`, path, and cohort. A shared prefix in files of different
lengths does not match. LHE headers and event weights are excluded from this
kinematic signature.

The inventory hash is separately matched against all rows in the same inventory
pool and reported as `same_pool_inventory_alias_paths`. That supplied whole-file
identity is distinct from the fresh kinematic signature and the per-file
`repeated_event_fingerprints` count. A 1% sample can flag duplicates among
inspected files and inventory-known aliases, but cannot establish that the
unsampled pool contains no duplicates.

## Boundaries

The report does not decide whether two unlike physics samples should agree, infer
missing event counts from filenames or sizes, or prove absence of duplicates in
unread files. It reads only selected inventory rows and preserves their source
identity and hash for auditability. Remote staging requires a working `xrdcp`
and access credentials in the execution environment. If pylhe exits while
reading an event stream, the CLI fails and prints the source path and parser
exit code; it does not skip the file.
