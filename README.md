# Full MC Production

This repository contains the refactored production core for the CERN/IHEP
heavy-flavor Monte Carlo workflow. The supported entry point for new work is
`mcprod`; the retained `dag_generator.py` and related scripts support
existing production workflows and compatibility operations.

The core separates campaign data, deterministic source/block planning, bounded
CERN DAG waves, IHEP batch execution, runtime packaging, storage transfers, and
analysis aggregation. The intended operator layout is a separate wrapper
repository containing this core as a pinned submodule, reviewed campaign
specifications, production history, ignored work, and reusable runtimes.

## Quick start

Initialize submodules and install the checkout in editable mode if you want the
`mcprod` console script:

```bash
git submodule update --init --recursive
python3 -m pip install -e .
./mcprod catalog
```

To create a wrapper workspace around a committed core revision:

```bash
./mcprod workspace init --directory /path/to/production-wrapper --core "$PWD"
```

The initializer writes a schema-valid `campaigns/example.json`. Copy it,
replace the source inventory and finite campaign limits, and prepare a
site-specific runtime before planning:

```bash
./mcprod runtime prepare --site cern --release production-v1 \
  --output-dir /path/to/production-wrapper/runtimes/production-v1/cern
./mcprod run plan --spec /path/to/campaign.json \
  --output-dir /path/to/production-wrapper/work/example/cern
```

Every run uses an explicit runtime release and exact counted inputs. Existing
LHE sources require their URL, event count, and seed; generated sources require
a finite event cap. Storage roots and processing defaults are centralized in
`common/node_config_defaults.json`.

The full CLI lifecycle, configuration fields, handoff, IHEP batch, aggregation,
recovery, and worker commands are documented in
[docs/README.md](docs/README.md). Use
[docs/testing.md](docs/testing.md) for live scheduler/storage validation and
the retained legacy workflow.

## Development checks

Run the offline core contracts with:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_core_*.py'
```

The retained baseline checks are:

```bash
./tests/run_all_tests.sh
```

Runtime preparation requires the HELAC package for generation. Ntuple releases
use `common/packages/cmssw15_tpsonia2mumu_runtime.tar.gz` when present and
fall back to the initialized `external/TPS-Onia2MuMu` source package.
Do not commit proxies, generated ROOT products, runtime work directories, or
production credentials.
