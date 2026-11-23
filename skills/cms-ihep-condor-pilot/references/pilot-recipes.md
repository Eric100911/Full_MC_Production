# v3 pilot commands

Use a reviewed campaign spec with a unique run ID and a pinned CERN runtime.

```bash
./tests/submit_tests.sh /path/to/spec.json /path/to/new-run
./mcprod run submit --run /path/to/new-run
python3 tools/mcprod_snapshot.py --run /path/to/new-run
```

For submission prerequisites, selective recovery, and the IHEP follow-through,
see the [pilot operations guide](../../../docs/testing.md).
