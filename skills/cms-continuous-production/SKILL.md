---
name: cms-continuous-production
description: Operate v3 CMS production across CERN SOURCE/SHARD DAGMan and IHEP HepJob, including checkpoint verification, cutover, and batch recovery.
---

# CMS Continuous Production Operator

Use the repository's [v3 operations guide](../../docs/testing.md) and
[mcprod command reference](../../docs/README.md). Keep the frozen run,
runtime revision, logs, and scheduler identities together.

- On CERN, use `mcprod run status` and `tools/mcprod_snapshot.py` with the
  recorded root. Use the Condor operator skill for scheduler actions.
- Verify terminal block reclamation and a complete run before exporting
  the handoff. Preserve component IDs, detector metadata, and group order.
- On IHEP, run `mcprod batch prepare` with the matching runtime and proxy.
  Inspect the prepared manifest, then operate the batch through
  `mcprod batch submit`, `status`, and `resume`. The backend uses HepJob.
- Use the [IHEP submission reference](references/hepjob-submission.md) for
  site-specific credentials, host requirements, and failed batch diagnostics.
