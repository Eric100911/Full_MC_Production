---
name: cms-ihep-condor-pilot
description: Plan and validate small Full_MC_Production v3 CERN SOURCE/SHARD pilots and their IHEP handoffs.
---

# CMS CERN to IHEP Pilot

Read [pilot procedures](../../docs/testing.md) and the [Condor operator skill](../cms-condor-job-operator/SKILL.md).
Use the Condor skill for live queue inspection, submission, and recovery.

- Freeze exact counted LHE references, a positive planned event count, and a
  small processing-node count. Record expected blocks, subprocess coverage,
  detector variants, and merge target before submission.
- Render an unsubmitted DAG with `mcprod run plan`. Inspect the SOURCE and
  SHARD children, worker transfer configuration, and terminal cleanup.
- For an authorized pilot, record the exact schedd and root cluster ID.
  Check the HepMC checkpoint before MiniAOD and exercise selective recovery.
- Confirm terminal cleanup, export the handoff, and verify the existing
  IHEP merge and ntuple stages. Use the HepJob backend on IHEP.
