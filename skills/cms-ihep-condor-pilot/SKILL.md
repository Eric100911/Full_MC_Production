---
name: cms-ihep-condor-pilot
description: Plan and validate small Full_MC_Production CERN SOURCE/PLAN/PROC_SUBDAG/RECLAIM pilots and their IHEP handoffs.
---

# CMS CERN to IHEP Pilot

Read [pilot procedures](../../docs/testing.md) and the [Condor operator skill](../cms-condor-job-operator/SKILL.md).
Use the Condor skill for live queue inspection, submission, and recovery.

- Freeze exact counted LHE references, a positive planned event count, and a
  small processing-node count. Record expected blocks, subprocess coverage,
  detector variants, and merge target before submission.
- Render an unsubmitted DAG with `mcprod run plan`. Inspect the SOURCE dependencies, PLAN transfer, flat PROC child DAG and RECLAIM.
- For an authorized pilot, record the exact schedd and root cluster ID.
  Verify the CMSSW thread and stream counts and exercise a whole-job retry.
- Confirm terminal cleanup, export the handoff, and verify the existing
  IHEP merge and ntuple stages. Use the HepJob backend on IHEP.
