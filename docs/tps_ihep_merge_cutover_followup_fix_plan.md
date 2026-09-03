# TPS CERN/IHEP Cutover Follow-up Fix Plan

Status: outline only. This document records the next documentation pass; it
does not authorize or implement those edits.

## Goal

Document the intended operating boundary clearly:

- CERN production runs unattended under HTCondor DAGMan for multiple days and
  produces block MiniAODs.
- IHEP merge and ntuple stages run in human-watched batches, with a person
  checking queue state, logs, outputs, and the next submission.
- The CERN/IHEP boundary remains an explicit handoff rather than an
  agent-dependent end-to-end controller.

## Planned documentation updates

1. **README workflow description**
   - Replace stale worker proxy wording that refers to shared temporary paths.
   - Describe the private worker credential path and the persistent submit-side
     proxy requirement.
   - Separate the current split workflow from the legacy monolithic HepJob
     backend.

2. **Testing and operations procedures**
   - Update command examples for the explicit proxy argument and
     `X509_USER_PROXY` alternative.
   - Document the manual IHEP sequence: inventory, merge submission, audit,
     ntuple submission, audit, and reconciliation.
   - State that cleanup remains preview-only unless separately authorized.

3. **HepJob reference material**
   - Document the configurable IHEP user/path variables and the legacy
     orchestrator’s no-active-jobs wait.
   - Explain when to use the pinned ntuple submitter and how safe-node pinning
     relates to the CMSSW15 runtime.
   - Keep recovery and submission as login-node operations rather than worker
     responsibilities.

4. **Inventory and completion criteria**
   - Document the separate inventory failure report.
   - Define complete component, merge, and ntuple products as requiring both
     the expected sidecar and a positive-size ROOT output.
   - Clarify that missing products are classified for human follow-up; the
     inventory command does not submit or remove jobs.

5. **Legacy path cleanup**
   - Mark obsolete username-, timestamp-, and deployment-specific examples.
   - Replace remaining stale proxy examples with persistent, user-selected
     paths while retaining exact storage URLs.

## Out of scope

- No cross-site autonomous controller.
- No in-flight lock protocol.
- No automatic cleanup, forced DAG release, or destructive recovery action.
- No changes to the authoritative inventory or campaign job specifications.
