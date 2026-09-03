# TPS CERN/IHEP Cutover Follow-up Fix Plan

Status: implemented on 2026-09-03. This record captures the completed
documentation and split-submitter follow-up; it does not authorize production
submission, cleanup, or recovery actions.

## Goal

Document the intended operating boundary clearly:

- CERN production runs unattended under HTCondor DAGMan for multiple days and
  produces block MiniAODs.
- IHEP merge and ntuple stages run in human-watched batches, with a person
  checking queue state, logs, outputs, and the next submission.
- The CERN/IHEP boundary remains an explicit handoff rather than an
  agent-dependent end-to-end controller.

## Implemented follow-up

1. **README workflow description**
   - Replaced stale shared-temporary-path proxy wording in both English and
     Chinese READMEs with the job-private
     `${PWD}/credentials/x509_user_proxy` path.
   - Kept the separate persistent submit-side proxy requirement.
   - Distinguished the current split workflow from the legacy monolithic
     HepJob backend.

2. **Testing and operations procedures**
   - Documented the explicit persistent `X509_USER_PROXY` and workspace-root
     setup, plus the human-watched sequence: inventory, merge, audit, ntuple,
     audit, and reconciliation.
   - Clarified that `audit-split` validates sidecars only; the TPS inventory
     also checks positive-size ROOT products and writes a separate failure
     report.
   - Kept cleanup preview-only in the canonical procedure.

3. **HepJob reference material and submitter**
   - Replaced username-, timestamp-, and deployment-specific workspace examples
     with `IHEP_WORK_ROOT` and `$USER`.
   - Marked the legacy orchestrator’s global no-active-jobs wait as unsuitable
     for cutover control.
   - Changed generated split NTUPLE submitters to pin each CMSSW15 task
     round-robin to the eleven AVX2-safe nodes; MERGE remains unpinned.

4. **Inventory and completion criteria**
   - Defined complete component, merge, and ntuple products as requiring both
     the expected sidecar and a positive-size ROOT output.
   - Clarified that missing products are classified for human follow-up; the
     inventory command does not submit, retry, or remove jobs.

5. **Regression coverage**
   - Extended the split-workflow test to require pinned, per-index NTUPLE
     submission while preserving array-style MERGE and retry snapshot behavior.

## Out of scope

- No cross-site autonomous controller.
- No in-flight lock protocol.
- No automatic cleanup, forced DAG release, or destructive recovery action.
- No changes to the authoritative inventory or campaign job specifications.
