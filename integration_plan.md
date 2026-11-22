# Archived Integration Plan: Efficiency/Acceptance Workflow

> **Historical, non-operational document.** This plan predates the unified
> `T2_CN_Beijing` workflow and has been implemented or superseded. Its branch
> advice, package assumptions, and IHEP URLs must not be used for current
> production. Use `README.md`, `docs/testing.md`, and
> `docs/directory_path_reference.md` for maintained instructions.

## Historical Summary

  Use ihep as the integration base, keep hepthu local-HTCondor behavior as a secondary portability
  layer, and make production outputs directly consumable by multileppat_vertex_batch efficiency
  workflows.

  The target acceptance/efficiency contract is the one implemented in ~/python3-utils/
  multileppat_vertex_batch/: JpsiJpsiPhi only, tree mkcands/X_data, full-GEN denominator, and
  cumulative steps ending in final_nominal.

  ## Key Changes

  - Enable full MC truth ntuple output for JJP efficiency samples.
    The efficiency package requires branches like:

    "Phi_K_1_genMatchIdx",
    "Phi_K_2_genMatchIdx",
    "muGenMatchIdx",
    "muIsJpsiFilterMatch",
    "MC_GenPart_pdgId",
    "MC_GenPart_motherGenIdx",
    "MC_GenPart_pt",
    "MC_GenPart_eta",
    "MC_GenPart_phi",
    "MC_GenPart_mass",
    These are consumed in EFFICIENCY_BRANCHES and build_event_efficiency_row(...).
    These are consumed in EFFICIENCY_BRANCHES and build_event_efficiency_row(...).

  - Update the ntuple CMSSW configuration and packaged worker config contract so MC efficiency runs
    use:

    DoMonteCarloTree = cms.untracked.bool(True)
    RequireAcceptedCandidatesForMonteCarloTree = cms.untracked.bool(False)

    This is required because the reference efficiency docs define the denominator as all full-GEN
    events, including events without accepted reconstructed candidates.

  - Extend processing/run_chain.sh ntuple invocation to pass explicit MC-efficiency flags when
    enabled:

    run_cmsrun_cmssw15 "${cfg_path}" \
        inputFiles="file:${MINIAOD_OUTPUT}" \
        outputFile="${NTUPLE_OUTPUT}" \
        runOnMC=True \
        era=Run2022 \
        analysisMode="${analysis_mode}" \
        doMonteCarloTree=True \
        requireAcceptedCandidatesForMonteCarloTree=False \
        maxEvents=-1

  - Add workflow-level switches to both production entrypoints:
      - dag_generator.py: add --efficiency-ntuple or equivalent option that implies --enable-ntuple
        and passes the MC truth flags into processing jobs.
      - hepjob_workflow.py: mirror the same option for IHEP hep_sub jobs.
      - Keep default production behavior unchanged unless the efficiency flag is requested.
  - Add an ntuple manifest output step after successful transfer.
    The manifest should match multileppat_vertex_batch:

    {
      "JJP_DPS1": [

  "root://cceos.ihep.ac.cn///eos/ihep/cms/store/user/xcheng/MC_Production_v3/output/JJP_DPS1/0/output_ntuple.root"
      ]
    }

    This lets users run:

    run-multileppat-efficiency \
      --analysis-mode JpsiJpsiPhi \
      --input-file-manifest jjp_efficiency_files.json \
      --samples JJP_DPS1,JJP_DPS2_CS,JJP_DPS2_G,JJP_SPS_CS,JJP_SPS_G \
      --output-dir /tmp/chiw/jjp_efficiency_v1

  - Preserve the external efficiency semantics:

    EFFICIENCY_STEPS = (
        "full_gen",
        "fiducial_acceptance",
        "hlt_muon_matched",
        "single_jpsi_reco",
        "double_jpsi_reco",
        "single_phi_reco",
        "triple_gen_matched_candidate",
        "jpsi_quality",
        "phi_quality",
        "all6_same_recVtx",
        "Pri_fitValid",
        "Pri_fitPass",
        "Pri_assocPVPass",
        "Pri_trackPVPass",
        "final_nominal",
    )

    Do not reimplement these calculations in the production repo; production should only produce
    compatible ntuples and manifests.

  ## Branch Integration

  - Implement first on ihep, because it is current and contains the latest IHEP HepJob workflow.
  - Port the same options to hepthu only after ihep works, preserving LOCAL_OUTPUT_BASE and wrapper
    behavior from hepthu.
  - Do not base the work on master; it is behind the active workflow branches.
  - Treat VtxSmeared/T2_CN_Beijing as feature sources only if specific LHE/shower changes are
    needed later.

  ## Test Plan

  - Static checks:

    python3 -m py_compile dag_generator.py hepjob_workflow.py
    bash -n processing/run_chain.sh

  - DAG generation smoke:

    python3 dag_generator.py generate-test \
      --campaign JJP_DPS1 \
      --jobs 1 \
      --max-events 5 \
      --enable-ntuple \
      --efficiency-ntuple \
      --dry-run

  - HepJob generation smoke:

    python3 hepjob_workflow.py generate-test \
      --campaign JJP_DPS1 \
      --jobs 1 \
      --max-events 5 \
      --enable-ntuple \
      --efficiency-ntuple

  - Ntuple compatibility check on one produced file:

    run-multileppat-efficiency \
      --analysis-mode JpsiJpsiPhi \
      --input-files
  root://cceos.ihep.ac.cn///eos/ihep/cms/store/user/xcheng/MC_Production_v3/output/JJP_DPS1/0/output_ntuple.root
  \
      --sample-name JJP_DPS1_smoke \
      --output-dir /tmp/chiw/jjp_eff_smoke \
      --skip-plots

  - Acceptance criteria:
      - mkcands/X_data exists in output_ntuple.root.
      - Required MC_GenPart_*, muGenMatchIdx, Phi_K_*_genMatchIdx, trigger-match, and primary-
        vertex flag branches are present.
      - event_step_flags.parquet has nonzero full_gen rows for valid JJP MC.
      - subprocess_summary.csv is written with n_full_gen, n_final_nominal, and final_efficiency.

  ## Assumptions

  - Efficiency/acceptance sync is required for JpsiJpsiPhi first; JUP is not implemented in the
    reference efficiency package yet.
  - The packaged jjp_code.tar.gz ConfFile_cfg.py accepts or can be updated to accept the new MC
    truth options.
  - Existing production defaults should remain unchanged unless the new efficiency mode is
    explicitly enabled.
