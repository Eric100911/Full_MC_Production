# ==============================================================================
# ntuple_jup_cfg.py - Ntuple configuration for JUP (J/psi + Upsilon + phi) analysis
# ==============================================================================
# Repo-owned config used by the production wrapper for JpsiUpsPhi ntuples.
# Based on TPS-Onia2MuMu-Dev-J-U-P branch
# Reads MiniAOD and produces flat ntuple for physics analysis
#
# Usage:
#   cmsRun ntuple_jup_cfg.py inputFiles=file:input.root outputFile=output.root runOnMC=True
# ==============================================================================

import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing

# Command line options
ivars = VarParsing.VarParsing('analysis')

ivars.inputFiles = (
    'file:input_MINIAOD.root',
)
ivars.outputFile = 'ntuple_jup.root'

# Custom options
ivars.register('runOnMC',
               True,
               VarParsing.VarParsing.multiplicity.singleton,
               VarParsing.VarParsing.varType.bool,
               "Run on MC (True) or Data (False)")

# Note: maxEvents is already registered by VarParsing('analysis')
# Set default value instead of re-registering
ivars.maxEvents = -1

ivars.parseArguments()

# Configuration flags
ANALYSIS_MODE = 'JpsiUpsPhi'
DO_MONTE_CARLO_TREE = False
REQUIRE_ACCEPTED_CANDIDATES_FOR_MONTE_CARLO_TREE = False
AddCaloMuon = False
runOnMC = ivars.runOnMC
HIFormat = False
UseGenPlusSim = False
UsepatMuonsWithTrigger = False

# Process definition
process = cms.Process("mkcands")

# Message logger
process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.suppressInfo = cms.untracked.vstring("mkcands")
process.MessageLogger.suppressWarning = cms.untracked.vstring("mkcands")
process.MessageLogger.cerr.FwkReport.reportEvery = 100

# Geometry and conditions
process.load("TrackingTools/TransientTrack/TransientTrackBuilder_cfi")
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load("Configuration.StandardSequences.Reconstruction_cff")
process.load("Configuration.StandardSequences.MagneticField_AutoFromDBCurrent_cff")

# Max events
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(ivars.maxEvents)
)

# Global tag
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')
from Configuration.AlCa.GlobalTag import GlobalTag

if runOnMC:
    process.GlobalTag.globaltag = cms.string('124X_mcRun3_2022_realistic_v12')
else:
    process.GlobalTag = GlobalTag(process.GlobalTag, '124X_dataRun3_PromptAnalysis_v1', '')

# Input source
process.source = cms.Source("PoolSource",
    skipEvents = cms.untracked.uint32(0),
    fileNames = cms.untracked.vstring(ivars.inputFiles),
)

# Output module (for debugging, usually not used)
process.out = cms.OutputModule("PoolOutputModule",
    fileName = cms.untracked.string('test.root'),
    SelectEvents = cms.untracked.PSet(SelectEvents = cms.vstring('p')),
    outputCommands = cms.untracked.vstring('drop *')
)

# Filters (MiniAOD has no generalTracks, so omit noscraping)
process.primaryVertexFilter = cms.EDFilter("GoodVertexFilter",
    vertexCollection = cms.InputTag('offlineSlimmedPrimaryVertices'),
    minimumNDOF = cms.uint32(4),
    maxAbsZ = cms.double(24),
    maxd0 = cms.double(2)
)

process.noscraping = cms.EDFilter("FilterOutScraping",
    applyfilter = cms.untracked.bool(True),
    debugOn = cms.untracked.bool(False),
    numtrack = cms.untracked.uint32(10),
    thresh = cms.untracked.double(0.25)
)

# PAT setup
process.load("PhysicsTools.PatAlgos.patSequences_cff")
process.load("PhysicsTools.PatAlgos.cleaningLayer1.genericTrackCleaner_cfi")
process.cleanPatTracks.checkOverlaps.muons.requireNoOverlaps = cms.bool(False)
process.cleanPatTracks.checkOverlaps.electrons.requireNoOverlaps = cms.bool(False)

from PhysicsTools.PatAlgos.producersLayer1.muonProducer_cfi import *
patMuons.embedTrack = cms.bool(True)
patMuons.embedPickyMuon = cms.bool(False)
patMuons.embedTpfmsMuon = cms.bool(False)

# Filter sequence (only PV filter for MiniAOD inputs)
process.filter = cms.Sequence(process.primaryVertexFilter)

# Gen particle producer for MC matching
process.genParticlePlusGEANT = cms.EDProducer("GenPlusSimParticleProducer",
    src = cms.InputTag("g4SimHits"),
    setStatus = cms.int32(8),
    filter = cms.vstring("pt > 0.0"),
    genParticles = cms.InputTag("genParticles")
)

# MC matching configuration
if HIFormat:
    process.muonMatch.matched = cms.InputTag("hiGenParticles")
    process.genParticlePlusGEANT.genParticles = cms.InputTag("hiGenParticles")

if UseGenPlusSim:
    process.muonMatch.matched = cms.InputTag("genParticlePlusGEANT")

# Track tools for JUP analysis
from PhysicsTools.PatAlgos.tools.trackTools import *

# ==============================================================================
# JUP-specific analyzer configuration
# ==============================================================================

process.mkcands = cms.EDAnalyzer('MultiLepPAT',
        analysisMode = cms.untracked.string(ANALYSIS_MODE),
        HLTriggerResults = cms.untracked.InputTag("TriggerResults","","HLT"),
        inputGEN  = cms.untracked.InputTag("genParticles"),
        VtxSample   = cms.untracked.string('offlineSlimmedPrimaryVertices'),
        DoJPsiMassConstraint = cms.untracked.bool(True),
        DoMonteCarloTree = cms.untracked.bool(DO_MONTE_CARLO_TREE),
        RequireAcceptedCandidatesForMonteCarloTree = cms.untracked.bool(REQUIRE_ACCEPTED_CANDIDATES_FOR_MONTE_CARLO_TREE),
        MonteCarloParticleId = cms.untracked.int32(20443),
        trackQualities = cms.untracked.vstring('loose','tight','highPurity'),
        MinNumMuPixHits = cms.untracked.int32(1),
        MinNumMuSiHits = cms.untracked.int32(3),
        MaxMuNormChi2 = cms.untracked.double(99999),
        MaxMuD0 = cms.untracked.double(10.0),
        MaxJPsiMass = cms.untracked.double(3.4),
        MinJPsiMass = cms.untracked.double(2.7),
        MinNumTrSiHits = cms.untracked.int32(4),
        MinMuPt = cms.untracked.double(1.95),
        JPsiKKKMaxDR = cms.untracked.double(1.5),
        XCandPiPiMaxDR = cms.untracked.double(1.5),
        UseXDr = cms.untracked.bool(False),
        JPsiKKKMaxMass = cms.untracked.double(5.6),
        JPsiKKKMinMass = cms.untracked.double(5.0),
        resolvePileUpAmbiguity = cms.untracked.bool(True),
        addXlessPrimaryVertex = cms.untracked.bool(True),
        Debug_Output = cms.untracked.bool(False),

        TriggersForJpsi = cms.untracked.vstring(
            "HLT_Dimuon0_Jpsi3p5_Muon2_v",
            "HLT_DoubleMu4_3_LowMass_v"
        ),
        FiltersForJpsi = cms.untracked.vstring(
            "hltJpsiMuonL3Filtered3p5",
            "hltDoubleMu43LowMassL3Filtered"
        ),

        TriggersForUpsilon = cms.untracked.vstring("HLT_Trimuon5_3p5_2_Upsilon_Muon_v"),
        FiltersForUpsilon = cms.untracked.vstring("hltVertexmumuFilterUpsilonMuon"),
 
        Chi2NDF_Track =  cms.untracked.double(15.0),
        OniaDecayVtxProbCut = cms.untracked.double(0.01)
)

if HIFormat:
    process.mkcands.GenLabel = cms.InputTag('hiGenParticles')
if UseGenPlusSim:
    process.mkcands.GenLabel = cms.InputTag('genParticlePlusGEANT')

# ==============================================================================
# TFile Service for output
# ==============================================================================

process.TFileService = cms.Service("TFileService",
    fileName = cms.string(ivars.outputFile)
)

# ==============================================================================
# Path definition
# ==============================================================================

process.p = cms.Path(
    process.filter *
    process.mkcands
)

# Schedule
process.schedule = cms.Schedule(process.p)
