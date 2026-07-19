# Auto generated configuration file
# HepMC to GEN-SIM configuration for MC Production
# Based on Run3Summer22 campaign settings
#
# Usage:
#   cmsRun hepmc_to_GENSIM.py inputFiles=file:input.hepmc outputFile=file:output.root maxEvents=-1

import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing

# Command line options
options = VarParsing()
options.register('inputFiles', 
                 ['file:input.hepmc'],
                 VarParsing.multiplicity.list, 
                 VarParsing.varType.string, 
                 "Input HepMC files")
options.register('outputFile', 
                 'file:output_GENSIM.root',
                 VarParsing.multiplicity.singleton, 
                 VarParsing.varType.string, 
                 "Output file")
options.register('maxEvents',
                 -1,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.int,
                 "Maximum number of events to process")
options.register('nThreads',
                 8,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.int,
                 "Number of threads")
options.register('firstRun',
                 1,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.int,
                 "First run number for EDM EventID")
options.register('firstLuminosityBlock',
                 1,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.int,
                 "First luminosity block for EDM EventID")
options.register('firstEvent',
                 1,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.int,
                 "First event number for EDM EventID")
options.register('numberEventsInLuminosityBlock',
                 0,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.int,
                 "Events per luminosity block (0 = no auto-advance)")
options.register('debugDump',
                 False,
                 VarParsing.multiplicity.singleton,
                 VarParsing.varType.bool,
                 "Dump event content and exit")
options.parseArguments()

print(f"[EDM EventID] firstRun={options.firstRun} "
      f"firstLuminosityBlock={options.firstLuminosityBlock} "
      f"firstEvent={options.firstEvent} "
      f"numberEventsInLuminosityBlock={options.numberEventsInLuminosityBlock}")

if options.firstRun < 1:
    raise RuntimeError(f"firstRun must be >= 1, got {options.firstRun}")
if options.firstLuminosityBlock < 1:
    raise RuntimeError(
        f"firstLuminosityBlock must be >= 1, got {options.firstLuminosityBlock}"
    )
if options.firstEvent < 1:
    raise RuntimeError(f"firstEvent must be >= 1, got {options.firstEvent}")
if options.numberEventsInLuminosityBlock < 0:
    raise RuntimeError(
        "numberEventsInLuminosityBlock must be >= 0, "
        f"got {options.numberEventsInLuminosityBlock}"
    )

# Normalize inputFiles in case nested lists sneak in from CLI parsing
normalized_inputs = []
for item in options.inputFiles:
    if isinstance(item, (list, tuple)):
        normalized_inputs.extend(item)
    else:
        normalized_inputs.append(item)
options.inputFiles = normalized_inputs

from Configuration.Eras.Era_Run3_cff import Run3

process = cms.Process('SIM', Run3)

# Import standard configurations
process.load('Configuration.StandardSequences.Services_cff')
process.load('SimGeneral.HepPDTESSource.pythiapdt_cfi')
process.load('FWCore.MessageService.MessageLogger_cfi')
process.load('Configuration.EventContent.EventContent_cff')
process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.GeometrySimDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.Generator_cff')
process.load('GeneratorInterface.Core.genFilterSummary_cff')
process.load('Configuration.StandardSequences.SimIdeal_cff')
process.load('Configuration.StandardSequences.EndOfProcess_cff')
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')

print("[config] LHCTransport present after standard loads:", hasattr(process, 'LHCTransport'))

# Max events
process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(options.maxEvents),
    output = cms.optional.untracked.allowed(cms.int32, cms.PSet)
)

# Input source - HepMC format
process.source = cms.Source("MCFileSource",
    fileNames = cms.untracked.vstring(options.inputFiles),
    firstRun = cms.untracked.uint32(options.firstRun),
    firstLuminosityBlock = cms.untracked.uint32(options.firstLuminosityBlock),
    firstEvent = cms.untracked.uint64(options.firstEvent),
    numberEventsInLuminosityBlock = cms.untracked.uint32(
        options.numberEventsInLuminosityBlock
    ),
    firstLuminosityBlockForEachRun = cms.untracked.VLuminosityBlockID([]),
)

# Process options
process.options = cms.untracked.PSet(
    IgnoreCompletely = cms.untracked.vstring(),
    Rethrow = cms.untracked.vstring(),
    accelerators = cms.untracked.vstring('*'),
    canDeleteEarly = cms.untracked.vstring(),
    deleteNonConsumedUnscheduledModules = cms.untracked.bool(True),
    dumpOptions = cms.untracked.bool(False),
    eventSetup = cms.untracked.PSet(
        forceNumberOfConcurrentIOVs = cms.untracked.PSet(
            allowAnyLabel_=cms.required.untracked.uint32
        ),
        numberOfConcurrentIOVs = cms.untracked.uint32(0)
    ),
    fileMode = cms.untracked.string('FULLMERGE'),
    forceEventSetupCacheClearOnNewRun = cms.untracked.bool(False),
    numberOfConcurrentLuminosityBlocks = cms.untracked.uint32(0),
    numberOfConcurrentRuns = cms.untracked.uint32(1),
    numberOfStreams = cms.untracked.uint32(0),
    numberOfThreads = cms.untracked.uint32(1),
    printDependencies = cms.untracked.bool(False),
    throwIfIllegalParameter = cms.untracked.bool(True),
    wantSummary = cms.untracked.bool(False)
)

# Production info
process.configurationMetadata = cms.untracked.PSet(
    annotation = cms.untracked.string('HepMC from Pythia8 shower -> GEN-SIM'),
    name = cms.untracked.string('MC_Production'),
    version = cms.untracked.string('v1.0')
)

# Output definition
process.RAWSIMoutput = cms.OutputModule("PoolOutputModule",
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('generation_step')
    ),
    compressionAlgorithm = cms.untracked.string('LZMA'),
    compressionLevel = cms.untracked.int32(1),
    dataset = cms.untracked.PSet(
        dataTier = cms.untracked.string('GEN-SIM'),
        filterName = cms.untracked.string('')
    ),
    eventAutoFlushCompressedSize = cms.untracked.int32(20971520),
    fileName = cms.untracked.string(options.outputFile),
    outputCommands = process.RAWSIMEventContent.outputCommands,
    splitLevel = cms.untracked.int32(0)
)

# Global tag
if hasattr(process, "XMLFromDBSource"): 
    process.XMLFromDBSource.label = "Extended"
if hasattr(process, "DDDetectorESProducerFromDB"): 
    process.DDDetectorESProducerFromDB.label = "Extended"

process.genstepfilter.triggerConditions = cms.vstring("generation_step")

from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, '124X_mcRun3_2022_realistic_v12', '')

# Vertex smearing for Run3
from IOMC.EventVertexGenerators.VtxSmearedParameters_cfi import (
    Realistic25ns13p6TeVEarly2022CollisionVtxSmearingParameters, 
    VtxSmearedCommon
)

# Configure for HepMC input
# MCFileSource registers an edm::HepMCProduct with module label "source" and
# instance label "generator" (see EventContentAnalyzer debug above).
VtxSmearedCommon.src = cms.InputTag("source", "generator")

process.VtxSmeared = cms.EDProducer("BetafuncEvtVtxGenerator",
    Realistic25ns13p6TeVEarly2022CollisionVtxSmearingParameters,
    VtxSmearedCommon
)

# Path and EndPath definitions
process.generation_step = cms.Path(process.pgen)
process.simulation_step = cms.Path(process.psim)
process.genfiltersummary_step = cms.EndPath(process.genFilterSummary)
process.endjob_step = cms.EndPath(process.endOfProcess)
process.RAWSIMoutput_step = cms.EndPath(process.RAWSIMoutput)

# Schedule (can be overridden by debug mode below)
process.schedule = cms.Schedule(
    process.generation_step,
    process.genfiltersummary_step,
    process.simulation_step,
    process.endjob_step,
    process.RAWSIMoutput_step
)

# Multithreading
process.options.numberOfThreads = options.nThreads
process.options.numberOfStreams = options.nThreads

# Customization
from Configuration.DataProcessing.Utils import addMonitoring
process = addMonitoring(process)

# Keep the standard transport module from the Run3 GEN-SIM sequence.
print("[config] LHCTransport present after standard cleanup:", hasattr(process, 'LHCTransport'))

# Keep the standard GEN-SIM HepMC chain:
# source:generator -> VtxSmeared -> generatorSmeared -> genParticles / g4SimHits
if hasattr(process.g4SimHits, 'HepMCProductLabel'):
    process.g4SimHits.HepMCProductLabel = cms.InputTag('generatorSmeared')
if hasattr(process.g4SimHits.Generator, 'HepMCProductLabel'):
    process.g4SimHits.Generator.HepMCProductLabel = cms.InputTag('generatorSmeared')
if hasattr(process, 'genParticles'):
    process.genParticles.src = cms.InputTag('generatorSmeared')
if hasattr(process.g4SimHits, 'HepMCProductLabel'):
    print("[config] g4SimHits HepMCProductLabel:", process.g4SimHits.HepMCProductLabel)
if hasattr(process.g4SimHits.Generator, 'HepMCProductLabel'):
    print("[config] g4SimHits.Generator HepMCProductLabel:", process.g4SimHits.Generator.HepMCProductLabel)

# Debug: optionally dump event content only
if options.debugDump:
    if hasattr(process.g4SimHits, 'HepMCProductLabel'):
        print("[debugDump] g4SimHits HepMCProductLabel:", process.g4SimHits.HepMCProductLabel)
    if hasattr(process.g4SimHits.Generator, 'HepMCProductLabel'):
        print("[debugDump] g4SimHits.Generator HepMCProductLabel:", process.g4SimHits.Generator.HepMCProductLabel)
    print("[debugDump] g4SimHits.Generator PSets:\n", process.g4SimHits.Generator.dumpPython())
    print("[debugDump] Does g4SimHits dump reference LHCTransport?:", "LHCTransport" in process.g4SimHits.dumpPython())
    for line in process.g4SimHits.dumpPython().splitlines():
        if "LHCTransport" in line:
            print("[debugDump] g4SimHits contains:", line)
    process.dumpContent = cms.EDAnalyzer("EventContentAnalyzer")
    process.dumpContent_step = cms.Path(process.dumpContent)
    process.schedule = cms.Schedule(process.dumpContent_step)
    if hasattr(process, 'RAWSIMoutput'):
        del process.RAWSIMoutput
        if hasattr(process, 'RAWSIMoutput_step'):
            del process.RAWSIMoutput_step

# Filter efficiency summary
process.MessageLogger.cerr.FwkReport.reportEvery = 100
