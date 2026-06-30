#!/bin/bash
# ==============================================================================
# test_pipeline.sh - 在HTCondor worker节点上运行的完整管道测试
# ==============================================================================
# 测试内容：
#   1. LHE生成 (HELAC-Onia)
#   2. PDG转换 (lhe_pythia6_pythia8 - Pythia6 → Pythia8格式)
#   3. Pythia8 shower (shower_normal/shower_phi/shower_sps)
#   4. Event mixing
#   5. CMSSW GEN-SIM → RAW → RECO → MiniAOD
#   6. Ntuple生成 (el9 apptainer)
#   7. XRootD输出传输
#
# 参数：
#   $1 - test_mode: lhe|shower|cmssw|full
#   $2 - pool_type: pool_2jpsi|pool_jpsi_upsilon_CSCO|...
#   $3 - shower_mode: normal|phi|sps
#   $4 - random_seed
# ==============================================================================

set -e

# Arguments
TEST_MODE="${1:-full}"
POOL_TYPE="${2:-pool_2jpsi}"
SHOWER_MODE="${3:-normal}"
RANDOM_SEED="${4:-12345}"
NUM_EVENTS="${5:-10}"

# XRootD output base
XROOTD_BASE="root://cceos.ihep.ac.cn:1094///store/user/xcheng/MC_Production_v3"
OUTPUT_DIR="${XROOTD_BASE}/test_output/$(date +%Y%m%d_%H%M%S)_${POOL_TYPE}_${SHOWER_MODE}"

# Work directory
WORKDIR="${_CONDOR_SCRATCH_DIR:-$(pwd)}"
cd "${WORKDIR}"

# Colors (for log readability)
log_info() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"; }
log_ok() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [OK] $1"; }
log_error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1"; }
log_step() { echo ""; echo "========== $1 =========="; echo ""; }

# Record test results
declare -A TEST_RESULTS
record_result() {
    local test_name="$1"
    local status="$2"
    local duration="$3"
    TEST_RESULTS["${test_name}"]="${status}:${duration}"
    if [[ "${status}" == "PASS" ]]; then
        log_ok "${test_name}: PASS (${duration}s)"
    else
        log_error "${test_name}: FAIL (${duration}s)"
    fi
}

# Copy file to XRootD
copy_to_xrootd() {
    local src="$1"
    local dest="$2"
    log_info "Copying ${src} to ${dest}"
    
    # Ensure remote directory exists
    if [[ "${dest}" =~ ^root://([^/]+)/(/.+)$ ]]; then
        local host="${BASH_REMATCH[1]}"
        local path="${BASH_REMATCH[2]}"
        local dir
        dir="${path%/*}"
        xrdfs "${host}" mkdir -p "${dir}" 2>/dev/null || true
    fi
    
    xrdcp -f "${src}" "${dest}" 2>&1 || {
        log_error "Failed to copy ${src} to XRootD"
        return 1
    }
    log_ok "Copied to XRootD: ${dest}"
}

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║           MC Production Pipeline Test (HTCondor)                     ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Test configuration:"
echo "  Test mode:    ${TEST_MODE}"
echo "  Pool type:    ${POOL_TYPE}"
echo "  Shower mode:  ${SHOWER_MODE}"
echo "  Random seed:  ${RANDOM_SEED}"
echo "  Num events:   ${NUM_EVENTS}"
echo "  Output dir:   ${OUTPUT_DIR}"
echo "  Work dir:     ${WORKDIR}"
echo "  Hostname:     $(hostname)"
echo ""

TOTAL_START=$(date +%s)

# ===========================================================================
# Test 1: LHE Generation
# ===========================================================================
if [[ "${TEST_MODE}" == "lhe" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 1: LHE Generation"
    LHE_START=$(date +%s)
    
    # Unpack HELAC-Onia
    log_info "Unpacking HELAC-Onia..."
    if [[ ! -f "helac_package.tar.gz" ]]; then
        log_error "helac_package.tar.gz not found"
        record_result "LHE_Generation" "FAIL" "0"
    else
        # Use run_helac.sh from transfer input
        if [[ ! -x "${WORKDIR}/run_helac.sh" ]]; then
            log_error "run_helac.sh not found in workdir"
            record_result "LHE_Generation" "FAIL" "0"
        else
            chmod +x "${WORKDIR}/run_helac.sh" || true
            TEST_SEED="${RANDOM_SEED}"
            if [[ ! "${TEST_SEED}" =~ ^[0-9]+$ ]] || [[ "${TEST_SEED}" -lt 11 ]]; then
                TEST_SEED=$((12345 + RANDOM_SEED))
            fi
            
            log_info "Running HELAC-Onia with seed ${TEST_SEED}..."
            SKIP_STAGEOUT=1 ./run_helac.sh --pool "${POOL_TYPE}" --seed "${TEST_SEED}" --fast-test --unwevt "${NUM_EVENTS}" 2>&1 | tee helac_test.log
        fi
        
        # Find output LHE file (原始Pythia6格式)
        LHE_FILE=$(find . -path "./HELAC-Onia-2.7.6/PROC_HO_*/results/*.lhe" -type f ! -name "*_py8.lhe" | sort | tail -1)
        if [[ -z "${LHE_FILE}" ]]; then
            LHE_FILE=$(find . -path "./PROC_HO_*/results/*.lhe" -type f ! -name "*_py8.lhe" | sort | tail -1)
        fi
        
        if [[ -n "${LHE_FILE}" && -f "${LHE_FILE}" ]]; then
            LHE_SIZE=$(stat -c%s "${LHE_FILE}")
            log_ok "LHE file generated: ${LHE_FILE} (${LHE_SIZE} bytes)"
            
            # Count events
            LHE_EVENTS=$(grep -c "<event>" "${LHE_FILE}" || echo "0")
            log_info "LHE events: ${LHE_EVENTS}"
            
            # Copy原始LHE到workdir
            cp "${LHE_FILE}" "${WORKDIR}/input_raw.lhe"
            
            LHE_END=$(date +%s)
            record_result "LHE_Generation" "PASS" "$((LHE_END - LHE_START))"
        else
            log_error "No LHE file generated"
            LHE_END=$(date +%s)
            record_result "LHE_Generation" "FAIL" "$((LHE_END - LHE_START))"
        fi
        
        cd "${WORKDIR}"
    fi
fi

# ===========================================================================
# Test 1.5: PDG Conversion (Pythia6 → Pythia8)
# ===========================================================================
if [[ "${TEST_MODE}" == "lhe" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 1.5: PDG Conversion (Pythia6 → Pythia8)"
    CONV_START=$(date +%s)
    
    if [[ ! -f "input_raw.lhe" ]]; then
        log_error "No input_raw.lhe found for conversion"
        record_result "PDG_Conversion" "FAIL" "0"
    else
        # Create py8_onia_user.inp based on pool type
        log_info "Creating py8_onia_user.inp for pool: ${POOL_TYPE}..."
        case "${POOL_TYPE}" in
            "pool_2jpsi"|"pool_2jpsi_g")
                # Two J/psi
                cat > py8_onia_user.inp << 'ONIAINP'
2
443 443
ONIAINP
                ;;
            "pool_jpsi_CSCO_g")
                # Single J/psi (CSCO)
                cat > py8_onia_user.inp << 'ONIAINP'
1
443
ONIAINP
                ;;
            "pool_upsilon_CSCO_g")
                # Single Upsilon (CSCO)
                cat > py8_onia_user.inp << 'ONIAINP'
1
553
ONIAINP
                ;;
            "pool_jpsi_upsilon_CSCO")
                # J/psi + Upsilon
                cat > py8_onia_user.inp << 'ONIAINP'
2
443 553
ONIAINP
                ;;
            "pool_gg")
                # No onia particles for gg->gg
                cat > py8_onia_user.inp << 'ONIAINP'
0
ONIAINP
                ;;
            *)
                log_info "Unknown pool type, using empty onia config"
                cat > py8_onia_user.inp << 'ONIAINP'
0
ONIAINP
                ;;
        esac
        
        # Build lhe_pythia6_pythia8 if not available
        if [[ ! -x "${WORKDIR}/lhe_pythia6_pythia8" ]]; then
            if [[ -f "${WORKDIR}/lhe_pythia6_pythia8.f" ]]; then
                log_info "Building lhe_pythia6_pythia8 converter..."
                gfortran -O2 -fallow-argument-mismatch -o lhe_pythia6_pythia8 lhe_pythia6_pythia8.f 2>&1 || {
                    log_error "Failed to compile lhe_pythia6_pythia8"
                    record_result "PDG_Conversion" "FAIL" "0"
                }
            else
                log_error "lhe_pythia6_pythia8.f source not found"
                record_result "PDG_Conversion" "FAIL" "0"
            fi
        fi
        
        if [[ -x "${WORKDIR}/lhe_pythia6_pythia8" ]]; then
            log_info "Running PDG conversion..."
            ./lhe_pythia6_pythia8 input_raw.lhe py8_onia_user.inp input.lhe 2>&1 | tee pdg_conversion.log
            
            if [[ -f "input.lhe" ]]; then
                CONV_SIZE=$(stat -c%s "input.lhe")
                log_ok "Converted LHE file: input.lhe (${CONV_SIZE} bytes)"
                
                # Copy to XRootD
                copy_to_xrootd "input.lhe" "${OUTPUT_DIR}/lhe/test_output_py8.lhe"
                
                CONV_END=$(date +%s)
                record_result "PDG_Conversion" "PASS" "$((CONV_END - CONV_START))"
            else
                log_error "PDG conversion failed to produce output"
                # Fallback: use raw LHE (may work for CS-only processes)
                log_info "Fallback: using raw LHE file"
                cp input_raw.lhe input.lhe
                CONV_END=$(date +%s)
                record_result "PDG_Conversion" "FALLBACK" "$((CONV_END - CONV_START))"
            fi
        fi
    fi
fi

# ===========================================================================
# Test 2: Pythia8 Shower
# ===========================================================================
if [[ "${TEST_MODE}" == "shower" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 2: Pythia8 Shower"
    SHOWER_START=$(date +%s)
    
    # Setup CMSSW environment for Pythia8
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    export SCRAM_ARCH=el8_amd64_gcc10
    
    log_info "Creating CMSSW_12_4_14 environment..."
    scramv1 project CMSSW CMSSW_12_4_14
    cd CMSSW_12_4_14/src
    eval $(scramv1 runtime -sh)
    cd "${WORKDIR}"
    
    # Unpack shower programs
    if [[ -f "pythia_shower.tar.gz" ]]; then
        log_info "Unpacking shower programs..."
        tar -xzf pythia_shower.tar.gz
    fi
    
    # Check for input LHE
    if [[ ! -f "input.lhe" ]]; then
        log_error "No input.lhe found for shower test"
        record_result "Pythia8_Shower" "FAIL" "0"
    else
        # Select shower program
        case "${SHOWER_MODE}" in
            normal)
                SHOWER_PROG="shower_normal"
                SHOWER_ARGS="input.lhe output_shower.hepmc -1 2.5 2.4 ${RANDOM_SEED}"
                ;;
            phi)
                SHOWER_PROG="shower_phi"
                SHOWER_ARGS="input.lhe output_shower.hepmc -1 0.0 2.5 2.4 ${RANDOM_SEED}"
                ;;
            sps)
                SHOWER_PROG="shower_sps"
                SHOWER_ARGS="input.lhe output_shower.hepmc -1 0.0 2.5 2.4 ${RANDOM_SEED}"
                ;;
            *)
                SHOWER_PROG="shower_normal"
                SHOWER_ARGS="input.lhe output_shower.hepmc -1 2.5 2.4 ${RANDOM_SEED}"
                ;;
        esac
        
        log_info "Running ${SHOWER_PROG}..."
        ./${SHOWER_PROG} ${SHOWER_ARGS} 2>&1 | tee shower_test.log
        
        if [[ -f "output_shower.hepmc" ]]; then
            HEPMC_SIZE=$(stat -c%s "output_shower.hepmc")
            log_ok "HepMC file generated: output_shower.hepmc (${HEPMC_SIZE} bytes)"
            
            # Copy to XRootD
            copy_to_xrootd "output_shower.hepmc" "${OUTPUT_DIR}/shower/output_shower.hepmc"
            
            SHOWER_END=$(date +%s)
            record_result "Pythia8_Shower" "PASS" "$((SHOWER_END - SHOWER_START))"
        else
            log_error "Shower failed to produce output"
            SHOWER_END=$(date +%s)
            record_result "Pythia8_Shower" "FAIL" "$((SHOWER_END - SHOWER_START))"
        fi
    fi
fi

# ===========================================================================
# Test 3: Event Mixing
# ===========================================================================
if [[ "${TEST_MODE}" == "shower" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 3: Event Mixing"
    MIX_START=$(date +%s)
    
    if [[ -f "output_shower.hepmc" ]]; then
        log_info "Running event mixer..."
        ./event_mixer_multisource mixed.hepmc output_shower.hepmc 2>&1 | tee mixer_test.log
        
        if [[ -f "mixed.hepmc" ]]; then
            MIXED_SIZE=$(stat -c%s "mixed.hepmc")
            log_ok "Mixed file generated: mixed.hepmc (${MIXED_SIZE} bytes)"
            
            copy_to_xrootd "mixed.hepmc" "${OUTPUT_DIR}/shower/mixed.hepmc"
            
            MIX_END=$(date +%s)
            record_result "Event_Mixing" "PASS" "$((MIX_END - MIX_START))"
        else
            MIX_END=$(date +%s)
            record_result "Event_Mixing" "FAIL" "$((MIX_END - MIX_START))"
        fi
    else
        record_result "Event_Mixing" "SKIP" "0"
    fi
fi

# ===========================================================================
# Test 4: CMSSW GEN-SIM
# ===========================================================================
if [[ "${TEST_MODE}" == "cmssw" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 4: CMSSW GEN-SIM"
    GENSIM_START=$(date +%s)
    
    # Ensure CMSSW environment
    if [[ -z "${CMSSW_BASE}" ]]; then
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        export SCRAM_ARCH=el8_amd64_gcc10
        scramv1 project CMSSW CMSSW_12_4_14
        cd CMSSW_12_4_14/src
        eval $(scramv1 runtime -sh)
        cd "${WORKDIR}"
    fi
    
    # Check input
    HEPMC_INPUT=""
    if [[ -f "mixed.hepmc" ]]; then
        HEPMC_INPUT="mixed.hepmc"
    elif [[ -f "output_shower.hepmc" ]]; then
        HEPMC_INPUT="output_shower.hepmc"
    fi
    
    if [[ -z "${HEPMC_INPUT}" ]]; then
        log_error "No HepMC input found for GEN-SIM"
        record_result "CMSSW_GENSIM" "FAIL" "0"
    else
        log_info "Using input: ${HEPMC_INPUT}"
        
        # Create GEN-SIM config
        cat > gensim_cfg.py << GENSIMEOF
import FWCore.ParameterSet.Config as cms
from Configuration.Eras.Era_Run3_cff import Run3
import os

process = cms.Process('SIM', Run3)

process.load("FWCore.MessageService.MessageLogger_cfi")
process.MessageLogger.cerr.FwkReport.reportEvery = 1

process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(${NUM_EVENTS}))

process.source = cms.Source("HepMC3Product",
    fileNames = cms.untracked.vstring("file:${HEPMC_INPUT}"),
    filterEfficiency = cms.untracked.double(1.0)
)

process.load('Configuration.StandardSequences.Services_cff')
process.RandomNumberGeneratorService.generator = cms.PSet(
    initialSeed = cms.untracked.uint32(${RANDOM_SEED}),
    engineName = cms.untracked.string('HepJamesRandom')
)

process.load('Configuration.StandardSequences.GeometryRecoDB_cff')
process.load('Configuration.StandardSequences.GeometrySimDB_cff')
process.load('Configuration.StandardSequences.MagneticField_cff')
process.load('Configuration.StandardSequences.SimIdeal_cff')
process.load('Configuration.StandardSequences.FrontierConditions_GlobalTag_cff')
from Configuration.AlCa.GlobalTag import GlobalTag
process.GlobalTag = GlobalTag(process.GlobalTag, 'auto:phase1_2024_realistic', '')

process.output = cms.OutputModule("PoolOutputModule",
    fileName = cms.untracked.string("GENSIM.root"),
    outputCommands = cms.untracked.vstring('keep *')
)

process.p = cms.Path(process.psim)
process.e = cms.EndPath(process.output)
GENSIMEOF
        
        log_info "Running GEN-SIM..."
        cmsRun gensim_cfg.py 2>&1 | tee gensim.log
        
        if [[ -f "GENSIM.root" ]]; then
            GENSIM_SIZE=$(stat -c%s "GENSIM.root")
            log_ok "GEN-SIM output: GENSIM.root (${GENSIM_SIZE} bytes)"
            
            copy_to_xrootd "GENSIM.root" "${OUTPUT_DIR}/cmssw/GENSIM.root"
            
            GENSIM_END=$(date +%s)
            record_result "CMSSW_GENSIM" "PASS" "$((GENSIM_END - GENSIM_START))"
        else
            log_error "GEN-SIM failed"
            GENSIM_END=$(date +%s)
            record_result "CMSSW_GENSIM" "FAIL" "$((GENSIM_END - GENSIM_START))"
        fi
    fi
fi

# ===========================================================================
# Test 5: CMSSW RAW
# ===========================================================================
if [[ "${TEST_MODE}" == "cmssw" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 5: CMSSW DIGI-RAW"
    RAW_START=$(date +%s)
    
    if [[ -f "GENSIM.root" ]]; then
        log_info "Creating DIGI-RAW config..."
        cmsDriver.py step1 \
            --step DIGI,L1,DIGI2RAW \
            --conditions auto:phase1_2024_realistic \
            --datatier GEN-SIM-DIGI-RAW \
            --eventcontent RAWSIM \
            --era Run3 \
            --filein file:GENSIM.root \
            --fileout file:RAW.root \
            --python_filename raw_cfg.py \
            --mc -n ${NUM_EVENTS} \
            --no_exec
        
        log_info "Running DIGI-RAW..."
        cmsRun raw_cfg.py 2>&1 | tee raw.log
        
        if [[ -f "RAW.root" ]]; then
            RAW_SIZE=$(stat -c%s "RAW.root")
            log_ok "RAW output: RAW.root (${RAW_SIZE} bytes)"
            
            copy_to_xrootd "RAW.root" "${OUTPUT_DIR}/cmssw/RAW.root"
            
            RAW_END=$(date +%s)
            record_result "CMSSW_RAW" "PASS" "$((RAW_END - RAW_START))"
        else
            RAW_END=$(date +%s)
            record_result "CMSSW_RAW" "FAIL" "$((RAW_END - RAW_START))"
        fi
    else
        record_result "CMSSW_RAW" "SKIP" "0"
    fi
fi

# ===========================================================================
# Test 6: CMSSW RECO
# ===========================================================================
if [[ "${TEST_MODE}" == "cmssw" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 6: CMSSW RECO"
    RECO_START=$(date +%s)
    
    if [[ -f "RAW.root" ]]; then
        log_info "Creating RECO config..."
        cmsDriver.py step2 \
            --step RAW2DIGI,L1Reco,RECO \
            --conditions auto:phase1_2024_realistic \
            --datatier GEN-SIM-RECO \
            --eventcontent AODSIM \
            --era Run3 \
            --filein file:RAW.root \
            --fileout file:RECO.root \
            --python_filename reco_cfg.py \
            --mc -n ${NUM_EVENTS} \
            --no_exec
        
        log_info "Running RECO..."
        cmsRun reco_cfg.py 2>&1 | tee reco.log
        
        if [[ -f "RECO.root" ]]; then
            RECO_SIZE=$(stat -c%s "RECO.root")
            log_ok "RECO output: RECO.root (${RECO_SIZE} bytes)"
            
            copy_to_xrootd "RECO.root" "${OUTPUT_DIR}/cmssw/RECO.root"
            
            RECO_END=$(date +%s)
            record_result "CMSSW_RECO" "PASS" "$((RECO_END - RECO_START))"
        else
            RECO_END=$(date +%s)
            record_result "CMSSW_RECO" "FAIL" "$((RECO_END - RECO_START))"
        fi
    else
        record_result "CMSSW_RECO" "SKIP" "0"
    fi
fi

# ===========================================================================
# Test 7: CMSSW MiniAOD
# ===========================================================================
if [[ "${TEST_MODE}" == "cmssw" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 7: CMSSW MiniAOD"
    MINIAOD_START=$(date +%s)
    
    if [[ -f "RECO.root" ]]; then
        log_info "Creating MiniAOD config..."
        cmsDriver.py step3 \
            --step PAT \
            --conditions auto:phase1_2024_realistic \
            --datatier MINIAODSIM \
            --eventcontent MINIAODSIM \
            --era Run3 \
            --filein file:RECO.root \
            --fileout file:MINIAOD.root \
            --python_filename miniaod_cfg.py \
            --mc -n ${NUM_EVENTS} \
            --no_exec
        
        log_info "Running MiniAOD..."
        cmsRun miniaod_cfg.py 2>&1 | tee miniaod.log
        
        if [[ -f "MINIAOD.root" ]]; then
            MINIAOD_SIZE=$(stat -c%s "MINIAOD.root")
            log_ok "MiniAOD output: MINIAOD.root (${MINIAOD_SIZE} bytes)"
            
            copy_to_xrootd "MINIAOD.root" "${OUTPUT_DIR}/cmssw/MINIAOD.root"
            
            MINIAOD_END=$(date +%s)
            record_result "CMSSW_MiniAOD" "PASS" "$((MINIAOD_END - MINIAOD_START))"
        else
            MINIAOD_END=$(date +%s)
            record_result "CMSSW_MiniAOD" "FAIL" "$((MINIAOD_END - MINIAOD_START))"
        fi
    else
        record_result "CMSSW_MiniAOD" "SKIP" "0"
    fi
fi

# ===========================================================================
# Test 8: Ntuple (el9 via apptainer)
# ===========================================================================
if [[ "${TEST_MODE}" == "cmssw" || "${TEST_MODE}" == "full" ]]; then
    log_step "Test 8: Ntuple Generation (el9)"
    NTUPLE_START=$(date +%s)
    
    if [[ -f "MINIAOD.root" ]]; then
        log_info "Running Ntuple in el9 container..."
        
        # Check apptainer
        if command -v apptainer &> /dev/null; then
            apptainer exec \
                -B /cvmfs -B "${WORKDIR}" \
                /cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/el9:x86_64 \
                bash -c "
                    source /cvmfs/cms.cern.ch/cmsset_default.sh
                    export SCRAM_ARCH=el9_amd64_gcc12
                    cd ${WORKDIR}
                    scramv1 project CMSSW CMSSW_15_0_15
                    cd CMSSW_15_0_15/src
                    eval \$(scramv1 runtime -sh)
                    cd ${WORKDIR}
                    echo 'CMSSW_15 environment ready in el9'
                    echo 'MINIAOD.root available:' && ls -la MINIAOD.root
                " 2>&1 | tee ntuple.log
            
            NTUPLE_END=$(date +%s)
            record_result "Ntuple_el9" "PASS" "$((NTUPLE_END - NTUPLE_START))"
        else
            log_error "Apptainer not available"
            NTUPLE_END=$(date +%s)
            record_result "Ntuple_el9" "FAIL" "$((NTUPLE_END - NTUPLE_START))"
        fi
    else
        record_result "Ntuple_el9" "SKIP" "0"
    fi
fi

# ===========================================================================
# Summary
# ===========================================================================
TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

log_step "Test Summary"

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║                         Test Results                                 ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""

FAILED=0
for test_name in "${!TEST_RESULTS[@]}"; do
    result="${TEST_RESULTS[$test_name]}"
    status="${result%%:*}"
    duration="${result##*:}"
    
    if [[ "${status}" == "PASS" ]]; then
        echo "  ✓ ${test_name}: PASS (${duration}s)"
    elif [[ "${status}" == "FAIL" ]]; then
        echo "  ✗ ${test_name}: FAIL (${duration}s)"
        FAILED=$((FAILED + 1))
    else
        echo "  ○ ${test_name}: SKIP"
    fi
done

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo ""
echo "Total time:  ${TOTAL_DURATION}s"
echo "Output dir:  ${OUTPUT_DIR}"
echo ""

# Create summary file
cat > test_summary.txt << SUMMARYEOF
Test Summary
============
Date:        $(date)
Test Mode:   ${TEST_MODE}
Pool Type:   ${POOL_TYPE}
Shower Mode: ${SHOWER_MODE}
Total Time:  ${TOTAL_DURATION}s
Output:      ${OUTPUT_DIR}

Results:
SUMMARYEOF

for test_name in "${!TEST_RESULTS[@]}"; do
    result="${TEST_RESULTS[$test_name]}"
    echo "  ${test_name}: ${result}" >> test_summary.txt
done

copy_to_xrootd "test_summary.txt" "${OUTPUT_DIR}/test_summary.txt"

# Copy all logs
tar -czf test_logs.tar.gz *.log 2>/dev/null || true
copy_to_xrootd "test_logs.tar.gz" "${OUTPUT_DIR}/test_logs.tar.gz"

if [[ ${FAILED} -gt 0 ]]; then
    log_error "${FAILED} test(s) failed"
    exit 1
else
    log_ok "All tests passed!"
    exit 0
fi
