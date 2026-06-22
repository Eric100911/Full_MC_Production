// ==============================================================================
// shower_sps.cc - SPS (Single Parton Scattering) Pythia8 shower processing
// ==============================================================================
// Specialized for JJP_SPS and JUP_SPS processes where:
// - MPI (Multiple Parton Interactions) is turned OFF
// - Phi meson enrichment through hadronization retry
// - Supports both Color Singlet and Color Octet processes
//
// Key differences from shower_phi.cc:
// - MPI disabled (PartonLevel:MPI = off) for clean SPS topology
// - Designed for 2-body production processes (2J/psi, J/psi+Upsilon)
// - OniaShower enabled for color octet handling
// - 只要求 hadronized 末态中出现 phi，不做额外 muon acceptance 过滤
//
// Compilation (in CMSSW environment):
//   g++ -std=c++17 -O2 shower_sps.cc -o shower_sps \
//       $(pythia8-config --cxxflags --libs) \
//       -I$HEPMC3/include -L$HEPMC3/lib64 -lHepMC3
//
// Usage:
//   ./shower_sps input.lhe output.hepmc [nEvents] [minPhiPt] [minMuonPt] [maxMuonEta] [maxRetry] [rngSeed]
// ==============================================================================

#include "Pythia8/Pythia.h"
#include "Pythia8Plugins/HepMC3.h"

#include <algorithm>
#include <iostream>
#include <string>

using namespace Pythia8;
using namespace std;

// Check for phi meson satisfying pT requirement
// Note: phi meson typically decays immediately, so status is negative (-83, -84)
bool hasPhiMeson(Event& event, double minPt = 0.0) {
    for (int i = 0; i < event.size(); ++i) {
        int pid = abs(event[i].id());
        if (pid == 333) { // phi meson
            int status = event[i].status();
            // phi usually has decayed (status < 0) or is final state
            if ((status < 0) || event[i].isFinal()) {
                if (event[i].pT() > minPt) {
                    return true;
                }
            }
        }
    }
    return false;
}

// 以下函数保留作调试用途；SPS phi 模式本身只要求 phi 条件。
bool hasValidJpsiMuons(Event& event, double minPt = 2.5, double maxEta = 2.4) {
    for (int i = 0; i < event.size(); ++i) {
        if (abs(event[i].id()) != 443) continue;
        
        int status = event[i].status();
        if (status >= 0 && !event[i].isFinal()) continue;
        
        int d1 = event[i].daughter1();
        int d2 = event[i].daughter2();
        
        if (d1 <= 0 || d2 <= 0) continue;
        
        bool foundMuPlus = false, foundMuMinus = false;
        bool muPlusValid = false, muMinusValid = false;
        
        for (int j = d1; j <= d2; ++j) {
            int pdgid = event[j].id();
            if (pdgid == 13) { // mu-
                foundMuMinus = true;
                if (event[j].pT() > minPt && abs(event[j].eta()) < maxEta) {
                    muMinusValid = true;
                }
            } else if (pdgid == -13) { // mu+
                foundMuPlus = true;
                if (event[j].pT() > minPt && abs(event[j].eta()) < maxEta) {
                    muPlusValid = true;
                }
            }
        }
        
        if (foundMuPlus && foundMuMinus && muPlusValid && muMinusValid) {
            return true;
        }
    }
    return false;
}

// Check if Upsilon decay muons satisfy kinematic requirements
bool hasValidUpsilonMuons(Event& event, double minPt = 2.5, double maxEta = 2.4) {
    for (int i = 0; i < event.size(); ++i) {
        int pid = abs(event[i].id());
        // Upsilon(1S)=553, Upsilon(2S)=100553, Upsilon(3S)=200553
        if (pid != 553 && pid != 100553 && pid != 200553) continue;
        
        int status = event[i].status();
        if (status >= 0 && !event[i].isFinal()) continue;
        
        int d1 = event[i].daughter1();
        int d2 = event[i].daughter2();
        
        if (d1 <= 0 || d2 <= 0) continue;
        
        bool foundMuPlus = false, foundMuMinus = false;
        bool muPlusValid = false, muMinusValid = false;
        
        for (int j = d1; j <= d2; ++j) {
            int pdgid = event[j].id();
            if (pdgid == 13) { // mu-
                foundMuMinus = true;
                if (event[j].pT() > minPt && abs(event[j].eta()) < maxEta) {
                    muMinusValid = true;
                }
            } else if (pdgid == -13) { // mu+
                foundMuPlus = true;
                if (event[j].pT() > minPt && abs(event[j].eta()) < maxEta) {
                    muPlusValid = true;
                }
            }
        }
        
        if (foundMuPlus && foundMuMinus && muPlusValid && muMinusValid) {
            return true;
        }
    }
    return false;
}

// Count particles for statistics
void countParticles(Event& event, int& nJpsi, int& nUpsilon, int& nPhi, int& nMuon) {
    nJpsi = 0;
    nUpsilon = 0;
    nPhi = 0;
    nMuon = 0;
    
    for (int i = 0; i < event.size(); ++i) {
        int pid = abs(event[i].id());
        int status = event[i].status();
        
        if ((status < 0) || event[i].isFinal()) {
            if (pid == 443) nJpsi++;
            else if (pid == 553 || pid == 100553 || pid == 200553) nUpsilon++;
            else if (pid == 333) nPhi++;
            else if (pid == 13) nMuon++;
        }
    }
}

int main(int argc, char* argv[]) {
    
    if (argc < 3) {
        cerr << "\n====== SPS Phi-Enriched Shower Processing ======" << endl;
        cerr << "Specialized for Single Parton Scattering (SPS) processes" << endl;
        cerr << "MPI is DISABLED for clean SPS event topology" << endl;
        cerr << "\nUsage: " << argv[0] << " input.lhe output.hepmc [nEvents] [minPhiPt] [minMuonPt] [maxMuonEta] [maxRetry] [rngSeed]" << endl;
        cerr << "\nArguments:" << endl;
        cerr << "  input.lhe   : Input LHE file from HELAC-Onia" << endl;
        cerr << "  output.hepmc: Output HepMC file" << endl;
        cerr << "  nEvents     : Number of events to process (default: -1, all)" << endl;
        cerr << "  minPhiPt    : Minimum phi pT in GeV (default: 0)" << endl;
        cerr << "  minMuonPt   : Minimum muon pT in GeV (default: 2.5)" << endl;
        cerr << "  maxMuonEta  : Maximum muon |eta| (default: 2.4)" << endl;
        cerr << "  maxRetry    : Maximum hadronization retries (default: 5000)" << endl;
        cerr << "  rngSeed     : Optional Pythia RNG seed" << endl;
        cerr << "\nExample:" << endl;
        cerr << "  ./shower_sps 2jpsi.lhe jjp_sps.hepmc 1000 3.0 2.5 2.4 1000" << endl;
        return 1;
    }
    
    string inputFile = argv[1];
    string outputFile = argv[2];
    int nEvents = (argc > 3) ? atoi(argv[3]) : -1;
    double minPhiPt = (argc > 4) ? atof(argv[4]) : 0.0;
    double minMuonPt = (argc > 5) ? atof(argv[5]) : 2.5;
    double maxMuonEta = (argc > 6) ? atof(argv[6]) : 2.4;
    int maxRetry = (argc > 7) ? atoi(argv[7]) : 5000;
    int rngSeed = (argc > 8) ? atoi(argv[8]) : 0;
    
    cout << "\n====== SPS Phi-Enriched Shower Processing ======" << endl;
    cout << "Mode:         SPS (MPI disabled)" << endl;
    cout << "Input LHE:    " << inputFile << endl;
    cout << "Output HepMC: " << outputFile << endl;
    cout << "Events:       " << (nEvents > 0 ? to_string(nEvents) : "all") << endl;
    cout << "Min phi pT:   " << minPhiPt << " GeV" << endl;
    cout << "Min muon pT:  " << minMuonPt << " GeV (legacy arg, SPS phi 模式不做筛选)" << endl;
    cout << "Max muon eta: " << maxMuonEta << " (legacy arg, SPS phi 模式不做筛选)" << endl;
    cout << "Max retries:  " << maxRetry << endl;
    cout << "RNG seed:     " << (rngSeed > 0 ? to_string(rngSeed) : "Pythia default") << endl;
    cout << "================================================\n" << endl;
    
    // Initialize Pythia
    Pythia pythia;

    if (rngSeed > 0) {
        pythia.readString("Random:setSeed = on");
        pythia.readString("Random:seed = " + to_string(rngSeed));
    }

    auto setFlagIfExists = [&](const string& name, bool value) {
        if (pythia.settings.isFlag(name)) {
            pythia.readString(name + " = " + string(value ? "on" : "off"));
        } else {
            cerr << "[WARN] Pythia setting not found (flag): " << name << endl;
        }
    };
    auto setModeIfExists = [&](const string& name, int value) {
        if (pythia.settings.isMode(name)) {
            pythia.readString(name + " = " + to_string(value));
        } else {
            cerr << "[WARN] Pythia setting not found (mode): " << name << endl;
        }
    };
    auto setParmIfExists = [&](const string& name, double value) {
        if (pythia.settings.isParm(name)) {
            pythia.readString(name + " = " + to_string(value));
        } else {
            cerr << "[WARN] Pythia setting not found (parm): " << name << endl;
        }
    };
    auto readSetting = [&](const string& setting) {
        if (!pythia.readString(setting)) {
            cerr << "[WARN] Failed to apply Pythia setting: " << setting << endl;
        }
    };
    
    // LHE 输入与通用 Run 3 CP5 配置
    pythia.readString("Beams:frameType = 4"); // Read from LHEF
    pythia.readString("Beams:LHEF = " + inputFile);
    pythia.readString("Beams:eCM = 13600."); // 13.6 TeV Run3
    setModeIfExists("Tune:preferLHAPDF", 2);
    readSetting("Main:timesAllowErrors = 10000");
    setParmIfExists("Check:epTolErr", 0.01);
    setParmIfExists("SLHA:minMassSM", 1000.);
    readSetting("ParticleDecays:limitTau0 = on");
    readSetting("ParticleDecays:tau0Max = 10");
    setFlagIfExists("HadronLevel:QED", true);
    setFlagIfExists("Beams:setProductionScalesFromLHEF", false);
    readSetting("SpaceShower:pTmaxMatch = 1");
    readSetting("SpaceShower:pTmaxFudge = 1");
    readSetting("SpaceShower:MEcorrections = off");
    readSetting("TimeShower:pTmaxMatch = 1");
    readSetting("TimeShower:pTmaxFudge = 1");
    readSetting("TimeShower:MEcorrections = off");
    readSetting("TimeShower:globalRecoil = on");
    readSetting("TimeShower:limitPTmaxGlobal = on");
    readSetting("TimeShower:nMaxGlobalRecoil = 1");
    readSetting("TimeShower:globalRecoilMode = 2");
    readSetting("TimeShower:nMaxGlobalBranch = 1");
    readSetting("TimeShower:weightGluonToQuark = 1");
    readSetting("UncertaintyBands:doVariations = on");
    readSetting(
        "UncertaintyBands:List = {"
        "isrRedHi isr:muRfac=0.707,fsrRedHi fsr:muRfac=0.707,isrRedLo isr:muRfac=1.414,fsrRedLo fsr:muRfac=1.414,"
        "isrDefHi isr:muRfac=0.5,fsrDefHi fsr:muRfac=0.5,isrDefLo isr:muRfac=2.0,fsrDefLo fsr:muRfac=2.0,"
        "isrConHi isr:muRfac=0.25,fsrConHi fsr:muRfac=0.25,isrConLo isr:muRfac=4.0,fsrConLo fsr:muRfac=4.0,"
        "fsr_G2GG_muR_dn fsr:G2GG:muRfac=0.5,"
        "fsr_G2GG_muR_up fsr:G2GG:muRfac=2.0,"
        "fsr_G2QQ_muR_dn fsr:G2QQ:muRfac=0.5,"
        "fsr_G2QQ_muR_up fsr:G2QQ:muRfac=2.0,"
        "fsr_Q2QG_muR_dn fsr:Q2QG:muRfac=0.5,"
        "fsr_Q2QG_muR_up fsr:Q2QG:muRfac=2.0,"
        "fsr_X2XG_muR_dn fsr:X2XG:muRfac=0.5,"
        "fsr_X2XG_muR_up fsr:X2XG:muRfac=2.0,"
        "fsr_G2GG_cNS_dn fsr:G2GG:cNS=-2.0,"
        "fsr_G2GG_cNS_up fsr:G2GG:cNS=2.0,"
        "fsr_G2QQ_cNS_dn fsr:G2QQ:cNS=-2.0,"
        "fsr_G2QQ_cNS_up fsr:G2QQ:cNS=2.0,"
        "fsr_Q2QG_cNS_dn fsr:Q2QG:cNS=-2.0,"
        "fsr_Q2QG_cNS_up fsr:Q2QG:cNS=2.0,"
        "fsr_X2XG_cNS_dn fsr:X2XG:cNS=-2.0,"
        "fsr_X2XG_cNS_up fsr:X2XG:cNS=2.0,"
        "isr_G2GG_muR_dn isr:G2GG:muRfac=0.5,"
        "isr_G2GG_muR_up isr:G2GG:muRfac=2.0,"
        "isr_G2QQ_muR_dn isr:G2QQ:muRfac=0.5,"
        "isr_G2QQ_muR_up isr:G2QQ:muRfac=2.0,"
        "isr_Q2QG_muR_dn isr:Q2QG:muRfac=0.5,"
        "isr_Q2QG_muR_up isr:Q2QG:muRfac=2.0,"
        "isr_X2XG_muR_dn isr:X2XG:muRfac=0.5,"
        "isr_X2XG_muR_up isr:X2XG:muRfac=2.0,"
        "isr_G2GG_cNS_dn isr:G2GG:cNS=-2.0,"
        "isr_G2GG_cNS_up isr:G2GG:cNS=2.0,"
        "isr_G2QQ_cNS_dn isr:G2QQ:cNS=-2.0,"
        "isr_G2QQ_cNS_up isr:G2QQ:cNS=2.0,"
        "isr_Q2QG_cNS_dn isr:Q2QG:cNS=-2.0,"
        "isr_Q2QG_cNS_up isr:Q2QG:cNS=2.0,"
        "isr_X2XG_cNS_dn isr:X2XG:cNS=-2.0,"
        "isr_X2XG_cNS_up isr:X2XG:cNS=2.0}"
    );
    readSetting("UncertaintyBands:nFlavQ = 4");
    readSetting("UncertaintyBands:MPIshowers = on");
    readSetting("UncertaintyBands:overSampleFSR = 10.0");
    readSetting("UncertaintyBands:overSampleISR = 10.0");
    readSetting("UncertaintyBands:FSRpTmin2Fac = 20");
    readSetting("UncertaintyBands:ISRpTmin2Fac = 20");

    // Onia settings (guarded by availability in the installed Pythia version)
    // These are critical for color octet handling
    setParmIfExists("Onia:massSplit", 0.2);
    setFlagIfExists("Onia:forceMassSplit", true);
    setModeIfExists("OniaShower:octetSplit", 1);
    
    // ==========================================================================
    // SPS-SPECIFIC SETTINGS: MPI is turned OFF
    // ==========================================================================
    // For SPS processes, we want clean parton shower without additional
    // multiple parton interactions that would add underlying event activity
    // not originating from the hard process.
    // ==========================================================================
    pythia.readString("PartonLevel:ISR = on");   // Initial State Radiation
    pythia.readString("PartonLevel:FSR = on");   // Final State Radiation
    pythia.readString("PartonLevel:MPI = off");  // Multiple Parton Interactions OFF
    
    // Disable automatic hadronization for retry mechanism
    pythia.readString("HadronLevel:all = off");
    
    // Tune settings (some MPI-related settings are kept for consistency but won't be used)
    pythia.readString("Tune:pp = 14");
    pythia.readString("Tune:ee = 7");
    // MPI-related settings (kept for potential future use but inactive due to MPI=off)
    pythia.readString("MultipartonInteractions:ecmPow = 0.03344");
    pythia.readString("MultipartonInteractions:bProfile = 2");
    pythia.readString("MultipartonInteractions:pT0Ref = 1.41");
    pythia.readString("MultipartonInteractions:coreRadius = 0.7634");
    pythia.readString("MultipartonInteractions:coreFraction = 0.63");
    pythia.readString("ColourReconnection:range = 5.176");
    pythia.readString("SigmaTotal:zeroAXB = off");
    pythia.readString("SpaceShower:alphaSorder = 2");
    pythia.readString("SpaceShower:alphaSvalue = 0.118");
    pythia.readString("SigmaProcess:alphaSvalue = 0.118");
    pythia.readString("SigmaProcess:alphaSorder = 2");
    pythia.readString("MultipartonInteractions:alphaSvalue = 0.118");
    pythia.readString("MultipartonInteractions:alphaSorder = 2");
    pythia.readString("TimeShower:alphaSorder = 2");
    pythia.readString("TimeShower:alphaSvalue = 0.118");
    pythia.readString("SigmaTotal:mode = 0");
    pythia.readString("SigmaTotal:sigmaEl = 22.08");
    pythia.readString("SigmaTotal:sigmaTot = 101.037");
    pythia.readString("PDF:pSet = LHAPDF6:NNPDF31_nnlo_as_0118");

    // Enhanced strange quark production for phi enrichment
    pythia.readString("StringFlav:probStoUD = 0.30");  // default 0.217
    pythia.readString("StringFlav:mesonUDvector = 0.60");  // enhance vector mesons
    pythia.readString("StringFlav:mesonSvector = 0.60");
    
    // Force J/psi -> mu+ mu-
    pythia.readString("443:onMode = off");
    pythia.readString("443:onIfMatch = 13 -13");
    
    // Force phi -> K+ K-
    pythia.readString("333:onMode = off");
    pythia.readString("333:onIfMatch = 321 -321");
    
    // Force Upsilon(1S) -> mu+ mu-
    pythia.readString("553:onMode = off");
    pythia.readString("553:onIfMatch = 13 -13");
    
    // Initialize
    if (!pythia.init()) {
        cerr << "Pythia initialization failed!" << endl;
        return 1;
    }
    
    // HepMC3 output
    Pythia8::Pythia8ToHepMC toHepMC(outputFile);
    
    // Statistics
    int iEvent = 0;
    int iAbort = 0;
    int maxAbort = 10;
    int totalRetries = 0;
    int successWithPhi = 0;
    int failedToFindPhi = 0;
    
    // Particle counts
    int totalJpsi = 0, totalUpsilon = 0, totalPhi = 0, totalMuon = 0;
    
    cout << "Starting SPS event processing (MPI disabled)..." << endl;
    
    while (true) {
        if (nEvents > 0 && iEvent >= nEvents) break;
        
        // Run parton level (without hadronization, no MPI)
        if (!pythia.next()) {
            if (pythia.info.atEndOfFile()) {
                cout << "Reached end of LHE file." << endl;
                break;
            }
            if (++iAbort < maxAbort) continue;
            cout << "Event generation aborted prematurely!" << endl;
            break;
        }
        
        // Save parton level state after ISR/FSR shower (before hadronization)
        Event savedEvent = pythia.event;
        PartonSystems savedPartonSystems = pythia.partonSystems;
        
        // Try multiple hadronizations until phi + muon requirements are met
        bool foundValid = false;
        int nRetry = 0;
        
        for (nRetry = 0; nRetry < maxRetry; ++nRetry) {
            // Restore parton level state
            pythia.event = savedEvent;
            pythia.partonSystems = savedPartonSystems;
            
            // Perform hadronization
            if (!pythia.forceHadronLevel()) {
                continue;
            }
            
            bool hasPhi = hasPhiMeson(pythia.event, minPhiPt);

            if (hasPhi) {
                foundValid = true;
                break;
            }
        }
        
        totalRetries += nRetry + 1;
        
        if (foundValid) {
            successWithPhi++;
            
            // Count particles
            int nJpsi, nUpsilon, nPhi, nMuon;
            countParticles(pythia.event, nJpsi, nUpsilon, nPhi, nMuon);
            totalJpsi += nJpsi;
            totalUpsilon += nUpsilon;
            totalPhi += nPhi;
            totalMuon += nMuon;
            
            // Write to HepMC
            toHepMC.writeNextEvent(pythia);
        } else {
            failedToFindPhi++;
        }
        
        ++iEvent;
        if (iEvent % 100 == 0) {
            double efficiency = 100.0 * successWithPhi / iEvent;
            double avgRetry = (double)totalRetries / iEvent;
            cout << "Processed " << iEvent << " events, "
                 << "phi efficiency: " << efficiency << "%, "
                 << "avg retries: " << avgRetry << endl;
        }
    }
    
    pythia.stat();
    
    cout << "\n======================================================" << endl;
    cout << "SPS Phi-Enriched Processing Summary:" << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Mode: SPS (MPI disabled)" << endl;
    cout << "Selection criteria:" << endl;
    cout << "  Phi pT > " << minPhiPt << " GeV" << endl;
    cout << "  Accept event once a phi meson is present after hadronization" << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Total LHE events processed:   " << iEvent << endl;
    cout << "Events written (all cuts):    " << successWithPhi 
         << " (" << 100.0*successWithPhi/max(1,iEvent) << "%)" << endl;
    cout << "Events skipped (failed cuts): " << failedToFindPhi << endl;
    cout << "Total hadronization tries:    " << totalRetries << endl;
    cout << "Average retries per event:    " << (double)totalRetries/max(1,iEvent) << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Particle counts (in written events):" << endl;
    cout << "  Total J/psi:   " << totalJpsi << endl;
    cout << "  Total Upsilon: " << totalUpsilon << endl;
    cout << "  Total phi:     " << totalPhi << endl;
    cout << "  Total muons:   " << totalMuon << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Output events: " << successWithPhi << endl;
    cout << "Output file:   " << outputFile << endl;
    cout << "======================================================" << endl;
    
    return 0;
}
