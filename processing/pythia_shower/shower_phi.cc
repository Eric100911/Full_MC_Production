// ==============================================================================
// shower_phi.cc - Phi-enriched Pythia8 shower processing
// ==============================================================================
// Performs parton shower + hadronization with phi meson enrichment.
// Uses Pythia8 save/restore mechanism to retry hadronization until
// a phi meson is found in the event.
//
// Key features:
// - Enriched strange quark production to enhance phi yield
// - Multiple hadronization retries to find events with phi mesons
// - Kinematic filtering for both phi and J/psi decay products
//
// Compilation (in CMSSW environment):
//   g++ -std=c++17 -O2 shower_phi.cc -o shower_phi \
//       $(pythia8-config --cxxflags --libs) \
//       -I$HEPMC3/include -L$HEPMC3/lib64 -lHepMC3
//
// Usage:
//   ./shower_phi input.lhe output.hepmc [nEvents] [minPhiPt] [minMuonPt] [maxMuonEta] [maxRetry] [rngSeed]
// ==============================================================================

#include "Pythia8/Pythia.h"
#include "Pythia8Plugins/HepMC3.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_set>

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

bool phiHasAncestorHardGluon(const Event& event, int idx, unordered_set<int>& visited) {
    if (idx <= 0 || idx >= event.size()) return false;
    if (!visited.insert(idx).second) return false;

    const Particle& particle = event[idx];
    int mother1 = particle.mother1();
    int mother2 = particle.mother2();
    if (mother1 <= 0 && mother2 <= 0) return false;

    for (int motherIdx : {mother1, mother2}) {
        if (motherIdx <= 0 || motherIdx >= event.size()) continue;
        const Particle& mother = event[motherIdx];
        if (mother.id() == 21 && mother.statusAbs() >= 21 && mother.statusAbs() < 30) {
            return true;
        }
        if (phiHasAncestorHardGluon(event, motherIdx, visited)) {
            return true;
        }
    }
    return false;
}

bool hasPhiFromHardGluon(Event& event, double minPt = 0.0) {
    for (int i = 0; i < event.size(); ++i) {
        if (abs(event[i].id()) != 333) continue;
        int status = event[i].status();
        if (!((status < 0) || event[i].isFinal())) continue;
        if (event[i].pT() <= minPt) continue;
        unordered_set<int> visited;
        if (phiHasAncestorHardGluon(event, i, visited)) {
            return true;
        }
    }
    return false;
}

// 以下函数保留作调试用途；phi enrichment 本身只要求 phi 条件。
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
            if (pdgid == 13) {
                foundMuMinus = true;
                if (event[j].pT() > minPt && abs(event[j].eta()) < maxEta) {
                    muMinusValid = true;
                }
            } else if (pdgid == -13) {
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
        cerr << "\n====== Phi-Enriched Shower Processing ======" << endl;
        cerr << "Usage: " << argv[0] << " input.lhe output.hepmc [nEvents] [minPhiPt] [minMuonPt] [maxMuonEta] [maxRetry] [requireLheGluon] [rngSeed]" << endl;
        cerr << "\nArguments:" << endl;
        cerr << "  input.lhe   : Input LHE file from HELAC-Onia" << endl;
        cerr << "  output.hepmc: Output HepMC file" << endl;
        cerr << "  nEvents     : Number of events to process (default: -1, all)" << endl;
        cerr << "  minPhiPt    : Minimum phi pT in GeV (default: 0)" << endl;
        cerr << "  minMuonPt   : Minimum muon pT in GeV (default: 2.5)" << endl;
        cerr << "  maxMuonEta  : Maximum muon |eta| (default: 2.4)" << endl;
        cerr << "  maxRetry    : Maximum hadronization retries (default: 5000)" << endl;
        cerr << "  requireLheGluon : 1/true 表示要求 phi 可追溯到 LHE 硬过程胶子" << endl;
        cerr << "  rngSeed     : Optional Pythia RNG seed" << endl;
        cerr << "\nExample:" << endl;
        cerr << "  ./shower_phi jpsi_jpsi.lhe phi_enriched.hepmc 1000 3.0 2.5 2.4 1000" << endl;
        return 1;
    }
    
    string inputFile = argv[1];
    string outputFile = argv[2];
    int nEvents = (argc > 3) ? atoi(argv[3]) : -1;
    double minPhiPt = (argc > 4) ? atof(argv[4]) : 0.0;
    double minMuonPt = (argc > 5) ? atof(argv[5]) : 2.5;
    double maxMuonEta = (argc > 6) ? atof(argv[6]) : 2.4;
    int maxRetry = (argc > 7) ? atoi(argv[7]) : 5000;
    bool requireLheGluon = false;
    if (argc > 8) {
        string modeArg = argv[8];
        requireLheGluon = (modeArg == "1" || modeArg == "true" || modeArg == "lhegluon");
    }
    int rngSeed = (argc > 9) ? atoi(argv[9]) : 0;
    int targetOutputEvents = (argc > 10) ? atoi(argv[10]) : nEvents;
    string manifestFile = (argc > 11) ? argv[11] : "";
    
    cout << "\n====== Phi-Enriched Shower Processing ======" << endl;
    cout << "Input LHE:    " << inputFile << endl;
    cout << "Output HepMC: " << outputFile << endl;
    cout << "Events:       " << (nEvents > 0 ? to_string(nEvents) : "all") << endl;
    cout << "Target output:" << (targetOutputEvents > 0 ? to_string(targetOutputEvents) : "all") << endl;
    cout << "Min phi pT:   " << minPhiPt << " GeV" << endl;
    cout << "Min muon pT:  " << minMuonPt << " GeV (legacy arg, phi 模式不做筛选)" << endl;
    cout << "Max muon eta: " << maxMuonEta << " (legacy arg, phi 模式不做筛选)" << endl;
    cout << "Max retries:  " << maxRetry << endl;
    cout << "Require LHE gluon ancestry: " << (requireLheGluon ? "yes" : "no") << endl;
    cout << "RNG seed:     " << (rngSeed > 0 ? to_string(rngSeed) : "Pythia default") << endl;
    if (!manifestFile.empty()) {
        cout << "Manifest:     " << manifestFile << endl;
    }
    cout << "=============================================\n" << endl;
    
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
    setParmIfExists("Onia:massSplit", 0.2);
    setFlagIfExists("Onia:forceMassSplit", true);
    setModeIfExists("OniaShower:octetSplit", 1);
    
    // Parton shower settings
    pythia.readString("PartonLevel:ISR = on");
    pythia.readString("PartonLevel:FSR = on");
    pythia.readString("PartonLevel:MPI = on");
    
    // Disable automatic hadronization for retry mechanism
    pythia.readString("HadronLevel:all = off");
    
    // Tune settings
    pythia.readString("Tune:pp = 14");
    pythia.readString("Tune:ee = 7");
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

    // Relax event checks for HELAC-Onia LHE color flow
    // pythia.readString("Check:event = off");

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
    int successfulPythiaEvents = 0;
    auto wallStart = chrono::steady_clock::now();
    
    // Particle counts
    int totalJpsi = 0, totalUpsilon = 0, totalPhi = 0, totalMuon = 0;
    
    cout << "Starting event processing..." << endl;
    
    while (true) {
        if (targetOutputEvents > 0 && successWithPhi >= targetOutputEvents) break;
        if (nEvents > 0 && iEvent >= nEvents) break;
        
        // Run parton level (without hadronization)
        if (!pythia.next()) {
            if (pythia.info.atEndOfFile()) {
                cout << "Reached end of LHE file." << endl;
                break;
            }
            if (++iAbort < maxAbort) continue;
            cout << "Event generation aborted prematurely!" << endl;
            break;
        }
        successfulPythiaEvents++;
        
        // Save parton level state
        Event savedEvent = pythia.event;
        PartonSystems savedPartonSystems = pythia.partonSystems;
        
        // Try multiple hadronizations until phi + muon requirements are met
        bool foundValid = false;
        int nRetry = 0;
        
        for (nRetry = 0; nRetry < maxRetry; ++nRetry) {
            pythia.event = savedEvent;
            pythia.partonSystems = savedPartonSystems;
            
            if (!pythia.forceHadronLevel()) {
                continue;
            }
            
            bool hasExpectedPhi = requireLheGluon
                ? hasPhiFromHardGluon(pythia.event, minPhiPt)
                : hasPhiMeson(pythia.event, minPhiPt);

            if (hasExpectedPhi) {
                foundValid = true;
                break;
            }
        }
        
        totalRetries += min(nRetry + 1, maxRetry);
        
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
    auto wallEnd = chrono::steady_clock::now();
    double wallSeconds = chrono::duration<double>(wallEnd - wallStart).count();
    
    cout << "\n======================================================" << endl;
    cout << "Phi-Enriched Processing Summary:" << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Selection criteria:" << endl;
    cout << "  Phi pT > " << minPhiPt << " GeV" << endl;
    cout << "  Phi selection mode: "
         << (requireLheGluon ? "MPI on + require hard-process gluon ancestry" : "MPI on + any phi accepted")
         << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Total LHE events processed:   " << iEvent << endl;
    cout << "Successful Pythia events:     " << successfulPythiaEvents << endl;
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

    if (!manifestFile.empty()) {
        double completion = targetOutputEvents > 0
            ? static_cast<double>(successWithPhi) / max(1, targetOutputEvents)
            : 1.0;
        ofstream manifest(manifestFile);
        if (!manifest.is_open()) {
            cerr << "Failed to write manifest: " << manifestFile << endl;
            return 1;
        }
        manifest << "{\n"
                 << "  \"mode\": \"phi_mpi_on_gluon\",\n"
                 << "  \"require_lhe_gluon\": " << (requireLheGluon ? "true" : "false") << ",\n"
                 << "  \"target_events\": " << targetOutputEvents << ",\n"
                 << "  \"input_budget\": " << nEvents << ",\n"
                 << "  \"max_hadronization_retries\": " << maxRetry << ",\n"
                 << "  \"attempted_lhe_events\": " << iEvent << ",\n"
                 << "  \"successful_pythia_events\": " << successfulPythiaEvents << ",\n"
                 << "  \"accepted_hepmc_events\": " << successWithPhi << ",\n"
                 << "  \"actual_hepmc_events\": " << successWithPhi << ",\n"
                 << "  \"failed_phi_selections\": " << failedToFindPhi << ",\n"
                 << "  \"total_hadronization_retries\": " << totalRetries << ",\n"
                 << "  \"average_retries_per_accepted_event\": " << (static_cast<double>(totalRetries) / max(1, successWithPhi)) << ",\n"
                 << "  \"average_retries_per_attempted_event\": " << (static_cast<double>(totalRetries) / max(1, iEvent)) << ",\n"
                 << "  \"wall_time_seconds\": " << wallSeconds << ",\n"
                 << "  \"completion_fraction\": " << completion << ",\n"
                 << "  \"complete\": " << (targetOutputEvents > 0 && successWithPhi >= targetOutputEvents ? "true" : "false") << ",\n"
                 << "  \"status\": \"" << (successWithPhi > 0 ? (targetOutputEvents > 0 && successWithPhi >= targetOutputEvents ? "ok" : "partial") : "failed") << "\",\n"
                 << "  \"output_file\": \"" << outputFile << "\"\n"
                 << "}\n";
    }
    
    return 0;
}
