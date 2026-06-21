// ==============================================================================
// shower_normal.cc - Standard Pythia8 shower processing
// ==============================================================================
// workbook_v2 中的 standard 模式要求“正常 shower”。
// 因此这里不做 phi enrichment，也不做额外的 muon 选择或重复 hadronization。
//
// Compilation (in CMSSW environment):
//   g++ -std=c++17 -O2 shower_normal.cc -o shower_normal \
//       $(pythia8-config --cxxflags --libs) \
//       -I$HEPMC3/include -L$HEPMC3/lib64 -lHepMC3
//
// Usage:
//   ./shower_normal input.lhe output.hepmc [nEvents] [minMuonPt] [maxMuonEta] [maxRetry]
// ==============================================================================

#include "Pythia8/Pythia.h"
#include "Pythia8Plugins/HepMC3.h"

#include <iostream>
#include <string>

using namespace Pythia8;
using namespace std;

// 以下函数保留作调试用途；standard 模式本身不依赖它们做筛选。
bool hasValidJpsiMuons(Event& event, double minPt = 2.5, double maxEta = 2.4) {
    for (int i = 0; i < event.size(); ++i) {
        if (abs(event[i].id()) != 443) continue; // Only J/psi
        
        int status = event[i].status();
        if (status >= 0 && !event[i].isFinal()) continue;
        
        int d1 = event[i].daughter1();
        int d2 = event[i].daughter2();
        
        if (d1 <= 0 || d2 <= 0) continue;
        
        bool foundMuPlus = false, foundMuMinus = false;
        bool muPlusValid = false, muMinusValid = false;
        
        for (int j = d1; j <= d2; ++j) {
            int pid = event[j].id();
            if (pid == 13) { // mu-
                foundMuMinus = true;
                if (event[j].pT() > minPt && abs(event[j].eta()) < maxEta) {
                    muMinusValid = true;
                }
            } else if (pid == -13) { // mu+
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

int main(int argc, char* argv[]) {
    
    if (argc < 3) {
        cerr << "\n=== Pythia8 Standard Shower Processing ===" << endl;
        cerr << "Usage: " << argv[0] << " input.lhe output.hepmc [nEvents] [minMuonPt] [maxMuonEta] [maxRetry] [rngSeed]" << endl;
        cerr << "\nArguments:" << endl;
        cerr << "  input.lhe   : Input LHE file" << endl;
        cerr << "  output.hepmc: Output HepMC file" << endl;
        cerr << "  nEvents     : Number of events to process (default: -1, all)" << endl;
        cerr << "  minMuonPt   : Minimum muon pT in GeV (default: 2.5)" << endl;
        cerr << "  maxMuonEta  : Maximum muon |eta| (default: 2.4)" << endl;
        cerr << "  maxRetry    : Maximum hadronization retries (default: 100)" << endl;
        cerr << "  rngSeed     : Optional Pythia RNG seed" << endl;
        return 1;
    }
    
    string inputFile = argv[1];
    string outputFile = argv[2];
    int nEvents = (argc > 3) ? atoi(argv[3]) : -1;
    double minMuonPt = (argc > 4) ? atof(argv[4]) : 2.5;
    double maxMuonEta = (argc > 5) ? atof(argv[5]) : 2.4;
    int maxRetry = (argc > 6) ? atoi(argv[6]) : 1000;
    int rngSeed = (argc > 7) ? atoi(argv[7]) : 0;
    
    cout << "\n=== Pythia8 Standard Shower Processing ===" << endl;
    cout << "Input LHE:    " << inputFile << endl;
    cout << "Output HepMC: " << outputFile << endl;
    cout << "Events:       " << (nEvents > 0 ? to_string(nEvents) : "all") << endl;
    cout << "Min muon pT:  " << minMuonPt << " GeV (legacy arg, standard 模式不做筛选)" << endl;
    cout << "Max muon eta: " << maxMuonEta << " (legacy arg, standard 模式不做筛选)" << endl;
    cout << "Max retries:  " << maxRetry << " (legacy arg, standard 模式不使用)" << endl;
    cout << "RNG seed:     " << (rngSeed > 0 ? to_string(rngSeed) : "Pythia default") << endl;
    cout << "==========================================\n" << endl;
    
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


    // Shower settings
    pythia.readString("PartonLevel:ISR = on");
    pythia.readString("PartonLevel:FSR = on");
    pythia.readString("PartonLevel:MPI = on");
    
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
    int successEvents = 0;
    int failedEvents = 0;
    
    cout << "Starting event processing..." << endl;
    
    while (true) {
        if (nEvents > 0 && iEvent >= nEvents) break;
        
        // standard 模式直接执行完整 shower + hadronization。
        if (!pythia.next()) {
            if (pythia.info.atEndOfFile()) {
                cout << "Reached end of LHE file." << endl;
                break;
            }
            if (++iAbort < maxAbort) continue;
            cout << "Event generation aborted prematurely!" << endl;
            break;
        }
        
        successEvents++;
        toHepMC.writeNextEvent(pythia);
        
        ++iEvent;
        if (iEvent % 100 == 0) {
            double efficiency = 100.0 * successEvents / iEvent;
            cout << "Processed " << iEvent << " events, "
                 << "efficiency: " << efficiency << "%" << endl;
        }
    }
    
    pythia.stat();
    
    cout << "\n======================================================" << endl;
    cout << "Processing Summary:" << endl;
    cout << "------------------------------------------------------" << endl;
    cout << "Total LHE events processed: " << iEvent << endl;
    cout << "Events written:             " << successEvents 
         << " (" << 100.0*successEvents/max(1,iEvent) << "%)" << endl;
    cout << "Events skipped:             " << failedEvents << endl;
    cout << "Average retries per event:  1 (standard 模式不重试)" << endl;
    cout << "Output file: " << outputFile << endl;
    cout << "======================================================" << endl;
    
    return 0;
}
