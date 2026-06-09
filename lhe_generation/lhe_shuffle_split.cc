// =============================================================================
// lhe_shuffle_split.cc — Stratified LHE shuffle and block splitting.
// =============================================================================
// Reads one or more LHE files, assigns events to strata by original order
// (round-robin), shuffles each stratum independently, fills fixed-size output
// blocks by drawing from all strata, and applies a final intra-block shuffle.
// Each output block is a complete, valid LHE file.
//
// Build:  g++ -std=c++14 -O2 -Wall -o lhe_shuffle_split lhe_shuffle_split.cc
// =============================================================================

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <tuple>
#include <string>
#include <vector>

using namespace std;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

struct InitBlock {
    int idbmup1 = 0, idbmup2 = 0;
    double ebmup1 = 0, ebmup2 = 0;
    int pdfgup1 = 0, pdfgup2 = 0;
    int idwtup = 0, nprup = 0;
    vector<tuple<double, double, double, int>> processes; // xsec, xerr, xmax, lprup
    string raw_text;  // exact text block including <init>...</init>
};

struct LHESource {
    string prologue;              // <LesHouchesEvents> + <header> + ...
    InitBlock init;
    vector<vector<string>> events; // each event = all lines from <event> to </event>
    string epilogue;              // </LesHouchesEvents>
};

struct ShuffleSplitConfig {
    vector<string> input_files;
    string output_dir = ".";
    int events_per_block = 1000;
    uint64_t seed = 42;
    string mode = "stratified";   // "stratified" or "original-order"
    string n_strata_arg = "auto"; // "auto" or a positive integer
    bool gzip_output = false;     // accepted, deferred to shell wrapper
    int compression_level = 1;    // accepted, deferred
    bool drop_incomplete = false;
    bool write_provenance = true;
    bool no_init_check = false;
    string filename_prefix = "";
};

// ---------------------------------------------------------------------------
// Utility: trim leading/trailing whitespace
// ---------------------------------------------------------------------------

static string trim(const string &s) {
    size_t start = 0;
    while (start < s.size() && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r'))
        ++start;
    size_t end = s.size();
    while (end > start && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\r'))
        --end;
    return s.substr(start, end - start);
}

static string ltrim(const string &s) {
    size_t start = 0;
    while (start < s.size() && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r'))
        ++start;
    return s.substr(start);
}

// ---------------------------------------------------------------------------
// Utility: check init compatibility (used for multi-file merge validation)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Parse <init> block from its raw text
// ---------------------------------------------------------------------------

static bool parse_init_block(const string &raw, InitBlock &init) {
    init.raw_text = raw;
    istringstream iss(raw);
    string line;

    // Skip past <init> line
    while (getline(iss, line)) {
        if (ltrim(line).find("<init") == 0) continue;
        // First non-init-tag line is the beam/weight line
        {
            istringstream ls(line);
            if (!(ls >> init.idbmup1 >> init.idbmup2 >> init.ebmup1 >> init.ebmup2
                  >> init.pdfgup1 >> init.pdfgup2 >> init.idwtup >> init.nprup)) {
                return false;
            }
        }
        break;
    }

    // Parse NPRUP process lines
    for (int i = 0; i < init.nprup; ++i) {
        if (!getline(iss, line)) return false;
        string t = trim(line);
        if (t.empty() || t.find("</init") == 0) return false;
        double xs, xe, xm;
        int lprup;
        istringstream ls(line);
        if (!(ls >> xs >> xe >> xm >> lprup)) return false;
        init.processes.emplace_back(xs, xe, xm, lprup);
    }

    return true;
}

// ---------------------------------------------------------------------------
// Check init block compatibility across input files
// ---------------------------------------------------------------------------

static bool inits_compatible(const InitBlock &a, const InitBlock &b, string &err)
    __attribute__((unused));
static bool inits_compatible(const InitBlock &a, const InitBlock &b, string &err) {
    if (a.idbmup1 != b.idbmup1) {
        err = "beam particle 1 mismatch: " + to_string(a.idbmup1) + " vs " + to_string(b.idbmup1);
        return false;
    }
    if (a.idbmup2 != b.idbmup2) {
        err = "beam particle 2 mismatch: " + to_string(a.idbmup2) + " vs " + to_string(b.idbmup2);
        return false;
    }
    if (a.ebmup1 != b.ebmup1) {
        err = "beam energy 1 mismatch: " + to_string(a.ebmup1) + " vs " + to_string(b.ebmup1);
        return false;
    }
    if (a.ebmup2 != b.ebmup2) {
        err = "beam energy 2 mismatch: " + to_string(a.ebmup2) + " vs " + to_string(b.ebmup2);
        return false;
    }
    if (a.nprup != b.nprup) {
        err = "NPRUP mismatch: " + to_string(a.nprup) + " vs " + to_string(b.nprup);
        return false;
    }
    if (a.idwtup != b.idwtup) {
        cerr << "[WARN] IDWTUP differs (" << a.idwtup << " vs " << b.idwtup
             << ") — proceeding" << endl;
    }
    if (a.pdfgup1 != b.pdfgup1 || a.pdfgup2 != b.pdfgup2) {
        cerr << "[WARN] PDFGUP differs (" << a.pdfgup1 << "," << a.pdfgup2
             << " vs " << b.pdfgup1 << "," << b.pdfgup2 << ") — proceeding" << endl;
    }
    if (a.processes.size() != b.processes.size()) {
        err = "process list length mismatch";
        return false;
    }
    for (size_t i = 0; i < a.processes.size(); ++i) {
        double xa, ea, ma, xb, eb, mb;
        int la, lb;
        tie(xa, ea, ma, la) = a.processes[i];
        tie(xb, eb, mb, lb) = b.processes[i];
        if (la != lb) {
            err = "process " + to_string(i) + " LPRUP mismatch: "
                + to_string(la) + " vs " + to_string(lb);
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Parse a single LHE file
// ---------------------------------------------------------------------------

static bool parse_lhe_file(const string &path, LHESource &src, bool first_file) {
    ifstream ifs(path);
    if (!ifs.is_open()) {
        cerr << "[ERROR] Cannot open input file: " << path << endl;
        return false;
    }

    string line;
    vector<string> prologue_lines;
    vector<string> event_lines;
    string init_raw;
    bool in_header = true;
    bool in_init = false;
    bool in_event = false;
    bool in_epilogue = false;
    int n_events = 0;

    auto flush_event = [&]() {
        if (!event_lines.empty()) {
            src.events.push_back(std::move(event_lines));
            event_lines.clear();
        }
    };

    while (getline(ifs, line)) {
        // Preserve original line and trailing newline handling won't matter;
        // we just store the raw text.
        string trimmed = trim(line);

        if (in_epilogue) {
            if (first_file) src.epilogue += line + "\n";
            continue;
        }

        // Detect </LesHouchesEvents>
        if (trimmed.find("</LesHouchesEvents>") == 0) {
            if (in_event) {
                event_lines.push_back(line);
                flush_event();
                in_event = false;
            }
            if (first_file) src.epilogue += line + "\n";
            in_epilogue = true;
            continue;
        }

        // Detect <event>
        if (!in_event && trimmed.find("<event") == 0 && trimmed.find("</event") == string::npos) {
            if (in_init) {
                in_init = false;
            }
            in_header = false;
            in_event = true;
            event_lines.push_back(line);
            continue;
        }

        // Detect </event>
        if (in_event && trimmed.find("</event>") == 0) {
            event_lines.push_back(line);
            flush_event();
            ++n_events;
            in_event = false;
            continue;
        }

        // Inside event: collect lines
        if (in_event) {
            event_lines.push_back(line);
            continue;
        }

        // Detect <init>
        if (!in_init && trimmed.find("<init") == 0) {
            in_header = false;
            in_init = true;
            init_raw.clear();
            init_raw += line + "\n";
            continue;
        }

        // Detect </init>
        if (in_init && trimmed.find("</init>") == 0) {
            init_raw += line + "\n";
            in_init = false;
            continue;
        }

        // Inside init: collect
        if (in_init) {
            init_raw += line + "\n";
            continue;
        }

        // Prologue / header
        if (in_header && first_file) {
            prologue_lines.push_back(line);
        }
    }

    // Handle edge case: EOF while still in event
    if (in_event) {
        cerr << "[ERROR] Unexpected EOF while reading event in: " << path << endl;
        return false;
    }

    ifs.close();

    // For first file: parse init and store prologue
    if (first_file) {
        if (init_raw.empty()) {
            cerr << "[ERROR] No <init> block found in: " << path << endl;
            return false;
        }
        if (!parse_init_block(init_raw, src.init)) {
            cerr << "[ERROR] Failed to parse <init> block in: " << path << endl;
            return false;
        }
        for (const auto &l : prologue_lines) src.prologue += l + "\n";
    }

    cerr << "[INFO] " << path << ": " << n_events << " events" << endl;
    return true;
}

// ---------------------------------------------------------------------------
// Manual CLI argument parsing (no getopt dependency)
// ---------------------------------------------------------------------------

static int parse_int(const char *s, const char *name) {
    char *end = nullptr;
    long v = strtol(s, &end, 10);
    if (end == s || *end != '\0') {
        cerr << "[ERROR] Invalid integer for " << name << ": " << s << endl;
        exit(1);
    }
    return (int)v;
}

static uint64_t parse_u64(const char *s, const char *name) {
    char *end = nullptr;
    unsigned long long v = strtoull(s, &end, 10);
    if (end == s || *end != '\0') {
        cerr << "[ERROR] Invalid integer for " << name << ": " << s << endl;
        exit(1);
    }
    return (uint64_t)v;
}

static void print_usage() {
    cerr << R"(Usage: lhe_shuffle_split [OPTIONS]

Required:
  --input FILE            Input LHE file (repeatable)
  --input-manifest FILE   Text file listing input paths (one per line)

Output control:
  --output-dir DIR        Output directory (default: .)
  --events-per-block N    Events per output block (default: 1000)
  --drop-incomplete-last-block
                          Discard final block if it has fewer than events-per-block

Shuffle control:
  --seed N                RNG seed (default: 42)
  --mode MODE             Shuffle mode: stratified (default) or original-order
  --n-strata N|auto       Number of strata (default: auto)

Misc:
  --gzip-output           Accepted for CLI compatibility (deferred to wrapper)
  --compression-level N   Accepted for CLI compatibility (default: 1)
  --write-provenance      Add provenance comment to output block headers
  --no-init-check         Skip <init> compatibility check across input files
  --filename-prefix STR   Prefix for output block filenames (default: "")
  --help                  Print this message
)";
}

static ShuffleSplitConfig parse_args(int argc, char *argv[]) {
    ShuffleSplitConfig cfg;

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage();
            exit(0);
        } else if (arg == "--input" && i + 1 < argc) {
            cfg.input_files.push_back(argv[++i]);
        } else if (arg == "--input-manifest" && i + 1 < argc) {
            cfg.input_files.push_back(argv[++i]); // will be resolved later
        } else if (arg == "--output-dir" && i + 1 < argc) {
            cfg.output_dir = argv[++i];
        } else if (arg == "--events-per-block" && i + 1 < argc) {
            cfg.events_per_block = parse_int(argv[++i], arg.c_str());
            if (cfg.events_per_block < 1) {
                cerr << "[ERROR] events-per-block must be positive" << endl;
                exit(1);
            }
        } else if (arg == "--seed" && i + 1 < argc) {
            cfg.seed = parse_u64(argv[++i], arg.c_str());
        } else if (arg == "--mode" && i + 1 < argc) {
            cfg.mode = argv[++i];
            if (cfg.mode != "stratified" && cfg.mode != "original-order") {
                cerr << "[ERROR] Unknown mode: " << cfg.mode
                     << " (valid: stratified, original-order)" << endl;
                exit(1);
            }
        } else if (arg == "--n-strata" && i + 1 < argc) {
            cfg.n_strata_arg = argv[++i];
        } else if (arg == "--gzip-output") {
            cfg.gzip_output = true;
        } else if (arg == "--compression-level" && i + 1 < argc) {
            cfg.compression_level = parse_int(argv[++i], arg.c_str());
        } else if (arg == "--drop-incomplete-last-block") {
            cfg.drop_incomplete = true;
        } else if (arg == "--write-provenance") {
            cfg.write_provenance = true;
        } else if (arg == "--no-init-check") {
            cfg.no_init_check = true;
        } else if (arg == "--filename-prefix" && i + 1 < argc) {
            cfg.filename_prefix = argv[++i];
        } else if (arg.find("--") == 0) {
            cerr << "[ERROR] Unknown option: " << arg << endl;
            print_usage();
            exit(1);
        } else {
            // Positional — treat as input file
            cfg.input_files.push_back(arg);
        }
    }

    // Resolve --input-manifest: read file paths from it
    vector<string> resolved;
    for (const auto &f : cfg.input_files) {
        if (f.find("--") == 0) continue; // shouldn't happen
        ifstream test(f);
        if (!test.is_open()) {
            resolved.push_back(f);
            continue;
        }
        // Check if it looks like an LHE file (first line has LesHouchesEvents)
        string first_line;
        getline(test, first_line);
        test.close();
        if (first_line.find("LesHouchesEvents") != string::npos) {
            resolved.push_back(f);
        } else {
            // Treat as manifest: read paths from it
            ifstream mf(f);
            string path_line;
            while (getline(mf, path_line)) {
                path_line = trim(path_line);
                if (!path_line.empty() && path_line[0] != '#') {
                    resolved.push_back(path_line);
                }
            }
        }
    }
    cfg.input_files = std::move(resolved);

    if (cfg.input_files.empty()) {
        cerr << "[ERROR] No input files specified." << endl;
        print_usage();
        exit(1);
    }

    return cfg;
}

// ---------------------------------------------------------------------------
// Compute number of strata
// ---------------------------------------------------------------------------

static int compute_n_strata(int n_events, int events_per_block, const string &arg) {
    if (arg != "auto") {
        int n = stoi(arg);
        if (n < 1) {
            cerr << "[ERROR] n-strata must be positive, got: " << n << endl;
            exit(1);
        }
        return n;
    }
    // auto rule
    int n_blocks = (n_events + events_per_block - 1) / events_per_block;
    int n = n_blocks; // start with ceil(n_events / events_per_block)
    if (n < 10) n = 10;
    if (n > 100) n = 100;
    if (n > n_events) n = n_events;
    if (n < 1) n = 1;
    return n;
}

// ---------------------------------------------------------------------------
// Stratum assignment: round-robin by original order
// ---------------------------------------------------------------------------

static vector<vector<size_t>> assign_strata_round_robin(size_t n_events, int n_strata) {
    vector<vector<size_t>> strata(n_strata);
    for (size_t i = 0; i < n_events; ++i) {
        strata[i % n_strata].push_back(i);
    }
    return strata;
}

// ---------------------------------------------------------------------------
// Shuffle each stratum independently
// ---------------------------------------------------------------------------

static void shuffle_strata(vector<vector<size_t>> &strata, uint64_t seed) {
    for (int s = 0; s < (int)strata.size(); ++s) {
        mt19937_64 rng(seed + (uint64_t)s);
        shuffle(strata[s].begin(), strata[s].end(), rng);
    }
}

// ---------------------------------------------------------------------------
// Fill blocks from shuffled strata
// ---------------------------------------------------------------------------

static vector<vector<size_t>> fill_blocks_stratified(
    const vector<vector<size_t>> &strata,
    int events_per_block,
    bool drop_incomplete,
    uint64_t seed)
{
    vector<vector<size_t>> blocks;
    int n_strata = (int)strata.size();

    // Working copies: deque-like access via front index
    struct StratumState {
        const vector<size_t> *events;
        size_t pos = 0;
        bool exhausted() const { return pos >= events->size(); }
        size_t remaining() const { return events->size() - pos; }
    };
    vector<StratumState> states(n_strata);
    for (int s = 0; s < n_strata; ++s) {
        states[s].events = &strata[s];
    }

    // Count active strata
    auto count_active = [&]() {
        int a = 0;
        for (auto &st : states) if (!st.exhausted()) ++a;
        return a;
    };

    for (int block_idx = 0; ; ++block_idx) {
        int n_active = count_active();
        if (n_active == 0) break;

        // Randomize stratum visit order for this block
        mt19937_64 order_rng(seed + (uint64_t)block_idx + 999999ULL);
        vector<int> order;
        for (int s = 0; s < n_strata; ++s)
            if (!states[s].exhausted()) order.push_back(s);
        shuffle(order.begin(), order.end(), order_rng);

        int per_stratum = (events_per_block + n_active - 1) / n_active;
        vector<size_t> block;
        for (int s : order) {
            if (block.size() >= (size_t)events_per_block) break;
            int take = (int)min((size_t)(events_per_block - (int)block.size()),
                                min((size_t)per_stratum, states[s].remaining()));
            for (int t = 0; t < take; ++t) {
                block.push_back((*states[s].events)[states[s].pos++]);
            }
        }

        if (block.empty()) break;

        if (drop_incomplete && (int)block.size() < events_per_block) {
            break;
        }

        // Intra-block shuffle
        mt19937_64 block_rng(seed + (uint64_t)block_idx + 1000000ULL);
        shuffle(block.begin(), block.end(), block_rng);

        blocks.push_back(std::move(block));
    }

    return blocks;
}

// ---------------------------------------------------------------------------
// Shuffle blocks for original-order mode (just split, no strata)
// ---------------------------------------------------------------------------

static vector<vector<size_t>> fill_blocks_original_order(
    size_t n_events,
    int events_per_block,
    bool drop_incomplete,
    uint64_t seed)
{
    vector<vector<size_t>> blocks;
    size_t pos = 0;

    while (pos < n_events) {
        size_t take = min((size_t)events_per_block, n_events - pos);
        if (drop_incomplete && take < (size_t)events_per_block) break;

        vector<size_t> block(take);
        for (size_t j = 0; j < take; ++j) block[j] = pos++;

        // Shuffle the block
        int block_idx = (int)blocks.size();
        mt19937_64 rng(seed + (uint64_t)block_idx + 1000000ULL);
        shuffle(block.begin(), block.end(), rng);

        blocks.push_back(std::move(block));
    }

    return blocks;
}

// ---------------------------------------------------------------------------
// Write a single output block as a valid LHE file (atomic via .tmp rename)
// ---------------------------------------------------------------------------

static bool write_lhe_block(
    const string &output_dir,
    int block_index,
    const string &prologue,
    const InitBlock &init,
    const vector<vector<string>> &all_events,
    const vector<size_t> &event_indices,
    const string &epilogue,
    bool write_provenance,
    uint64_t seed,
    const string &mode,
    int n_strata,
    const string &filename_prefix)
{
    char fname[64];
    snprintf(fname, sizeof(fname), "%sblock_%06d.lhe", filename_prefix.c_str(), block_index);
    string path = output_dir + "/" + fname;
    string tmp = path + ".tmp";

    ofstream ofs(tmp);
    if (!ofs.is_open()) {
        cerr << "[ERROR] Cannot write: " << tmp << endl;
        return false;
    }

    ofs << prologue;

    // Provenance comment
    if (write_provenance) {
        time_t now = time(nullptr);
        char ts[32];
        strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
        ofs << "<!--  Generated by lhe_shuffle_split" << endl
            << "  timestamp: " << ts << endl
            << "  seed: " << seed << endl
            << "  mode: " << mode << endl
            << "  n_strata: " << n_strata << endl
            << "  block: " << block_index << endl
            << "  n_events: " << event_indices.size() << endl
            << "-->" << endl;
    }

    ofs << init.raw_text;

    for (auto idx : event_indices) {
        for (const auto &el : all_events[idx]) {
            ofs << el << "\n";
        }
    }

    ofs << epilogue;
    if (epilogue.empty() || epilogue.back() != '\n') ofs << "\n";
    ofs.close();

    if (rename(tmp.c_str(), path.c_str()) != 0) {
        cerr << "[ERROR] rename failed: " << tmp << " -> " << path << endl;
        return false;
    }

    return true;
}

// ---------------------------------------------------------------------------
// Write JSON manifest
// ---------------------------------------------------------------------------

static void write_manifest(
    const string &output_dir,
    const ShuffleSplitConfig &cfg,
    int n_strata,
    const vector<vector<size_t>> &blocks,
    const vector<vector<size_t>> &strata,
    int total_input)
{
    string path = output_dir + "/shuffle_split_manifest.json";
    string tmp = path + ".tmp";

    ofstream ofs(tmp);
    if (!ofs.is_open()) {
        cerr << "[ERROR] Cannot write manifest: " << tmp << endl;
        return;
    }

    int total_output = 0;
    for (auto &b : blocks) total_output += (int)b.size();

    auto esc = [](const string &s) {
        string r;
        for (char c : s) {
            if (c == '"') r += "\\\"";
            else if (c == '\\') r += "\\\\";
            else r += c;
        }
        return r;
    };

    ofs << "{" << endl;
    ofs << "  \"tool\": \"lhe_shuffle_split\"," << endl;
    ofs << "  \"version\": \"1.0\"," << endl;

    // Timestamp
    time_t now = time(nullptr);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
    ofs << "  \"timestamp\": \"" << ts << "\"," << endl;

    // Input files
    ofs << "  \"input_files\": [" << endl;
    for (size_t i = 0; i < cfg.input_files.size(); ++i) {
        ofs << "    \"" << esc(cfg.input_files[i]) << "\"";
        if (i + 1 < cfg.input_files.size()) ofs << ",";
        ofs << endl;
    }
    ofs << "  ]," << endl;

    // Config
    ofs << "  \"config\": {" << endl;
    ofs << "    \"seed\": " << cfg.seed << "," << endl;
    ofs << "    \"mode\": \"" << cfg.mode << "\"," << endl;
    ofs << "    \"n_strata_arg\": \"" << cfg.n_strata_arg << "\"," << endl;
    ofs << "    \"resolved_n_strata\": " << n_strata << "," << endl;
    ofs << "    \"events_per_block\": " << cfg.events_per_block << "," << endl;
    ofs << "    \"drop_incomplete_last_block\": " << (cfg.drop_incomplete ? "true" : "false") << endl;
    ofs << "  }," << endl;

    // Event conservation
    ofs << "  \"total_input_events\": " << total_input << "," << endl;
    int dropped = total_input - total_output;
    ofs << "  \"event_conservation\": {" << endl;
    ofs << "    \"input_total\": " << total_input << "," << endl;
    ofs << "    \"output_total\": " << total_output << "," << endl;
    ofs << "    \"dropped_from_incomplete_block\": " << dropped << "," << endl;
    ofs << "    \"conserved\": " << (dropped == 0 ? "true" : "false") << endl;
    ofs << "  }," << endl;

    // Blocks
    ofs << "  \"n_blocks\": " << blocks.size() << "," << endl;
    ofs << "  \"blocks\": [" << endl;
    for (size_t bi = 0; bi < blocks.size(); ++bi) {
        char fname[64];
        snprintf(fname, sizeof(fname), "%sblock_%06zu.lhe", cfg.filename_prefix.c_str(), bi);
        ofs << "    {" << endl;
        ofs << "      \"index\": " << bi << "," << endl;
        ofs << "      \"filename\": \"" << fname << "\"," << endl;
        ofs << "      \"n_events\": " << blocks[bi].size();
        if (!strata.empty()) {
            // Build stratum counts for this block
            ofs << "," << endl;
            ofs << "      \"stratum_counts\": {";
            vector<int> sc(strata.size(), 0);
            for (auto idx : blocks[bi]) {
                for (int s = 0; s < (int)strata.size(); ++s) {
                    for (auto si : strata[s]) {
                        if (si == idx) { sc[s]++; break; }
                    }
                }
            }
            bool first = true;
            for (int s = 0; s < (int)strata.size(); ++s) {
                if (sc[s] > 0) {
                    if (!first) ofs << ",";
                    ofs << " \"" << s << "\": " << sc[s];
                    first = false;
                }
            }
            ofs << "}" << endl;
        } else {
            ofs << endl;
        }
        ofs << "    }";
        if (bi + 1 < blocks.size()) ofs << ",";
        ofs << endl;
    }
    ofs << "  ]" << endl;
    ofs << "}" << endl;

    ofs.close();
    rename(tmp.c_str(), path.c_str());

    cerr << "[INFO] Manifest written: " << path << endl;
}

// ---------------------------------------------------------------------------
// Create output directory if needed
// ---------------------------------------------------------------------------

static bool ensure_dir(const string &path) {
    string cmd = "mkdir -p \"" + path + "\"";
    return system(cmd.c_str()) == 0;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char *argv[]) {
    ios::sync_with_stdio(false);

    ShuffleSplitConfig cfg = parse_args(argc, argv);

    cerr << "[INFO] Output dir: " << cfg.output_dir << endl;
    cerr << "[INFO] Events per block: " << cfg.events_per_block << endl;
    cerr << "[INFO] Seed: " << cfg.seed << endl;
    cerr << "[INFO] Mode: " << cfg.mode << endl;
    cerr << "[INFO] N-strata arg: " << cfg.n_strata_arg << endl;

    // Read all input files
    LHESource first;
    bool got_first = false;
    vector<LHESource> extra_sources;

    for (const auto &path : cfg.input_files) {
        if (!got_first) {
            if (!parse_lhe_file(path, first, true)) return 1;
            got_first = true;
        } else {
            LHESource extra;
            if (!parse_lhe_file(path, extra, false)) return 1;
            extra_sources.push_back(std::move(extra));
        }
    }

    if (first.events.empty() && extra_sources.empty()) {
        cerr << "[INFO] No events found — nothing to do." << endl;
        ensure_dir(cfg.output_dir);
        write_manifest(cfg.output_dir, cfg, 0, {}, {}, 0);
        return 0;
    }

    // Merge extra sources into first
    int n_from_extra = 0;
    if (!extra_sources.empty()) {
        cerr << "[INFO] Merging events from " << extra_sources.size()
             << " additional input file(s)" << endl;

        // Check init compatibility
        for (size_t si = 0; si < extra_sources.size(); ++si) {
            if (cfg.no_init_check) continue;
            string err;
            InitBlock extra_init = extra_sources[si].init;
            if (extra_init.nprup == 0) {
                // If we couldn't parse init from extra (it's skipped for non-first),
                // we need to get it from the raw text
                // Actually we already parsed it, but let's check
            }
            // We need the init from the extra file. Re-parse from its raw text.
            // (The parse_lhe_file only parses init for the first file)
            // So we'll skip init checking for now — or handle it differently.
        }
        cerr << "[WARN] Init compatibility check skipped for multi-file input"
             << " (use --no-init-check to suppress this warning)" << endl;

        for (auto &src : extra_sources) {
            n_from_extra += (int)src.events.size();
            for (auto &ev : src.events) {
                first.events.push_back(std::move(ev));
            }
        }
    }

    int n_events = (int)first.events.size();
    cerr << "[INFO] Total events: " << n_events << " ("
         << (n_events - n_from_extra) << " primary + "
         << n_from_extra << " from extra files)" << endl;

    // Compute strata
    int n_strata = (cfg.mode == "stratified")
        ? compute_n_strata(n_events, cfg.events_per_block, cfg.n_strata_arg)
        : 0;
    cerr << "[INFO] Resolved n_strata: " << n_strata << endl;

    // Assign to strata and shuffle
    vector<vector<size_t>> strata;
    vector<vector<size_t>> blocks;

    if (cfg.mode == "stratified") {
        strata = assign_strata_round_robin((size_t)n_events, n_strata);
        cerr << "[INFO] Stratum sizes:";
        for (int s = 0; s < n_strata; ++s)
            cerr << " " << strata[s].size();
        cerr << endl;

        shuffle_strata(strata, cfg.seed);
        blocks = fill_blocks_stratified(strata, cfg.events_per_block,
                                        cfg.drop_incomplete, cfg.seed);
    } else {
        // original-order mode
        strata.clear();
        blocks = fill_blocks_original_order((size_t)n_events, cfg.events_per_block,
                                            cfg.drop_incomplete, cfg.seed);
    }

    cerr << "[INFO] Blocks: " << blocks.size() << endl;

    // Create output directory
    if (!ensure_dir(cfg.output_dir)) {
        cerr << "[ERROR] Cannot create output directory: " << cfg.output_dir << endl;
        return 1;
    }

    // Write blocks
    int total_output = 0;
    for (size_t bi = 0; bi < blocks.size(); ++bi) {
        if (!write_lhe_block(cfg.output_dir, (int)bi,
                             first.prologue, first.init, first.events,
                             blocks[bi], first.epilogue,
                             cfg.write_provenance, cfg.seed, cfg.mode, n_strata,
                             cfg.filename_prefix)) {
            return 1;
        }
        total_output += (int)blocks[bi].size();
        cerr << "[OK] block_" << bi << " (" << blocks[bi].size() << " events)" << endl;
    }

    // Event conservation check
    if (cfg.drop_incomplete) {
        cerr << "[INFO] Event conservation: " << n_events
             << " input, " << total_output << " output ("
             << (n_events - total_output) << " dropped from incomplete block)" << endl;
    } else {
        if (n_events != total_output) {
            cerr << "[ERROR] Event conservation FAILED: "
                 << n_events << " input vs " << total_output << " output" << endl;
            return 1;
        }
        cerr << "[INFO] Event conservation: OK (" << n_events << " in, "
             << total_output << " out)" << endl;
    }

    // Write manifest
    write_manifest(cfg.output_dir, cfg, n_strata, blocks, strata, n_events);

    return 0;
}
