// mergesort_ff.cpp - FastFlow baseline (parallel run generation with a Farm)
// Phase 1: Emitter reads blocks -> Workers sort+write runs in parallel
// Phase 2: mergeFiles(...) sequential (same as seq/omp)

#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <string>
#include <chrono>
#include <filesystem>
#include <memory>

#include <ff/ff.hpp>
#include <ff/farm.hpp>

#include "../tools/verifier.hpp"
#include "../tools/record_io.hpp"
#include "../tools/merger.hpp"

namespace fs = std::filesystem;
using namespace ff;

// ---- wall clock helper ----
static inline double wall_sec(std::chrono::high_resolution_clock::time_point a,
                              std::chrono::high_resolution_clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

// ---- Task passed through the farm ----
struct Task {
    int runID = 0;
    std::vector<char> rawBlock;
    std::vector<size_t> offsets;
};

// ---- Global timers (sum of stages; note overlap is expected in pipeline/farm) ----
static double t_read  = 0.0;   // emitter only
static double t_sort  = 0.0;   // sum over workers
static double t_write = 0.0;   // sum over workers
static double t_merge = 0.0;   // sequential merge

// ---------------- Emitter ----------------
struct Emitter : ff_node_t<Task> {
    std::string inputPath;

    size_t memoryLimitBytes;   // global budget (for merge budgets etc.)
    size_t blockBytes;         // per-task granularity (THIS creates parallelism)

    int runID = 0;

    std::ifstream inFile;
    std::vector<char> ioBuf;

    explicit Emitter(const std::string& path, size_t memBytes, size_t blkBytes)
  : inputPath(path),
    memoryLimitBytes(memBytes),
    blockBytes(blkBytes),
    ioBuf(1 << 20) {}

    int svc_init() override {
        inFile.open(inputPath, std::ios::binary);
        if (!inFile) return -1;
        inFile.rdbuf()->pubsetbuf(ioBuf.data(), ioBuf.size());
        return 0;
    }

    Task* svc(Task*) override {
        auto t0 = std::chrono::high_resolution_clock::now();

        auto* task = new Task();
        task->runID = runID++;

        // READ: use blockBytes, not memoryLimitBytes
        task->rawBlock.resize(blockBytes);
        inFile.read(task->rawBlock.data(), (std::streamsize)blockBytes);
        size_t bytesRead = (size_t)inFile.gcount();

        if (bytesRead == 0) {
            delete task;
            return EOS;
        }

        // Trim to full records if buffer filled
        if (bytesRead < blockBytes) {
            task->rawBlock.resize(bytesRead);
        } else {
            size_t pos = 0, lastValidEnd = 0;
            while (pos + 12 <= bytesRead) {
                uint32_t len = recordio::lenAt(task->rawBlock, pos);
                size_t recSize = 12ull + (size_t)len;
                if (pos + recSize > bytesRead) break;
                pos += recSize;
                lastValidEnd = pos;
            }
            inFile.seekg((std::streamoff)lastValidEnd - (std::streamoff)bytesRead, std::ios::cur);
            task->rawBlock.resize(lastValidEnd);
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        t_read += wall_sec(t0, t1);

        // INDEX
        task->offsets.clear();
        task->offsets.reserve(std::max<size_t>(1, task->rawBlock.size() / 64));

        size_t pos = 0, limit = task->rawBlock.size();
        while (pos + 12 <= limit) {
            uint32_t len = recordio::lenAt(task->rawBlock, pos);
            size_t recSize = 12ull + (size_t)len;
            if (pos + recSize > limit) break;
            task->offsets.push_back(pos);
            pos += recSize;
        }

        return task;
    }
};

// ---------------- Worker ----------------
struct Worker : ff_node_t<Task> {
    double local_sort  = 0.0;
    double local_write = 0.0;

    Task* svc(Task* task) override {
        // --- SORT ---
        auto t0 = std::chrono::high_resolution_clock::now();

        auto& raw = task->rawBlock;
        auto& off = task->offsets;

        std::sort(off.begin(), off.end(),
                  [&](size_t a, size_t b) {
                      return recordio::keyAt(raw, a) < recordio::keyAt(raw, b);
                  });

        auto t1 = std::chrono::high_resolution_clock::now();
        local_sort += wall_sec(t0, t1);

        // --- WRITE ---
        auto t2 = std::chrono::high_resolution_clock::now();

        std::string name = "run_" + std::to_string(task->runID) + ".bin";
        std::ofstream out(name, std::ios::binary);
        std::vector<char> wBuf(1 << 20);
        out.rdbuf()->pubsetbuf(wBuf.data(), wBuf.size());

        for (size_t o : off) recordio::writeAt(out, raw, o);
        out.close();

        auto t3 = std::chrono::high_resolution_clock::now();
        local_write += wall_sec(t2, t3);

        return task; // collector will delete
    }
};

// ---------------- Collector ----------------
struct Collector : ff_node_t<Task> {
    int runs = 0;

    Task* svc(Task* task) override {
        ++runs;
        delete task;
        return GO_ON;
    }
};

int main(int argc, char* argv[]) {
    // Usage:
    // mergesort_ff <file> <N> <PAYLOAD> <MemMB> <workers>
     
    if (argc < 6) {
    std::cerr << "Usage: mergesort_ff <file> <N> <PAYLOAD> <MemMB> <workers>\n";
    std::cerr << "argc=" << argc << "\n";
    return 1;
    }
	
    std::string file = argv[1];
    size_t nRec = std::stoull(argv[2]);
    size_t memBytes = (size_t)std::stoull(argv[4]) * 1024ull * 1024ull;
    int workers = std::stoi(argv[5]);
    if (workers < 1) workers = 1; 
    
    if (!fs::exists(file)) {
    std::cerr << "ERROR: input file not found: " << file << "\n";
    return 1;
    }

    auto wall_start = std::chrono::high_resolution_clock::now();

    // Build farm: Emitter -> Workers -> Collector
    size_t blockBytes = std::min(memBytes, size_t(128) * 1024 * 1024); // 128MB cap

   
    Emitter E(file, memBytes, blockBytes);    Collector C;
	
    // workers
    std::vector<std::unique_ptr<ff_node>> W;
    W.reserve(workers);
    std::vector<Worker*> workerPtrs;
    workerPtrs.reserve(workers);

    for (int i = 0; i < workers; ++i) {
        auto w = std::make_unique<Worker>();
        workerPtrs.push_back(w.get());
        W.push_back(std::move(w));
    }

    // Correct constructor: workers + emitter ref + collector ref
    ff_Farm<Task> farm(std::move(W), E, C);
    
    auto p1_start = std::chrono::high_resolution_clock::now();
	
    if (farm.run_and_wait_end() < 0) {
        std::cerr << "FastFlow farm error\n";
        return 1;
    }
	

    auto p1_end = std::chrono::high_resolution_clock::now();
    double t_phase1_wall = wall_sec(p1_start, p1_end);
    int runs = C.runs;

// Sum worker times
for (auto* w : workerPtrs) {
    t_sort  += w->local_sort;
    t_write += w->local_write;
}

    // Phase 2 merge (sequential)
    auto m_start = std::chrono::high_resolution_clock::now();

    std::vector<std::string> files;
    files.reserve(runs);
    for (int i = 0; i < runs; ++i)
        files.push_back("run_" + std::to_string(i) + ".bin");

    size_t mergeReadBudget = std::max<size_t>(256ull * 1024, memBytes / 2);
    size_t mergeOutBuf     = 8ull * 1024 * 1024;

    mergeFiles(files, "output.bin", mergeReadBudget, mergeOutBuf);

    auto m_end = std::chrono::high_resolution_clock::now();
    t_merge = wall_sec(m_start, m_end);

    auto wall_end = std::chrono::high_resolution_clock::now();
    double total_wall = wall_sec(wall_start, wall_end);

    std::cout << "\nDetailed FastFlow Results:\n";
    std::cout << "Workers:         " << workers << "\n";
    std::cout << "Runs:            " << runs << "\n";
    std::cout << "Read Time:       " << t_read  << "s\n";
    std::cout << "Sort Work(sum):  " << t_sort  << "s\n";
    std::cout << "Write Work(sum): " << t_write << "s\n";
    std::cout << "NOTE: Work(sum) is NOT wall time (workers overlap)\n";   
    std::cout << "Phase1 Wall:     " << t_phase1_wall << "s\n";
    std::cout << "Merge Time:      " << t_merge << "s\n";
    std::cout << "Total Wall Time: " << total_wall << "s\n";

    verifyOutput("output.bin", nRec);
    return 0;
}
