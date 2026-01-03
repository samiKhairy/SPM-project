// mergesort_seq.cpp - clean baseline (sequential external mergesort)

#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <string>
#include <chrono>
#include <filesystem>

#include "../tools/verifier.hpp"
#include "../tools/record_io.hpp"
#include "../tools/merger.hpp"

// Global Timers (Wall-Clock Only)
double t_read = 0.0, t_sort = 0.0, t_write = 0.0, t_merge = 0.0;

static inline double wall_sec(std::chrono::high_resolution_clock::time_point a,
                              std::chrono::high_resolution_clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

int generateRuns(const std::string &inputPath, size_t memoryLimitBytes)
{
    std::ifstream inFile(inputPath, std::ios::binary);
    if (!inFile) return 0;

    // IO buffering (simple)
    std::vector<char> ioBuf(1 << 20);
    inFile.rdbuf()->pubsetbuf(ioBuf.data(), ioBuf.size());

    std::vector<char> rawBlock;
    std::vector<size_t> offsets;

    rawBlock.reserve(memoryLimitBytes);
    offsets.reserve(std::max<size_t>(1, memoryLimitBytes / 64));

    int runID = 0;

    while (true)
    {
        // --- READ ---
        auto t0 = std::chrono::high_resolution_clock::now();

        rawBlock.resize(memoryLimitBytes);
        inFile.read(rawBlock.data(), (std::streamsize)memoryLimitBytes);
        size_t bytesRead = (size_t)inFile.gcount();
        if (bytesRead == 0) break;

        // Trim to full records (only needed when we filled the buffer)
        if (bytesRead < memoryLimitBytes) {
            rawBlock.resize(bytesRead);
        } else {
            size_t pos = 0, lastValidEnd = 0;
            while (pos + 12 <= bytesRead) {
                uint32_t len = recordio::lenAt(rawBlock, pos);
                size_t recSize = 12ull + (size_t)len;
                if (pos + recSize > bytesRead) break;
                pos += recSize;
                lastValidEnd = pos;
            }
            // move stream position back by the incomplete tail
            inFile.seekg((std::streamoff)lastValidEnd - (std::streamoff)bytesRead, std::ios::cur);
            rawBlock.resize(lastValidEnd);
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        t_read += wall_sec(t0, t1);

        // --- INDEX (record-safe scan) ---
        offsets.clear();
        size_t pos = 0, limit = rawBlock.size();
        while (pos + 12 <= limit) {
            uint32_t len = recordio::lenAt(rawBlock, pos);
            size_t recSize = 12ull + (size_t)len;
            if (pos + recSize > limit) break;     // don't read partial record
            offsets.push_back(pos);
            pos += recSize;
        }

        // --- SORT ---
        auto t2 = std::chrono::high_resolution_clock::now();

        std::sort(offsets.begin(), offsets.end(),
                  [&](size_t a, size_t b) {
                      return recordio::keyAt(rawBlock, a) < recordio::keyAt(rawBlock, b);
                  });

        auto t3 = std::chrono::high_resolution_clock::now();
        t_sort += wall_sec(t2, t3);

        // --- WRITE ---
        auto t4 = std::chrono::high_resolution_clock::now();

        std::string name = "run_" + std::to_string(runID++) + ".bin";
        std::ofstream out(name, std::ios::binary);
        std::vector<char> wBuf(1 << 20);
        out.rdbuf()->pubsetbuf(wBuf.data(), wBuf.size());

        for (size_t off : offsets)
            recordio::writeAt(out, rawBlock, off);

        out.close();

        auto t5 = std::chrono::high_resolution_clock::now();
        t_write += wall_sec(t4, t5);
    }

    return runID;
}

int main(int argc, char *argv[])
{
    if (argc < 5) return 1;

    std::string file = argv[1];
    size_t nRec = std::stoull(argv[2]);
    size_t memBytes = (size_t)std::stoull(argv[4]) * 1024ull * 1024ull;

    if (!std::filesystem::exists(file)) return 1;

    // --- GLOBAL WALL CLOCK START ---
    auto wall_start = std::chrono::high_resolution_clock::now();

    // Phase 1: generate sorted runs
    int runs = generateRuns(file, memBytes);

    // Phase 2: k-way merge
    auto m_start = std::chrono::high_resolution_clock::now();

    std::vector<std::string> files;
    files.reserve(runs);
    for (int i = 0; i < runs; ++i)
        files.push_back("run_" + std::to_string(i) + ".bin");

    // Simple budgets derived from memBytes (keeps it explainable)
    size_t mergeReadBudget = std::max<size_t>(256ull * 1024, memBytes / 2); // total budget for run buffers
    size_t mergeOutBuf     = 8ull * 1024 * 1024;                           // 8MB output buffer

    mergeFiles(files, "output.bin", mergeReadBudget, mergeOutBuf);

    auto m_end = std::chrono::high_resolution_clock::now();
    t_merge = wall_sec(m_start, m_end);

    // --- GLOBAL WALL CLOCK END ---
    auto wall_end = std::chrono::high_resolution_clock::now();
    double total_wall = wall_sec(wall_start, wall_end);

    std::cout << "\nDetailed Sequential Results:\n";
    std::cout << "Read Time:       " << t_read   << "s\n";
    std::cout << "Runs:            " << runs << "\n";
    std::cout << "Mem budget:      " << (memBytes / (1024ull*1024ull)) << " MB\n";
    std::cout << "Sort Time:       " << t_sort   << "s\n";
    std::cout << "Write Time:      " << t_write  << "s\n";
    std::cout << "Merge Time:      " << t_merge  << "s\n";
    std::cout << "Total Wall Time: " << total_wall << "s\n";

    verifyOutput("output.bin", nRec);
    return 0;

}

