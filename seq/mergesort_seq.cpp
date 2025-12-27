#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <cstdint>
#include <queue>
#include <string>
#include <memory>
#include <chrono>
#include <filesystem>
#include "../tools/verifier.hpp" // Shared Verifier
#include "../tools/record_io.hpp"

// Global timing variables for bottleneck analysis
double total_read_t = 0.0;
double total_sort_t = 0.0;
double total_write_t = 0.0;
double total_merge_t = 0.0;

struct MergeElement
{
    uint64_t key;
    uint32_t len;
    std::vector<char> payload;
    int runID;

    bool operator>(const MergeElement &other) const
    {
        return key > other.key;
    }
};

// --- PHASE 1: RUN GENERATION ---
void writeSortedRun(std::vector<char> &buffer, std::vector<size_t> &offsets, int runID)
{
    // INSTRUMENTATION: Sort timing
    auto s_start = std::chrono::high_resolution_clock::now();
    std::sort(offsets.begin(), offsets.end(), [&](size_t a, size_t b)
              { return recordio::keyAt(buffer, a) < recordio::keyAt(buffer, b); });
    total_sort_t += std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - s_start).count();

    // INSTRUMENTATION: Write timing
    auto w_start = std::chrono::high_resolution_clock::now();
    std::string name = "run_" + std::to_string(runID) + ".bin";
    std::ofstream out(name, std::ios::binary);

    for (size_t offset : offsets)
    {
        recordio::writeAt(out, buffer, offset);
    }
    out.close();
    total_write_t += std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - w_start).count();
}

int generateRuns(const std::string &inputPath, size_t memoryLimit)
{
    std::ifstream inFile(inputPath, std::ios::binary);
    std::vector<char> buffer;
    std::vector<size_t> offsets;
    int runID = 0;

    while (inFile.peek() != EOF)
    {
        // INSTRUMENTATION: Read timing
        auto r_start = std::chrono::high_resolution_clock::now();
        buffer.clear();
        offsets.clear();
        size_t currentMem = 0;

        while (currentMem < memoryLimit)
        {
            size_t posBeforeRecord = inFile.tellg();
            uint64_t key;
            uint32_t len;

            if (!inFile.read(reinterpret_cast<char *>(&key), 8))
                break;
            if (!inFile.read(reinterpret_cast<char *>(&len), 4))
                break;

            size_t totalSize = recordio::kHeaderSize + len;
            if (currentMem + totalSize > memoryLimit)
            {
                inFile.clear();
                inFile.seekg(posBeforeRecord);
                break;
            }

            size_t start = recordio::appendRecord(buffer, key, len, inFile);
            offsets.push_back(start);

            currentMem += totalSize;
            if (inFile.peek() == EOF)
                break;
        }
        total_read_t += std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - r_start).count();

        if (!offsets.empty())
        {
            writeSortedRun(buffer, offsets, runID++);
        }
    }
    return runID;
}

// --- PHASE 2: MULTI-WAY MERGE ---
bool readNextRecord(std::ifstream &f, MergeElement &el)
{
    return recordio::readRecord(f, el.key, el.len, el.payload);
}

void mergeRuns(int numRuns, const std::string &outPath)
{
    std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> pq;
    std::vector<std::unique_ptr<std::ifstream>> files;
    std::ofstream out(outPath, std::ios::binary);

    for (int i = 0; i < numRuns; ++i)
    {
        auto f = std::make_unique<std::ifstream>("run_" + std::to_string(i) + ".bin", std::ios::binary);
        MergeElement el;
        el.runID = i;
        if (readNextRecord(*f, el))
        {
            pq.push(std::move(el));
        }
        files.push_back(std::move(f));
    }

    while (!pq.empty())
    {
        MergeElement smallest = std::move(const_cast<MergeElement &>(pq.top()));
        pq.pop();

        recordio::writeRecord(out, smallest.key, smallest.len, smallest.payload);

        int id = smallest.runID;
        MergeElement next;
        next.runID = id;
        if (readNextRecord(*files[id], next))
        {
            pq.push(std::move(next));
        }
    }

    for (int i = 0; i < numRuns; ++i)
    {
        files[i]->close();
        std::remove(("run_" + std::to_string(i) + ".bin").c_str());
    }
}

int main(int argc, char *argv[])
{
    if (argc < 5)
    {
        std::cerr << "Usage: " << argv[0] << " <input_file> <num_records> <payload_max> <mem_limit_mb>" << std::endl;
        return 1;
    }

    std::string rawFile = argv[1];
    size_t numRecords = std::stoull(argv[2]);
    // argv[3] is payloadMax (unused in sort, but kept for arg compatibility)
    size_t memLimitBytes = std::stoull(argv[4]) * 1024 * 1024;
    std::string outFile = "output.bin";

    // 1. Check if Input File Exists (Tool Isolation)
    if (!std::filesystem::exists(rawFile))
    {
        std::cerr << "Error: Input file '" << rawFile << "' not found. Run datagen first." << std::endl;
        return 1;
    }

    // INSTRUMENTATION: Start of total sort process
    auto start_all = std::chrono::high_resolution_clock::now();

    int runs = generateRuns(rawFile, memLimitBytes);
    std::cout << "[Step 1] Total Runs created: " << runs << std::endl;

    // INSTRUMENTATION: Merge timing
    auto m_start = std::chrono::high_resolution_clock::now();
    mergeRuns(runs, outFile);
    total_merge_t = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - m_start).count();

    auto end_all = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> total_diff = end_all - start_all;

    // FINAL STANDARDIZED REPORTING
    std::cout << "\nDetailed Sequential Results:" << std::endl;
    std::cout << "Read Time:   " << total_read_t << "s" << std::endl;
    std::cout << "Sort Time:   " << total_sort_t << "s" << std::endl;
    std::cout << "Write Time:  " << total_write_t << "s" << std::endl;
    std::cout << "Merge Time:  " << total_merge_t << "s" << std::endl;
    std::cout << "Total Time:  " << total_diff.count() << "s" << std::endl;

    // 2. VERIFY using Shared Header
    verifyOutput(outFile, numRecords);

    return 0;
}