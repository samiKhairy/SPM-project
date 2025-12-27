#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <cstdint>
#include <queue>
#include <string>
#include <memory>
#include <chrono>
#include <omp.h>
#include <filesystem>
#include "../tools/verifier.hpp"
#include "../tools/record_io.hpp"

const int OMP_THRESHOLD = 10000;
const int MERGE_THRESHOLD = 5000;

struct MergeElement
{
    uint64_t key;
    uint32_t len;
    std::vector<char> payload;
    int runID;
    bool operator>(const MergeElement &other) const { return key > other.key; }
};

// Global Timers (Atomic for parallel update safety)
// Note: In pipeline, these will overlap, so SUM might exceed Wall Time.
double total_read_t = 0;
double total_sort_t = 0;
double total_write_t = 0;

// --- SORTING LOGIC ---

// Binary search to find the split point for Parallel Merge
int binarySearch(uint64_t val, size_t *offsets, const std::vector<char> &buffer, int l, int r)
{
    while (l <= r)
    {
        int mid = l + (r - l) / 2;
        if (recordio::keyAt(buffer, offsets[mid]) <= val)
            l = mid + 1;
        else
            r = mid - 1;
    }
    return l;
}

// Parallel Merge Algorithm
void parallelMerge(size_t *offsets, size_t *temp, const std::vector<char> &buffer,
                   int l1, int r1, int l2, int r2, int out_pos)
{
    int n1 = r1 - l1 + 1;
    int n2 = r2 - l2 + 1;

    if (n1 < n2)
    {
        std::swap(l1, l2);
        std::swap(r1, r2);
        std::swap(n1, n2);
    }
    if (n1 <= MERGE_THRESHOLD)
    {
        int i = l1, j = l2, k = out_pos;
        while (i <= r1 && j <= r2)
        {
            if (recordio::keyAt(buffer, offsets[i]) <= recordio::keyAt(buffer, offsets[j]))
                temp[k++] = offsets[i++];
            else
                temp[k++] = offsets[j++];
        }
        while (i <= r1)
            temp[k++] = offsets[i++];
        while (j <= r2)
            temp[k++] = offsets[j++];
        return;
    }

    int m1 = l1 + n1 / 2;
    uint64_t pivot = recordio::keyAt(buffer, offsets[m1]);
    int m2 = binarySearch(pivot, offsets, buffer, l2, r2);
    int m3 = out_pos + (m1 - l1) + (m2 - l2);

    temp[m3] = offsets[m1];

#pragma omp task shared(offsets, temp, buffer)
    parallelMerge(offsets, temp, buffer, l1, m1 - 1, l2, m2 - 1, out_pos);
#pragma omp task shared(offsets, temp, buffer)
    parallelMerge(offsets, temp, buffer, m1 + 1, r1, m2, r2, m3 + 1);
#pragma omp taskwait
}

void parallelMergeSort(size_t *offsets, size_t *temp, const std::vector<char> &buffer, int l, int r, bool to_temp)
{
    if (r - l < OMP_THRESHOLD)
    {
        std::sort(offsets + l, offsets + r + 1, [&](size_t a, size_t b)
                  { return recordio::keyAt(buffer, a) < recordio::keyAt(buffer, b); });
        if (to_temp)
        {
            for (int i = l; i <= r; i++)
                temp[i] = offsets[i];
        }
        return;
    }

    int m = l + (r - l) / 2;
#pragma omp task shared(offsets, temp, buffer)
    parallelMergeSort(offsets, temp, buffer, l, m, !to_temp);
#pragma omp task shared(offsets, temp, buffer)
    parallelMergeSort(offsets, temp, buffer, m + 1, r, !to_temp);
#pragma omp taskwait

    if (to_temp)
        parallelMerge(offsets, temp, buffer, l, m, m + 1, r, l);
    else
        parallelMerge(temp, offsets, buffer, l, m, m + 1, r, l);
}

// Task Function: Takes ownership of buffer/offsets
void processRun(std::vector<char> *bufferPtr, std::vector<size_t> *offsetsPtr, int runID)
{
    // 1. Sort Phase
    double s_start = omp_get_wtime();
    std::vector<size_t> temp_offsets(offsetsPtr->size());

    // We are already inside a task, but parallelMergeSort creates sub-tasks.
    // Standard OMP behavior handles recursive task generation fine.
    parallelMergeSort(offsetsPtr->data(), temp_offsets.data(), *bufferPtr, 0, offsetsPtr->size() - 1, false);

    // Atomic update for global stats (since multiple tasks run at once)
    double sort_dur = omp_get_wtime() - s_start;
#pragma omp atomic
    total_sort_t += sort_dur;

#pragma omp critical(disk_io)
    {

        // 2. Write Phase
        double w_start = omp_get_wtime();
        std::string name = "run_" + std::to_string(runID) + ".bin";
        std::ofstream out(name, std::ios::binary);
        for (size_t offset : *offsetsPtr)
        {
            recordio::writeAt(out, *bufferPtr, offset);
        }
        out.close();

        double write_dur = omp_get_wtime() - w_start;
        total_write_t += write_dur;
    }

    // Cleanup
    delete bufferPtr;
    delete offsetsPtr;
}

// --- PIPELINE GENERATION ---
int generateRuns(const std::string &inputPath, size_t memoryLimit)
{
    std::ifstream inFile(inputPath, std::ios::binary);
    int runID = 0;

// OPTIMIZATION B: TASK PIPELINE
// Main Thread = Producer (Reads)
// Worker Threads = Consumers (Sort & Write via Tasks)
#pragma omp parallel
    {
#pragma omp single
        {
            while (inFile.peek() != EOF)
            {
                // Allocate NEW buffers for each task (Task ownership)
                auto buffer = new std::vector<char>();
                auto offsets = new std::vector<size_t>();
                buffer->reserve(memoryLimit); // Pre-allocate approx

                double r_start = omp_get_wtime();

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
                    size_t start = recordio::appendRecord(*buffer, key, len, inFile);
                    offsets->push_back(start);
                    currentMem += totalSize;
                    if (inFile.peek() == EOF)
                        break;
                }

                double read_dur = omp_get_wtime() - r_start;
#pragma omp atomic
                total_read_t += read_dur;

                if (!offsets->empty())
                {
// Spawn Task to process this block
// Pass pointers to transfer ownership
#pragma omp task firstprivate(buffer, offsets, runID)
                    processRun(buffer, offsets, runID);

                    runID++;
                }
                else
                {
                    delete buffer;
                    delete offsets;
                }
            } // End While
        } // End Single (Implicit Barrier waits for all tasks to finish)
    } // End Parallel

    return runID;
}

// --- PHASE 2: MULTI-WAY MERGE ---
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
        if (recordio::readRecord(*f, el.key, el.len, el.payload))
            pq.push(std::move(el));
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
        if (recordio::readRecord(*files[id], next.key, next.len, next.payload))
            pq.push(std::move(next));
    }
    for (int i = 0; i < numRuns; ++i)
    {
        files[i]->close();
        std::remove(("run_" + std::to_string(i) + ".bin").c_str());
    }
}

int main(int argc, char *argv[])
{
    if (argc < 6)
        return 1;
    std::string rawFile = argv[1];
    size_t numRecords = std::stoull(argv[2]);
    uint32_t payloadMax = std::stoul(argv[3]);
    size_t memLimitBytes = std::stoull(argv[4]) * 1024 * 1024;
    int numThreads = std::stoi(argv[5]);

    omp_set_num_threads(numThreads);

    if (!std::filesystem::exists(rawFile))
    {
        std::cerr << "Error: Input file '" << rawFile << "' not found." << std::endl;
        return 1;
    }

    auto start_all = std::chrono::high_resolution_clock::now();
    int runs = generateRuns(rawFile, memLimitBytes);

    double m_start = omp_get_wtime();
    mergeRuns(runs, "output.bin");
    double total_merge_t = omp_get_wtime() - m_start;

    auto end_all = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> total_diff = end_all - start_all;

    std::cout << "\nDetailed Results (Threads: " << numThreads << ", Records: " << numRecords << ", MemoryLimitByte: " << memLimitBytes / 1024 / 1024 << " ,PayloadMax: " << payloadMax << "  ):" << std::endl;
    std::cout << "\nDetailed Results (Threads: " << numThreads << "):" << std::endl;

    std::cout << "Read Time:  " << total_read_t << "s" << std::endl;

    // --- UPDATED SORT TIME LOG ---
    // Dividing by numThreads to display Effective Wall-Clock Time
    std::cout << "Sort Time:  " << (total_sort_t / numThreads) << "s (Effective Wall-Clock)" << std::endl;

    std::cout << "Write Time: " << total_write_t << "s" << std::endl;
    std::cout << "Merge Time: " << total_merge_t << "s" << std::endl;
    std::cout << "Total Time: " << total_diff.count() << "s" << std::endl;

    verifyOutput("output.bin", numRecords);

    return 0;
}