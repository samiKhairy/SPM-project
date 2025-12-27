#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <cstdint>
#include <string>
#include <ff/ff.hpp>
#include <ff/farm.hpp>
// Include FastFlow bounded queue for the pool
#include <ff/buffer.hpp>
#include <filesystem>
#include <queue>
#include <atomic>
#include <chrono>
#include "../tools/verifier.hpp"
#include "../tools/record_io.hpp"

using namespace ff;

// --- GLOBAL METRICS ---
std::atomic<long long> g_read_ns{0};
std::atomic<long long> g_sort_ns{0};
std::atomic<long long> g_write_ns{0};

struct SortTask
{
    std::vector<char> rawBlock;
    std::vector<size_t> offsets;
    int runID;
};

// --- BUFFER POOL ---
// Shared queue for recycling tasks between Collector (Producer of free tasks)
// and Emitter (Consumer of free tasks).
// Size 32 is sufficient as we only have ~16 workers max + buffering.
typedef SWSR_Ptr_Buffer pool_t;

// --- EMITTER: READS ONLY ---
struct Emitter : ff_node_t<SortTask>
{
    std::string inputPath;
    size_t blockSize;
    int *totalRunsPtr;
    pool_t *pool; // Reference to the recycling pool

    Emitter(std::string path, size_t memBytes, int *runs, pool_t *p)
        : inputPath(path), blockSize(memBytes), totalRunsPtr(runs), pool(p) {}

    SortTask *svc(SortTask *) override
    {
        std::ifstream inFile(inputPath, std::ios::binary);
        inFile.unsetf(std::ios::skipws);

        int runID = 0;
        while (inFile.peek() != EOF)
        {
            SortTask *task = nullptr;
            void *recycled = nullptr;

            // OPTIMIZATION A: BUFFER REUSE
            // Try to pop a used task from the pool
            if (pool->pop(&recycled))
            {
                task = static_cast<SortTask *>(recycled);
                // Clear vectors but KEEP CAPACITY
                task->rawBlock.clear();
                task->offsets.clear();
            }
            else
            {
                // Pool empty (start of execution), allocate new
                task = new SortTask();
                task->rawBlock.reserve(blockSize);
            }

            // Ensure size is correct for reading
            task->rawBlock.resize(blockSize);

            // INSTRUMENTATION: READ
            auto start = std::chrono::high_resolution_clock::now();
            inFile.read(task->rawBlock.data(), blockSize);
            auto end = std::chrono::high_resolution_clock::now();
            g_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

            size_t bytesRead = inFile.gcount();

            if (bytesRead < blockSize)
            {
                task->rawBlock.resize(bytesRead);
            }
            else
            {
                size_t pos = 0;
                size_t lastValidEnd = 0;

                while (pos + recordio::kHeaderSize <= bytesRead)
                {
                    uint32_t len = recordio::lenAt(task->rawBlock, pos);
                    size_t recSize = recordio::kHeaderSize + len;
                    if (pos + recSize > bytesRead)
                        break;
                    pos += recSize;
                    lastValidEnd = pos;
                }

                if (lastValidEnd == 0 && bytesRead > 0)
                {
                    std::cerr << "Error: Record larger than Memory Limit or File Corrupted!" << std::endl;
                    // Don't recycle corrupted tasks, just delete
                    delete task;
                    return EOS;
                }

                long backtrack = bytesRead - lastValidEnd;

                // INSTRUMENTATION: SEEK
                auto s_start = std::chrono::high_resolution_clock::now();
                inFile.seekg(-backtrack, std::ios::cur);
                auto s_end = std::chrono::high_resolution_clock::now();
                g_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(s_end - s_start).count();

                task->rawBlock.resize(lastValidEnd);
            }

            if (!task->rawBlock.empty())
            {
                task->runID = runID++;
                ff_send_out(task);
            }
            else
            {
                // Push back to pool if we read nothing (rare edge case)
                pool->push(task);
            }
        }
        *totalRunsPtr = runID;
        return EOS;
    }
};

// --- WORKER: CPU ONLY ---
struct Worker : ff_node_t<SortTask>
{
    SortTask *svc(SortTask *task) override
    {
        auto start = std::chrono::high_resolution_clock::now();

        size_t pos = 0;
        size_t limit = task->rawBlock.size();

        // Reserve only if capacity is insufficient (reusing vector logic)
        if (task->offsets.capacity() < limit / 64)
        {
            task->offsets.reserve(limit / 64);
        }

        while (pos < limit)
        {
            task->offsets.push_back(pos);
            uint32_t len = recordio::lenAt(task->rawBlock, pos);
            pos += (recordio::kHeaderSize + len);
        }

        std::sort(task->offsets.begin(), task->offsets.end(), [&](size_t a, size_t b)
                  { return recordio::keyAt(task->rawBlock, a) < recordio::keyAt(task->rawBlock, b); });

        auto end = std::chrono::high_resolution_clock::now();
        g_sort_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

        return task;
    }
};

// --- COLLECTOR: WRITES ONLY ---
struct Collector : ff_node_t<SortTask>
{
    pool_t *pool;

    Collector(pool_t *p) : pool(p) {}

    SortTask *svc(SortTask *task) override
    {
        auto start = std::chrono::high_resolution_clock::now();

        std::string name = "run_" + std::to_string(task->runID) + ".bin";
        std::ofstream out(name, std::ios::binary);

        // Write loop
        for (size_t offset : task->offsets)
        {
            recordio::writeAt(out, task->rawBlock, offset);
        }

        // --- THE FIX ---
        // Explicitly close here to force the OS to flush the buffer to disk
        // while the timer is still running.
        out.close();

        auto end = std::chrono::high_resolution_clock::now();

        // Now this metric includes the actual I/O time, matching OpenMP
        g_write_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

        // Buffer Reuse Logic
        if (!pool->push(task))
        {
            delete task;
        }

        return GO_ON;
    }
};

// --- MERGE LOGIC ---
struct MergeElement
{
    uint64_t key;
    uint32_t len;
    std::vector<char> payload;
    int runID;
    bool operator>(const MergeElement &o) const { return key > o.key; }
};

void mergeRuns(int numRuns, const std::string &outPath)
{
    std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> pq;
    std::vector<std::unique_ptr<std::ifstream>> files;
    std::ofstream out(outPath, std::ios::binary);

    auto readElem = [&](std::ifstream *f, MergeElement &e)
    {
        return recordio::readRecord(*f, e.key, e.len, e.payload);
    };

    for (int i = 0; i < numRuns; ++i)
    {
        auto f = std::make_unique<std::ifstream>("run_" + std::to_string(i) + ".bin", std::ios::binary);
        MergeElement e;
        e.runID = i;
        if (readElem(f.get(), e))
            pq.push(e);
        files.push_back(std::move(f));
    }

    while (!pq.empty())
    {
        auto top = pq.top();
        pq.pop();
        recordio::writeRecord(out, top.key, top.len, top.payload);

        MergeElement next;
        next.runID = top.runID;
        if (readElem(files[top.runID].get(), next))
            pq.push(next);
    }

    for (int i = 0; i < numRuns; ++i)
    {
        files[i]->close();
        std::filesystem::remove("run_" + std::to_string(i) + ".bin");
    }
}

int main(int argc, char *argv[])
{
    if (argc < 5)
        return 1;
    std::string file = argv[1];
    size_t nRec = std::stoull(argv[2]);
    uint32_t pMax = std::stoul(argv[3]);
    size_t memLimitBytes = std::stoull(argv[4]) * 1024 * 1024;
    int nWorkers = (argc > 5) ? std::stoi(argv[5]) : 4;

    if (!std::filesystem::exists(file))
    {
        std::cerr << "Error: Input file '" << file << "' not found." << std::endl;
        return 1;
    }

    g_read_ns = 0;
    g_sort_ns = 0;
    g_write_ns = 0;

    int totalRuns = 0;
    auto start_all = std::chrono::high_resolution_clock::now();

    // Setup Pool (Size 64 covers all workers + buffering)
    pool_t pool(64);
    if (pool.init() < 0)
    {
        std::cerr << "Error initializing pool" << std::endl;
        return -1;
    }

    // Pass pool to Emitter and Collector
    Emitter emitter(file, memLimitBytes, &totalRuns, &pool);
    Collector collector(&pool);

    std::vector<std::unique_ptr<ff_node>> workers;
    for (int i = 0; i < nWorkers; ++i)
        workers.push_back(std::make_unique<Worker>());

    ff_Farm<SortTask> farm(std::move(workers), emitter, collector);
    farm.set_scheduling_ondemand();

    if (farm.run_and_wait_end() < 0)
        return -1;

    // Cleanup: Drain pool if anything is left (to avoid Valgrind noise)
    void *p;
    while (pool.pop(&p))
    {
        delete static_cast<SortTask *>(p);
    }

    auto merge_start = std::chrono::high_resolution_clock::now();
    mergeRuns(totalRuns, "output.bin");
    auto merge_end = std::chrono::high_resolution_clock::now();

    auto end_all = std::chrono::high_resolution_clock::now();

    double merge_t = std::chrono::duration<double>(merge_end - merge_start).count();
    double total_t = std::chrono::duration<double>(end_all - start_all).count();

    // --- METRIC FIX START ---
    double total_sort_cpu_time = g_sort_ns / 1e9;
    double sort_wall_time = total_sort_cpu_time / nWorkers;
    // --- METRIC FIX END ---

    std::cout << "\nDetailed Results (Workers: " << nWorkers << ", Memory: " << (memLimitBytes / 1024 / 1024) << "MB):" << std::endl;
    std::cout << "Read Time:  " << (g_read_ns / 1e9) << "s" << std::endl;

    // UPDATED LOG
    std::cout << "Sort Time:  " << sort_wall_time << "s" << std::endl;

    std::cout << "Write Time: " << (g_write_ns / 1e9) << "s" << std::endl;
    std::cout << "Merge Time: " << merge_t << "s" << std::endl;
    std::cout << "Total Time: " << total_t << "s" << std::endl;

    verifyOutput("output.bin", nRec);

    return 0;
}