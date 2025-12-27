#include <iostream>
#include <vector>
#include <algorithm>
#include <fstream>
#include <cstdint>
#include <string>
#include <ff/ff.hpp>
#include <ff/farm.hpp>
// Include FastFlow buffer for the pool
#include <ff/buffer.hpp>
#include <filesystem>
#include <queue>
#include <atomic>
#include <chrono>
#include <mpi.h>
#include "../tools/verifier.hpp"
#include "../tools/record_io.hpp"

using namespace ff;

// --- GLOBAL METRICS ---
std::atomic<long long> g_read_ns{0};
std::atomic<long long> g_sort_ns{0};
std::atomic<long long> g_write_ns{0};

// --- DATA STRUCTURES ---
struct SortTask
{
    std::vector<char> rawBlock;
    std::vector<size_t> offsets;
    int runID;
};

struct MergeElement
{
    uint64_t key;
    uint32_t len;
    std::vector<char> payload;
    int sourceID;
    bool operator>(const MergeElement &o) const { return key > o.key; }
};

// --- BUFFER POOL ---
// Shared queue for recycling tasks between Collector and Emitter
typedef SWSR_Ptr_Buffer pool_t;

// --- EMITTER: STRICT PARTITIONING ---
struct Emitter : ff_node_t<SortTask>
{
    std::string inputPath;
    size_t blockSize;
    int *totalRunsPtr;
    size_t startOffset, endOffset;
    pool_t *pool; // Pool reference

    Emitter(std::string path, size_t memBytes, int *runs, size_t start, size_t end, pool_t *p)
        : inputPath(path), blockSize(memBytes), totalRunsPtr(runs), startOffset(start), endOffset(end), pool(p) {}

    SortTask *svc(SortTask *) override
    {
        std::ifstream inFile(inputPath, std::ios::binary);
        if (!inFile)
            return EOS;
        inFile.unsetf(std::ios::skipws);

        // Seek to the PRE-CALCULATED safe boundary
        inFile.seekg(startOffset, std::ios::beg);

        int runID = 0;
        size_t currentPos = startOffset;

        while (currentPos < endOffset)
        {
            size_t remaining = endOffset - currentPos;
            size_t toRead = std::min(blockSize, remaining);

            if (toRead == 0)
                break;

            // OPTIMIZATION A: BUFFER REUSE
            SortTask *task = nullptr;
            void *recycled = nullptr;

            if (pool->pop(&recycled))
            {
                task = static_cast<SortTask *>(recycled);
                task->rawBlock.clear();
                task->offsets.clear();
            }
            else
            {
                task = new SortTask();
                task->rawBlock.reserve(blockSize);
            }

            task->rawBlock.resize(toRead);

            auto start = std::chrono::high_resolution_clock::now();
            inFile.read(task->rawBlock.data(), toRead);
            g_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start).count();

            size_t bytesRead = inFile.gcount();

            if (bytesRead == 0)
            {
                // Recycle or delete
                if (!pool->push(task))
                    delete task;
                break;
            }

            if (bytesRead < toRead)
            {
                task->rawBlock.resize(bytesRead);
            }
            else
            {
                size_t pos = 0, lastValidEnd = 0;
                while (pos + recordio::kHeaderSize <= bytesRead)
                {
                    uint32_t len = recordio::lenAt(task->rawBlock, pos);
                    if (pos + recordio::kHeaderSize + len > bytesRead)
                        break;
                    pos += recordio::kHeaderSize + len;
                    lastValidEnd = pos;
                }

                if (lastValidEnd == 0)
                {
                    // Should not happen with valid data/boundaries
                    if (!pool->push(task))
                        delete task;
                    break;
                }

                long backtrack = bytesRead - lastValidEnd;
                inFile.seekg(-backtrack, std::ios::cur);
                task->rawBlock.resize(lastValidEnd);
            }

            if (!task->rawBlock.empty())
            {
                currentPos += task->rawBlock.size();
                task->runID = runID++;
                ff_send_out(task);
            }
            else
            {
                if (!pool->push(task))
                    delete task;
            }
        }
        *totalRunsPtr = runID;
        return EOS;
    }
};

struct Worker : ff_node_t<SortTask>
{
    SortTask *svc(SortTask *task) override
    {
        auto start = std::chrono::high_resolution_clock::now();
        size_t pos = 0, limit = task->rawBlock.size();

        // Reserve only if needed to preserve capacity
        if (task->offsets.capacity() < limit / 64)
        {
            task->offsets.reserve(limit / 64);
        }

        while (pos < limit)
        {
            task->offsets.push_back(pos);
            uint32_t len = recordio::lenAt(task->rawBlock, pos);
            pos += recordio::kHeaderSize + len;
        }
        std::sort(task->offsets.begin(), task->offsets.end(), [&](size_t a, size_t b)
                  { return recordio::keyAt(task->rawBlock, a) < recordio::keyAt(task->rawBlock, b); });
        g_sort_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start).count();
        return task;
    }
};

struct Collector : ff_node_t<SortTask>
{
    int myRank;
    pool_t *pool;

    Collector(int rank, pool_t *p) : myRank(rank), pool(p) {}

    SortTask *svc(SortTask *task) override
    {
        auto start = std::chrono::high_resolution_clock::now();
        std::string name = "run_" + std::to_string(myRank) + "_" + std::to_string(task->runID) + ".bin";
        std::ofstream out(name, std::ios::binary);
        for (size_t offset : task->offsets)
        {
            recordio::writeAt(out, task->rawBlock, offset);
        }

        // Explicit flush close for accurate timing
        out.close();

        g_write_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start).count();

        // OPTIMIZATION A: BUFFER REUSE
        if (!pool->push(task))
        {
            delete task;
        }

        return GO_ON;
    }
};

void mergeFiles(const std::vector<std::string> &inputFiles, const std::string &outPath)
{
    std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> pq;
    std::vector<std::unique_ptr<std::ifstream>> files;
    std::ofstream out(outPath, std::ios::binary);

    auto readElem = [](std::ifstream *f, MergeElement &e)
    {
        {
            return recordio::readRecord(*f, e.key, e.len, e.payload);
        };
    };

    for (size_t i = 0; i < inputFiles.size(); ++i)
    {
        auto f = std::make_unique<std::ifstream>(inputFiles[i], std::ios::binary);
        if (!f->is_open())
            continue;
        MergeElement e;
        e.sourceID = i;
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
        next.sourceID = top.sourceID;
        if (readElem(files[next.sourceID].get(), next))
            pq.push(next);
    }

    for (size_t i = 0; i < inputFiles.size(); ++i)
    {
        if (files[i])
            files[i]->close();
        std::filesystem::remove(inputFiles[i]);
    }
}

// --- MAIN ---
int main(int argc, char *argv[])
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);

    auto start_all = std::chrono::high_resolution_clock::now();

    int myRank, numProcs;
    MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);

    if (argc < 5)
    {
        if (myRank == 0)
            std::cerr << "Usage: ./prog <input> <nRec> <pMax> <memMB> <workers>" << std::endl;
        MPI_Finalize();
        return 1;
    }

    std::string file = argv[1];
    size_t nRec = std::stoull(argv[2]);
    size_t memLimitBytes = std::stoull(argv[4]) * 1024 * 1024;
    int nWorkers = (argc > 5) ? std::stoi(argv[5]) : 4;

    size_t fileSize = std::filesystem::file_size(file);

    // --- ROBUST PARTITIONING (Scanner) ---
    // Rank 0 scans headers to find EXACT record boundaries
    std::vector<size_t> boundaries(numProcs + 1);

    if (myRank == 0)
    {
        std::ifstream scanFile(file, std::ios::binary);
        boundaries[0] = 0;
        boundaries[numProcs] = fileSize;

        size_t currentPos = 0;
        int boundaryIdx = 1;

        while (boundaryIdx < numProcs && scanFile.peek() != EOF)
        {
            size_t target = (fileSize / numProcs) * boundaryIdx;
            while (currentPos < target)
            {
                uint64_t k;
                uint32_t len;
                if (!scanFile.read((char *)&k, 8))
                    break;
                if (!scanFile.read((char *)&len, 4))
                    break;
                scanFile.seekg(len, std::ios::cur);
                currentPos += (recordio::kHeaderSize + len);
            }
            boundaries[boundaryIdx] = currentPos;
            boundaryIdx++;
        }
        while (boundaryIdx < numProcs)
            boundaries[boundaryIdx++] = fileSize;
    }

    MPI_Bcast(boundaries.data(), (numProcs + 1) * sizeof(size_t), MPI_BYTE, 0, MPI_COMM_WORLD);

    size_t myStart = boundaries[myRank];
    size_t myEnd = boundaries[myRank + 1];

    int totalRuns = 0;

    // --- INIT POOL ---
    pool_t pool(64);
    if (pool.init() < 0)
    {
        if (myRank == 0)
            std::cerr << "Pool init error" << std::endl;
        MPI_Finalize();
        return -1;
    }

    // --- PHASE 1: LOCAL SORT ---
    // Pass pool to Emitter and Collector
    Emitter emitter(file, memLimitBytes, &totalRuns, myStart, myEnd, &pool);
    Collector collector(myRank, &pool);

    std::vector<std::unique_ptr<ff_node>> workers;
    for (int i = 0; i < nWorkers; ++i)
        workers.push_back(std::make_unique<Worker>());

    ff_Farm<SortTask> farm(std::move(workers), emitter, collector);
    farm.set_scheduling_ondemand();

    if (farm.run_and_wait_end() < 0)
        MPI_Abort(MPI_COMM_WORLD, -1);

    // Cleanup Pool
    void *p;
    while (pool.pop(&p))
    {
        delete static_cast<SortTask *>(p);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // --- PHASE 2: TREE MERGE ---
    auto merge_start = std::chrono::high_resolution_clock::now();

    std::vector<std::string> myFiles;
    for (int i = 0; i < totalRuns; ++i)
    {
        myFiles.push_back("run_" + std::to_string(myRank) + "_" + std::to_string(i) + ".bin");
    }

    int step = 1;
    while (step < numProcs)
    {
        if (myRank % (2 * step) == 0)
        {
            int sourceRank = myRank + step;
            if (sourceRank < numProcs)
            {
                int sourceRunCount;
                MPI_Recv(&sourceRunCount, 1, MPI_INT, sourceRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for (int i = 0; i < sourceRunCount; ++i)
                {
                    myFiles.push_back("run_" + std::to_string(sourceRank) + "_" + std::to_string(i) + ".bin");
                }

                std::string mergedName = "run_" + std::to_string(myRank) + "_merged_" + std::to_string(step) + ".bin";
                mergeFiles(myFiles, mergedName);
                myFiles.clear();
                myFiles.push_back(mergedName);
            }
        }
        else
        {
            int targetRank = myRank - step;
            int count = myFiles.size();
            MPI_Send(&count, 1, MPI_INT, targetRank, 0, MPI_COMM_WORLD);
            myFiles.clear();
            break;
        }
        step *= 2;
    }

    if (myRank == 0 && !myFiles.empty())
    {
        try
        {
            if (myFiles.size() > 1)
            {
                mergeFiles(myFiles, "output.bin");
            }
            else
            {
                if (std::filesystem::exists("output.bin"))
                    std::filesystem::remove("output.bin");
                std::filesystem::rename(myFiles[0], "output.bin");
            }
            std::cout << "Sort Complete. Output: output.bin" << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cerr << "Merge Error: " << e.what() << std::endl;
        }
    }

    auto merge_end = std::chrono::high_resolution_clock::now();
    auto end_all = std::chrono::high_resolution_clock::now();

    // --- METRICS ---
    long long local_read = g_read_ns, local_sort = g_sort_ns, local_write = g_write_ns;
    long long global_read = 0, global_sort = 0, global_write = 0;

    MPI_Reduce(&local_read, &global_read, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_sort, &global_sort, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_write, &global_write, 1, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (myRank == 0)
    {
        // --- METRIC FIX START ---
        // Calculate Total Workers in the entire cluster
        int total_cluster_workers = nWorkers * numProcs;

        // Convert total CPU time to seconds
        double total_sort_cpu_time = global_sort / 1e9;

        // Calculate Effective Wall-Clock Time
        double sort_wall_time = total_sort_cpu_time / total_cluster_workers;
        // --- METRIC FIX END ---

        std::cout << "\nDetailed Results (MPI Ranks: " << numProcs << ", Workers/Rank: " << nWorkers << "):" << std::endl;
        std::cout << "Read Time:  " << (global_read / 1e9) << "s" << std::endl;

        // UPDATED LOG
        std::cout << "Sort Time:  " << sort_wall_time << "s" << std::endl;

        std::cout << "Write Time: " << (global_write / 1e9) << "s" << std::endl;
        std::cout << "Merge Time: " << std::chrono::duration<double>(merge_end - merge_start).count() << "s" << std::endl;
        std::cout << "Total Time: " << std::chrono::duration<double>(end_all - start_all).count() << "s" << std::endl;

        std::cout << "\n";
        verifyOutput("output.bin", nRec);
    }

    MPI_Finalize();
    return 0;
}
