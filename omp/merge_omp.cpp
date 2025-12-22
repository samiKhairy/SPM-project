// merge_omp.cpp - FIXED VERSION
#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <string>
#include <algorithm>
#include <filesystem>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <omp.h>
#include <iomanip>

#include "../tools/record_io.hpp"

namespace fs = std::filesystem;

static inline void append_bytes(std::vector<char> &buf, const void *src, size_t n)
{
    const size_t old = buf.size();
    buf.resize(old + n);
    std::memcpy(buf.data() + old, src, n);
}

struct RunCtx
{
    std::ifstream in;
    std::unique_ptr<recio::RecordReader> rr;
    recio::RecordView cur;
    bool has = false;
    RunCtx() = default;
};

struct HeapNode
{
    uint64_t key = 0;
    size_t run_idx = 0;
    bool operator>(const HeapNode &o) const { return key > o.key; }
};

static std::vector<std::string> list_runs(const std::string &prefix, const std::string &final_out)
{
    auto parse_run_id = [](const std::string &base, const fs::path &p, uint64_t &out_id) -> bool
    {
        const std::string name = p.filename().string();
        if (name.size() < base.size() + 1 + 4)
            return false;
        if (name.rfind(base, 0) != 0)
            return false;
        if (p.extension() != ".dat")
            return false;

        const std::string mid = name.substr(base.size(), name.size() - base.size() - 4);
        if (mid.empty())
            return false;
        for (char c : mid)
            if (c < '0' || c > '9')
                return false;
        try
        {
            out_id = std::stoull(mid);
            return true;
        }
        catch (...)
        {
            return false;
        }
    };

    std::vector<std::string> files;
    std::string dir = fs::path(prefix).parent_path().string();
    if (dir.empty())
        dir = ".";
    std::string base = fs::path(prefix).filename().string();
    const std::string final_out_abs = fs::absolute(final_out).string();

    struct Item
    {
        uint64_t id;
        std::string path;
    };
    std::vector<Item> items;

    for (const auto &p : fs::directory_iterator(dir))
    {
        if (!p.is_regular_file())
            continue;
        const std::string full_path = fs::absolute(p.path()).string();
        if (full_path == final_out_abs)
            continue;
        uint64_t id = 0;
        if (parse_run_id(base, p.path(), id))
            items.push_back({id, p.path().string()});
    }

    std::sort(items.begin(), items.end(), [](const Item &a, const Item &b)
              { return a.id < b.id; });
    files.reserve(items.size());
    for (const auto &it : items)
        files.push_back(it.path);
    return files;
}

// Returns bytes written
static uint64_t merge_k_runs(const std::vector<std::string> &inputs,
                             const std::string &output,
                             size_t buf_size,
                             uint32_t payload_max)
{
    std::vector<RunCtx> runs(inputs.size());
    std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<HeapNode>> heap;

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        runs[i].in.open(inputs[i], std::ios::binary);
        if (!runs[i].in)
        {
            std::cerr << "ERROR: Cannot open input file: " << inputs[i] << "\n";
            throw std::runtime_error("Failed to open input file");
        }
        runs[i].rr = std::make_unique<recio::RecordReader>(runs[i].in, buf_size, payload_max);
        runs[i].has = runs[i].rr->next(runs[i].cur);

        if (runs[i].has)
        {
            heap.push(HeapNode{runs[i].cur.key, i});
        }
    }

    std::ofstream out(output, std::ios::binary);
    if (!out)
    {
        std::cerr << "ERROR: Cannot create output file: " << output << "\n";
        throw std::runtime_error("Cannot create output");
    }

    const size_t OUT_LIMIT = 64 * 1024 * 1024;
    std::vector<char> out_buf;
    out_buf.reserve(OUT_LIMIT);
    uint64_t bytes_written = 0;

    while (!heap.empty())
    {
        const HeapNode top = heap.top();
        heap.pop();
        RunCtx &rc = runs[top.run_idx];

        const size_t rec_size = 12 + static_cast<size_t>(rc.cur.len);
        if (out_buf.size() + rec_size > OUT_LIMIT)
        {
            out.write(out_buf.data(), static_cast<std::streamsize>(out_buf.size()));
            if (!out.good())
            {
                std::cerr << "ERROR: Write failed to output file\n";
                throw std::runtime_error("Write failed");
            }
            out_buf.clear();
        }
        append_bytes(out_buf, &rc.cur.key, 8);
        append_bytes(out_buf, &rc.cur.len, 4);
        append_bytes(out_buf, rc.cur.payload, rc.cur.len);
        bytes_written += rec_size;

        rc.has = rc.rr->next(rc.cur);
        if (rc.has)
            heap.push(HeapNode{rc.cur.key, top.run_idx});
    }

    if (!out_buf.empty())
    {
        out.write(out_buf.data(), static_cast<std::streamsize>(out_buf.size()));
        if (!out.good())
        {
            std::cerr << "ERROR: Final write failed\n";
            throw std::runtime_error("Final write failed");
        }
    }

    out.close();
    if (!out.good())
    {
        std::cerr << "ERROR: Failed to close output file properly\n";
        throw std::runtime_error("Failed to close output");
    }

    return bytes_written;
}

static size_t clamp_size_t(size_t x, size_t lo, size_t hi)
{
    if (x < lo)
        return lo;
    if (x > hi)
        return hi;
    return x;
}

int main(int argc, char **argv)
{
    if (argc < 5)
    {
        std::cerr << "Usage: ./merge_omp <prefix> <final_out> <K> <TOTAL_MEM_GB> [payload_max]\n";
        return 1;
    }

    const std::string prefix = argv[1];
    const std::string final_out = argv[2];
    int K = std::stoi(argv[3]);
    const uint64_t total_mem_gb = std::stoull(argv[4]);

    uint32_t payload_max = (argc >= 6) ? static_cast<uint32_t>(std::stoul(argv[5])) : recio::HARD_PAYLOAD_MAX;

    std::vector<std::string> files = list_runs(prefix, final_out);
    if (files.empty())
    {
        std::cerr << "ERROR: No input files found\n";
        return 1;
    }

    // --- ADAPTIVE MEMORY & THREAD MANAGEMENT ---
    const size_t PROJECT_RAM_LIMIT = total_mem_gb * 1024ULL * 1024ULL * 1024ULL;
    const size_t SAFE_RAM = (PROJECT_RAM_LIMIT * 3) / 4; // 75% of available

    int max_threads = omp_get_max_threads();
    if (max_threads < 1)
        max_threads = 1;

    // CRITICAL FIX: Limit concurrent file handles
    // Most systems allow ~1024 file descriptors per process
    // Reserve some for OS, stdout, etc.
    const size_t MAX_SAFE_FILE_HANDLES = 800;

    // Calculate theoretical concurrent streams
    size_t theoretical_streams = static_cast<size_t>(K) * static_cast<size_t>(max_threads);

    // ADAPTIVE STRATEGY: If we exceed safe limits, reduce parallelism
    int actual_threads = max_threads;
    int actual_K = K;

    if (theoretical_streams > MAX_SAFE_FILE_HANDLES)
    {
        // Strategy 1: Reduce K if it's large
        if (K > 32)
        {
            actual_K = std::max(16, static_cast<int>(MAX_SAFE_FILE_HANDLES / max_threads));
            theoretical_streams = static_cast<size_t>(actual_K) * static_cast<size_t>(max_threads);
            std::cout << "WARNING: Reducing K from " << K << " to " << actual_K
                      << " to stay within file handle limits\n";
        }

        // Strategy 2: If still too many, reduce thread count
        if (theoretical_streams > MAX_SAFE_FILE_HANDLES)
        {
            actual_threads = std::max(1, static_cast<int>(MAX_SAFE_FILE_HANDLES / actual_K));
            theoretical_streams = static_cast<size_t>(actual_K) * static_cast<size_t>(actual_threads);
            std::cout << "WARNING: Reducing threads from " << max_threads << " to " << actual_threads
                      << " to stay within file handle limits\n";
            omp_set_num_threads(actual_threads);
        }
    }

    // Calculate buffer size based on actual concurrent streams
    size_t buf_size;
    if (theoretical_streams > 64)
    {
        buf_size = 4 * 1024 * 1024; // 4MB for high stream count
    }
    else
    {
        buf_size = SAFE_RAM / (theoretical_streams > 0 ? theoretical_streams : 1);
        buf_size = clamp_size_t(buf_size, 2 * 1024 * 1024, 32 * 1024 * 1024);
    }

    std::cout << "=== merge_omp configuration ===\n"
              << "Total RAM Budget: " << total_mem_gb << " GB\n"
              << "Threads (requested): " << max_threads << "\n"
              << "Threads (actual): " << actual_threads << "\n"
              << "K (requested): " << K << "\n"
              << "K (actual): " << actual_K << "\n"
              << "Concurrent streams: " << theoretical_streams << "\n"
              << "Buffer per stream: " << (buf_size / 1024.0 / 1024.0) << " MB\n"
              << "Initial file count: " << files.size() << "\n\n";

    int round = 0;
    while (files.size() > 1)
    {
        std::vector<std::vector<std::string>> groups;
        groups.reserve((files.size() + actual_K - 1) / actual_K);

        for (size_t i = 0; i < files.size(); i += static_cast<size_t>(actual_K))
        {
            std::vector<std::string> g;
            const size_t end = std::min(files.size(), i + static_cast<size_t>(actual_K));
            for (size_t j = i; j < end; ++j)
                g.push_back(files[j]);
            groups.push_back(std::move(g));
        }

        std::vector<std::string> next_files(groups.size());
        const double t0 = omp_get_wtime();

        bool error_occurred = false;

#pragma omp parallel for schedule(dynamic) if (groups.size() >= 2)
        for (size_t i = 0; i < groups.size(); ++i)
        {
            if (error_occurred)
                continue; // Skip if another thread failed

            double t_group_start = omp_get_wtime();
            try
            {
                const bool last_round = (groups.size() == 1 && files.size() == groups[i].size());
                const std::string out_name = last_round ? final_out : (prefix + "_rnd" + std::to_string(round) + "_" + std::to_string(i) + ".tmp");

                uint64_t bytes = merge_k_runs(groups[i], out_name, buf_size, payload_max);
                next_files[i] = out_name;

                double dt = omp_get_wtime() - t_group_start;
#pragma omp critical
                {
                    std::cout << "[R" << round << "][G" << i << "] Merged " << groups[i].size()
                              << " files -> " << fs::path(out_name).filename().string()
                              << " in " << std::fixed << std::setprecision(2) << dt << "s ("
                              << (bytes / 1024.0 / 1024.0) / dt << " MB/s)\n";
                }
            }
            catch (const std::exception &e)
            {
#pragma omp critical
                {
                    std::cerr << "ERROR in thread processing group " << i << ": " << e.what() << "\n";
                    error_occurred = true;
                }
            }
        }

        if (error_occurred)
        {
            std::cerr << "FATAL: Merge round " << round << " failed\n";
            return 2;
        }

        const double t1 = omp_get_wtime();
        std::cout << "Round " << round << " complete in " << std::fixed << std::setprecision(2)
                  << (t1 - t0) << " s\n\n";

        // Clean up old files (except on first round where they're the original runs)
        if (round > 0)
        {
            for (const auto &f : files)
            {
                try
                {
                    if (fs::absolute(f) != fs::absolute(final_out))
                        fs::remove(f);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "WARNING: Could not remove temp file " << f << ": " << e.what() << "\n";
                }
            }
        }

        files = std::move(next_files);
        round++;
    }

    // Final rename if needed
    if (files.size() == 1 && fs::absolute(files[0]) != fs::absolute(final_out))
    {
        try
        {
            fs::rename(files[0], final_out);
        }
        catch (const std::exception &e)
        {
            std::cerr << "ERROR: Failed to rename final output: " << e.what() << "\n";
            return 2;
        }
    }

    std::cout << "=== Merge Complete ===\n";
    std::cout << "Output: " << final_out << "\n";
    return 0;
}
