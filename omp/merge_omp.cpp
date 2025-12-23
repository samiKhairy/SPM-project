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
#include <atomic>
#include <omp.h>
#include <iomanip>

#include "../tools/record_io.hpp"

namespace fs = std::filesystem;

static inline void append_bytes(std::vector<char> &buf, size_t &pos, const void *src, size_t n)
{
    if (pos + n > buf.size())
    {
        throw std::runtime_error("Output buffer overflow");
    }
    std::memcpy(buf.data() + pos, src, n);
    pos += n;
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

struct MergeResult
{
    std::string path;
    bool is_temp = false;
};

struct MergeConfig
{
    std::string prefix;
    std::string final_out;
    size_t buf_size = 0;
    uint32_t payload_max = 0;
    int k = 2;
    std::atomic<uint64_t> temp_id{0};
};

static std::vector<std::string> list_runs(const std::string &prefix, const std::string &final_out)
{
    std::vector<std::string> files;
    std::string dir = fs::path(prefix).parent_path().string();
    if (dir.empty())
        dir = ".";
    std::string base = fs::path(prefix).filename().string();
    const std::string final_out_abs = fs::absolute(final_out).string();

    for (const auto &p : fs::directory_iterator(dir))
    {
        if (!p.is_regular_file())
            continue;
        const std::string fname = p.path().filename().string();
        const std::string full_path = fs::absolute(p.path()).string();
        if (fname.rfind(base, 0) == 0 && fname.find(".dat") != std::string::npos && full_path != final_out_abs)
            files.push_back(p.path().string());
    }
    std::sort(files.begin(), files.end());
    return files;
}

static std::string make_temp_name(const MergeConfig &cfg)
{
    const uint64_t id = cfg.temp_id.fetch_add(1, std::memory_order_relaxed);
    return cfg.prefix + "_task_" + std::to_string(id) + ".tmp";
}

static void safe_remove(const std::string &path)
{
    try
    {
        fs::remove(path);
    }
    catch (const std::exception &e)
    {
        std::cerr << "WARNING: Could not remove temp file " << path << ": " << e.what() << "\n";
    }
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
    std::vector<char> out_buf(OUT_LIMIT);
    size_t out_pos = 0;
    uint64_t bytes_written = 0;

    while (!heap.empty())
    {
        const HeapNode top = heap.top();
        heap.pop();
        RunCtx &rc = runs[top.run_idx];

        const size_t rec_size = 12 + static_cast<size_t>(rc.cur.len);
        if (out_pos + rec_size > OUT_LIMIT)
        {
            out.write(out_buf.data(), static_cast<std::streamsize>(out_pos));
            if (!out.good())
            {
                std::cerr << "ERROR: Write failed to output file\n";
                throw std::runtime_error("Write failed");
            }
            out_pos = 0;
        }
        append_bytes(out_buf, out_pos, &rc.cur.key, 8);
        append_bytes(out_buf, out_pos, &rc.cur.len, 4);
        append_bytes(out_buf, out_pos, rc.cur.payload, rc.cur.len);
        bytes_written += rec_size;

        rc.has = rc.rr->next(rc.cur);
        if (rc.has)
            heap.push(HeapNode{rc.cur.key, top.run_idx});
    }

    if (out_pos > 0)
    {
        out.write(out_buf.data(), static_cast<std::streamsize>(out_pos));
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

static void merge_task(const std::vector<std::string> &files,
                       const MergeConfig &cfg,
                       bool is_root,
                       MergeResult &result)
{
    if (files.empty())
        throw std::runtime_error("merge_task: empty file list");

    if (files.size() == 1)
    {
        if (is_root && fs::absolute(files[0]) != fs::absolute(cfg.final_out))
        {
            fs::rename(files[0], cfg.final_out);
            result.path = cfg.final_out;
        }
        else
        {
            result.path = files[0];
        }
        result.is_temp = false;
        return;
    }

    if (files.size() <= static_cast<size_t>(cfg.k))
    {
        result.path = is_root ? cfg.final_out : make_temp_name(cfg);
        result.is_temp = !is_root;
        merge_k_runs(files, result.path, cfg.buf_size, cfg.payload_max);
        return;
    }

    const size_t mid = files.size() / 2;
    std::vector<std::string> left(files.begin(), files.begin() + mid);
    std::vector<std::string> right(files.begin() + mid, files.end());

    MergeResult left_res;
    MergeResult right_res;

#pragma omp task shared(left_res) firstprivate(left, cfg)
    merge_task(left, cfg, false, left_res);

#pragma omp task shared(right_res) firstprivate(right, cfg)
    merge_task(right, cfg, false, right_res);

#pragma omp taskwait

    result.path = is_root ? cfg.final_out : make_temp_name(cfg);
    result.is_temp = !is_root;
    merge_k_runs({left_res.path, right_res.path}, result.path, cfg.buf_size, cfg.payload_max);

    if (left_res.is_temp)
        safe_remove(left_res.path);
    if (right_res.is_temp)
        safe_remove(right_res.path);
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

    MergeConfig cfg{
        prefix,
        final_out,
        buf_size,
        payload_max,
        actual_K};

    const double t0 = omp_get_wtime();
    MergeResult root_result;
    try
    {
#pragma omp parallel
        {
#pragma omp single
            merge_task(files, cfg, true, root_result);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "FATAL: Merge failed: " << e.what() << "\n";
        return 2;
    }
    const double t1 = omp_get_wtime();

    std::cout << "Merge complete in " << std::fixed << std::setprecision(2)
              << (t1 - t0) << " s\n\n";

    std::cout << "=== Merge Complete ===\n";
    std::cout << "Output: " << final_out << "\n";
    return 0;
}
