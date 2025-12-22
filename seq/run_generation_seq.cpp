// run_generation_seq.cpp - FIXED VERSION
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <chrono>
#include <stdexcept>
#include <iomanip>

#include "../tools/record_io.hpp"

struct Meta
{
    uint64_t key;
    uint64_t offset;
    uint32_t len;
};

// --- Statistics Wrapper ---
struct Stats
{
    double total_read = 0;
    double total_sort = 0;
    double total_write = 0;
    uint64_t total_bytes = 0;
} g_stats;

static inline double get_time()
{
    using namespace std::chrono;
    return duration_cast<duration<double>>(high_resolution_clock::now().time_since_epoch()).count();
}

static inline void append_bytes(std::vector<char> &buf, const void *src, size_t n)
{
    const size_t old = buf.size();
    buf.resize(old + n);
    std::memcpy(buf.data() + old, src, n);
}

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        std::cerr << "Usage: ./run_gen_seq <input_file> <mem_budget_mb> <run_prefix> [payload_max]\n";
        return 1;
    }

    const std::string input_path = argv[1];
    const uint64_t mem_budget_mb = std::stoull(argv[2]);
    const std::string run_prefix = argv[3];

    uint32_t payload_max = recio::HARD_PAYLOAD_MAX;
    if (argc >= 5)
        payload_max = static_cast<uint32_t>(std::stoul(argv[4]));

    const uint64_t MEM_BUDGET = mem_budget_mb * 1024ULL * 1024ULL;
    const size_t IN_BUF_SIZE = 64 * 1024 * 1024;  // 64MB input buffering
    const size_t OUT_BUF_SIZE = 16 * 1024 * 1024; // 16MB output buffering

    std::cout << "=== Sequential Run Generation ===\n";
    std::cout << "Memory budget: " << mem_budget_mb << " MB\n";
    std::cout << "Input buffer: " << (IN_BUF_SIZE / 1024.0 / 1024.0) << " MB\n";
    std::cout << "Output buffer: " << (OUT_BUF_SIZE / 1024.0 / 1024.0) << " MB\n\n";

    std::ifstream in(input_path, std::ios::binary);
    if (!in)
    {
        std::cerr << "ERROR: Cannot open input file: " << input_path << "\n";
        return 1;
    }

    recio::RecordReader rr(in, IN_BUF_SIZE, payload_max);
    recio::RecordView rv;

    // One-record stash to handle budget boundary
    bool has_stash = false;
    recio::RecordView stash;
    std::vector<char> stash_payload;

    std::vector<char> payload_buffer;
    std::vector<Meta> meta;

    std::vector<char> out_buffer;
    out_buffer.reserve(OUT_BUF_SIZE);

    uint64_t run_id = 0;
    double t_start_global = get_time();

    auto memory_used = [&]() -> uint64_t
    {
        return static_cast<uint64_t>(payload_buffer.size()) +
               static_cast<uint64_t>(meta.size()) * sizeof(Meta);
    };

    auto consume_record = [&](const recio::RecordView &rec)
    {
        const uint64_t off = payload_buffer.size();
        payload_buffer.resize(off + rec.len);
        std::memcpy(payload_buffer.data() + off, rec.payload, rec.len);
        meta.push_back(Meta{rec.key, off, rec.len});
    };

    while (true)
    {
        double t0_read = get_time();

        payload_buffer.clear();
        meta.clear();

        // First, consume stash if present
        if (has_stash)
        {
            consume_record(stash);
            has_stash = false;
            stash_payload.clear();
        }

        // Fill run
        while (true)
        {
            if (!meta.empty() && memory_used() >= MEM_BUDGET)
                break;

            const bool ok = rr.next(rv);
            if (!ok)
                break; // EOF

            const uint64_t projected =
                static_cast<uint64_t>(payload_buffer.size()) +
                static_cast<uint64_t>(meta.size()) * sizeof(Meta) +
                static_cast<uint64_t>(rv.len) +
                sizeof(Meta);

            // If adding this record would exceed budget and we already have something:
            if (!meta.empty() && projected > MEM_BUDGET)
            {
                // Stash this record for next run
                stash_payload.resize(rv.len);
                std::memcpy(stash_payload.data(), rv.payload, rv.len);
                stash.key = rv.key;
                stash.len = rv.len;
                stash.payload = stash_payload.data();
                has_stash = true;
                break;
            }

            consume_record(rv);
        }

        g_stats.total_read += (get_time() - t0_read);

        if (meta.empty())
        {
            break;
        }

        // --- SORT PHASE ---
        double t0_sort = get_time();
        std::sort(meta.begin(), meta.end(),
                  [](const Meta &a, const Meta &b)
                  { return a.key < b.key; });
        double dt_sort = get_time() - t0_sort;
        g_stats.total_sort += dt_sort;

        // --- WRITE PHASE ---
        double t0_write = get_time();
        const std::string run_name = run_prefix + std::to_string(run_id++) + ".dat";
        std::ofstream out(run_name, std::ios::binary);
        if (!out)
        {
            std::cerr << "ERROR: Cannot create run file: " << run_name << "\n";
            return 1;
        }

        out_buffer.clear();

        for (const auto &m : meta)
        {
            const size_t rec_size = 12 + static_cast<size_t>(m.len);
            if (out_buffer.size() + rec_size > OUT_BUF_SIZE)
            {
                out.write(out_buffer.data(), static_cast<std::streamsize>(out_buffer.size()));
                if (!out.good())
                {
                    std::cerr << "ERROR: Write failed for run " << run_name << "\n";
                    return 1;
                }
                out_buffer.clear();
            }

            append_bytes(out_buffer, &m.key, 8);
            append_bytes(out_buffer, &m.len, 4);
            append_bytes(out_buffer, payload_buffer.data() + m.offset, m.len);
        }

        if (!out_buffer.empty())
        {
            out.write(out_buffer.data(), static_cast<std::streamsize>(out_buffer.size()));
            if (!out.good())
            {
                std::cerr << "ERROR: Final write failed for run " << run_name << "\n";
                return 1;
            }
        }

        out.close();
        if (!out.good())
        {
            std::cerr << "ERROR: Failed to close run file: " << run_name << "\n";
            return 1;
        }

        double dt_write = get_time() - t0_write;
        g_stats.total_write += dt_write;
        g_stats.total_bytes += payload_buffer.size();

        std::cout << "[Run " << std::setw(3) << (run_id - 1) << "] "
                  << "Read: " << std::fixed << std::setprecision(3)
                  << (get_time() - t0_read - dt_sort - dt_write) << "s | "
                  << "\033[1;33mSort: " << dt_sort << "s\033[0m | " // Yellow
                  << "Write: " << dt_write << "s | "
                  << "Size: " << (payload_buffer.size() / 1024.0 / 1024.0) << " MB | "
                  << "Records: " << meta.size() << "\n";
    }

    double total_time = get_time() - t_start_global;

    std::cout << "\n=== Sequential Generation Complete ===\n";
    std::cout << "Total Time:  " << std::fixed << std::setprecision(2) << total_time << " s\n";
    std::cout << "Total Read:  " << g_stats.total_read << " s (Disk I/O)\n";
    std::cout << "Total Sort:  " << g_stats.total_sort << " s (CPU)\n";
    std::cout << "Total Write: " << g_stats.total_write << " s (Disk I/O)\n";
    std::cout << "Throughput:  " << (g_stats.total_bytes / 1024.0 / 1024.0 / total_time) << " MB/s\n";
    std::cout << "Runs created: " << run_id << "\n";

    return 0;
}