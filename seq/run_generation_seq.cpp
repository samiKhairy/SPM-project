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

static inline void append_bytes(std::vector<char> &buf, size_t &pos, const void *src, size_t n)
{
    if (pos + n > buf.size())
    {
        throw std::runtime_error("Output buffer overflow");
    }
    std::memcpy(buf.data() + pos, src, n);
    pos += n;
}

static inline size_t clamp(size_t v, size_t lo, size_t hi)
{
    if (v < lo)
        return lo;
    if (v > hi)
        return hi;
    return v;
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

    const size_t MEM_BUDGET = mem_budget_mb * 1024ull * 1024ull;

    // Policy-based buffer sizing
    const size_t IN_BUF_SIZE = clamp(MEM_BUDGET / 4, 8ull << 20, 128ull << 20);  // 25%
    const size_t OUT_BUF_SIZE = clamp(MEM_BUDGET / 10, 8ull << 20, 64ull << 20); // 10%
    const size_t SAFETY_GAP = MEM_BUDGET / 20;                                   // 5%

    if (MEM_BUDGET <= IN_BUF_SIZE + OUT_BUF_SIZE + SAFETY_GAP + payload_max)
    {
        throw std::runtime_error("Memory budget too small for buffers + safety");
    }

    const size_t PAYLOAD_BUDGET =
        MEM_BUDGET - IN_BUF_SIZE - OUT_BUF_SIZE - SAFETY_GAP - payload_max;

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
    payload_buffer.reserve(PAYLOAD_BUDGET); // Pre-allocate full budget capacity

    std::vector<Meta> meta;
    // Estimate meta capacity assuming an average record size (e.g., 32 bytes payload)
    meta.reserve(PAYLOAD_BUDGET / (sizeof(Meta) + 32));

    std::vector<char> out_buffer(OUT_BUF_SIZE);
    size_t out_pos = 0;

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
        payload_buffer.insert(payload_buffer.end(), rec.payload, rec.payload + rec.len);
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
            if (!meta.empty() && memory_used() >= PAYLOAD_BUDGET)
                break;

            const bool ok = rr.next(rv);
            if (!ok)
                break; // EOF

            size_t projected = 0;
            if (__builtin_add_overflow(payload_buffer.size(), meta.size() * sizeof(Meta), &projected) ||
                __builtin_add_overflow(projected, rv.len, &projected) ||
                __builtin_add_overflow(projected, sizeof(Meta), &projected))
            {
                throw std::runtime_error("Memory accounting overflow");
            }

            if (!meta.empty() && projected > PAYLOAD_BUDGET)
            {
                has_stash = true;
                stash_payload.assign(rv.payload, rv.payload + rv.len);
                stash.key = rv.key;
                stash.len = rv.len;
                stash.payload = stash_payload.data();
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

        out_pos = 0;

        for (const auto &m : meta)
        {
            const size_t rec_size = 12 + static_cast<size_t>(m.len);
            if (out_pos + rec_size > OUT_BUF_SIZE)
            {
                out.write(out_buffer.data(), static_cast<std::streamsize>(out_pos));
                if (!out.good())
                {
                    std::cerr << "ERROR: Write failed for run " << run_name << "\n";
                    return 1;
                }
                out_pos = 0;
            }

            append_bytes(out_buffer, out_pos, &m.key, 8);
            append_bytes(out_buffer, out_pos, &m.len, 4);
            append_bytes(out_buffer, out_pos, payload_buffer.data() + m.offset, m.len);
        }

        if (out_pos > 0)
        {
            out.write(out_buffer.data(), static_cast<std::streamsize>(out_pos));
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
