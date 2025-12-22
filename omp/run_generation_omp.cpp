// run_generation_omp.cpp - FIXED VERSION
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <stdexcept>
#include <omp.h>
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
    return omp_get_wtime();
}

static inline void append_bytes(std::vector<char> &buf, const void *src, size_t n)
{
    const size_t old = buf.size();
    buf.resize(old + n);
    std::memcpy(buf.data() + old, src, n);
}

// -------- OpenMP task mergesort over Meta --------
static constexpr size_t TASK_THRESHOLD = 1 << 14;

static inline void merge_meta(std::vector<Meta> &a, std::vector<Meta> &tmp, size_t l, size_t m, size_t r)
{
    size_t i = l, j = m, k = l;
    while (i < m && j < r)
        tmp[k++] = (a[i].key <= a[j].key) ? a[i++] : a[j++];
    while (i < m)
        tmp[k++] = a[i++];
    while (j < r)
        tmp[k++] = a[j++];
    for (size_t x = l; x < r; ++x)
        a[x] = tmp[x];
}

static void mergesort_task(std::vector<Meta> &a, std::vector<Meta> &tmp, size_t l, size_t r)
{
    const size_t n = r - l;
    if (n <= TASK_THRESHOLD)
    {
        std::sort(a.begin() + l, a.begin() + r, [](const Meta &x, const Meta &y)
                  { return x.key < y.key; });
        return;
    }
    const size_t m = l + n / 2;
#pragma omp task shared(a, tmp) if (n > TASK_THRESHOLD)
    mergesort_task(a, tmp, l, m);
#pragma omp task shared(a, tmp) if (n > TASK_THRESHOLD)
    mergesort_task(a, tmp, m, r);
#pragma omp taskwait
    merge_meta(a, tmp, l, m, r);
}

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        std::cerr << "Usage: ./run_gen_omp <input_file> <mem_budget_mb> <run_prefix> [payload_max]\n";
        return 1;
    }

    const std::string input_path = argv[1];
    const uint64_t mem_budget_mb = std::stoull(argv[2]);
    const std::string run_prefix = argv[3];

    uint32_t payload_max = recio::HARD_PAYLOAD_MAX;
    if (argc >= 5)
        payload_max = static_cast<uint32_t>(std::stoul(argv[4]));

    const uint64_t MEM_BUDGET_TOTAL = mem_budget_mb * 1024ULL * 1024ULL;
    const uint64_t DATA_BUDGET = (MEM_BUDGET_TOTAL * 4) / 10; // 40% for payload data

    const size_t IN_BUF_SIZE = 64 * 1024 * 1024;
    const size_t OUT_BUF_SIZE = 16 * 1024 * 1024;

    std::cout << "=== OpenMP Run Generation ===\n";
    std::cout << "Total RAM budget: " << mem_budget_mb << " MB\n";
    std::cout << "Effective data budget: " << (DATA_BUDGET / 1024.0 / 1024.0) << " MB (payload share)\n";
    std::cout << "Threads: " << omp_get_max_threads() << "\n\n";

    std::ifstream in(input_path, std::ios::binary);
    if (!in)
    {
        std::cerr << "ERROR: Cannot open input file: " << input_path << "\n";
        return 1;
    }

    recio::RecordReader rr(in, IN_BUF_SIZE, payload_max);
    recio::RecordView rv;

    bool has_stash = false;
    recio::RecordView stash;
    std::vector<char> stash_payload;

    std::vector<char> payload_buffer;
    std::vector<Meta> meta;
    std::vector<Meta> tmp;
    std::vector<char> out_buffer;
    out_buffer.reserve(OUT_BUF_SIZE);

    uint64_t run_id = 0;
    double t_start_global = get_time();

    auto projected_total_bytes = [&](uint64_t next_payload_len) -> uint64_t
    {
        const uint64_t payload_bytes = static_cast<uint64_t>(payload_buffer.size()) + next_payload_len;
        const uint64_t meta_bytes = static_cast<uint64_t>(meta.size() + 1) * sizeof(Meta);
        const uint64_t tmp_bytes = meta_bytes;
        return payload_bytes + meta_bytes + tmp_bytes;
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

        if (has_stash)
        {
            consume_record(stash);
            has_stash = false;
            stash_payload.clear();
        }

        while (true)
        {
            if (!meta.empty() && projected_total_bytes(0) >= MEM_BUDGET_TOTAL)
                break;

            if (!rr.next(rv))
                break; // EOF

            if (!meta.empty() && projected_total_bytes(rv.len) > MEM_BUDGET_TOTAL)
            {
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
            break;

        // --- OPENMP SORT PHASE ---
        double t0_sort = get_time();

        // Resize tmp to match meta size
        tmp.resize(meta.size());

#pragma omp parallel
        {
#pragma omp single
            mergesort_task(meta, tmp, 0, meta.size());
        }
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
            size_t rec_size = 12 + m.len;
            if (out_buffer.size() + rec_size > OUT_BUF_SIZE)
            {
                out.write(out_buffer.data(), out_buffer.size());
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
            out.write(out_buffer.data(), out_buffer.size());
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
                  << "\033[1;32mSort: " << dt_sort << "s\033[0m | " // Green for OMP
                  << "Write: " << dt_write << "s | "
                  << "Size: " << (payload_buffer.size() / 1024.0 / 1024.0) << " MB | "
                  << "Records: " << meta.size() << "\n";
    }

    double total_time = get_time() - t_start_global;
    std::cout << "\n=== OpenMP Generation Stats ===\n";
    std::cout << "Total Time:  " << std::fixed << std::setprecision(2) << total_time << " s\n";
    std::cout << "Total Read:  " << g_stats.total_read << " s (Disk I/O)\n";
    std::cout << "Total Sort:  " << g_stats.total_sort << " s (CPU Parallel)\n";
    std::cout << "Total Write: " << g_stats.total_write << " s (Disk I/O)\n";
    std::cout << "Throughput:  " << (g_stats.total_bytes / 1024.0 / 1024.0 / total_time) << " MB/s\n";
    std::cout << "Runs created: " << run_id << "\n";

    return 0;
}
