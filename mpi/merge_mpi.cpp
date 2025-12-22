/**
 * mpi_distributed_sort.cpp
 * Optimized Distributed Sort combining MPI + OpenMP [cite: 20]
 * * Fixes applied:
 * 1. Header-only scanning for splitters (IO Bottleneck Fix)
 * 2. Meta struct overhead accounting (Memory Stability Fix)
 * 3. Appending to per-rank files (File Handle Scalability Fix)
 */

#include <mpi.h>
#include <omp.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <queue>

#include "../tools/record_io.hpp"

namespace fs = std::filesystem;

// --- Config ---
// 256MB chunks for efficient sorting batches
static const uint64_t SORT_CHUNK_MEM_MB = 256;
static const uint64_t MEM_BUDGET_BYTES = SORT_CHUNK_MEM_MB * 1024ULL * 1024ULL;

struct Meta
{
    uint64_t key;
    uint64_t offset;
    uint32_t len;
};

// ==========================================
// === OPENMP TASK SORT (Re-used from Task 1) [cite: 21]
// ==========================================
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

// ==========================================
// === FIX 1: HEADER-ONLY SCANNERS
// ==========================================

// Quickly scan file to find chunk boundaries for each rank
std::vector<uint64_t> find_file_offsets_fast(const std::string &path, int n_ranks)
{
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    uint64_t total_size = in.tellg();
    in.seekg(0);

    std::vector<uint64_t> offsets(n_ranks + 1);
    offsets[0] = 0;
    offsets[n_ranks] = total_size;

    uint64_t ideal_chunk = total_size / n_ranks;
    uint64_t current_pos = 0;
    int current_rank = 1;

    // Buffer for header reading
    uint64_t key;
    uint32_t len;

    while (current_rank < n_ranks && in.peek() != EOF)
    {
        if (current_pos >= ideal_chunk * current_rank)
        {
            offsets[current_rank] = current_pos;
            current_rank++;
        }

        // READ HEADER ONLY (12 bytes)
        if (!in.read((char *)&key, 8))
            break;
        if (!in.read((char *)&len, 4))
            break;

        // SKIP PAYLOAD (Zero I/O cost for payload)
        in.seekg(len, std::ios::cur);

        current_pos += (12 + len);
    }

    // Fill remaining ranks if file ended early (edge case)
    while (current_rank < n_ranks)
    {
        offsets[current_rank++] = total_size;
    }

    return offsets;
}

// Quickly sample keys without reading payloads
std::vector<uint64_t> find_splitters_fast(const std::string &path, int n_ranks)
{
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    size_t fsize = in.tellg();
    in.seekg(0);

    size_t num_samples = n_ranks * 200; // 200 samples per rank is usually sufficient
    std::vector<uint64_t> samples;
    samples.reserve(num_samples);

    // Estimate stride to get ~num_samples evenly spaced
    // Average record size guess: 1KB.
    // This is just a heuristic to skip through the file.
    size_t avg_rec_size = 1024;
    size_t total_recs_est = fsize / avg_rec_size;
    size_t stride = std::max<size_t>(1, total_recs_est / num_samples);

    size_t count = 0;
    uint64_t key;
    uint32_t len;

    while (in.peek() != EOF)
    {
        if (!in.read((char *)&key, 8))
            break;
        if (!in.read((char *)&len, 4))
            break;
        in.seekg(len, std::ios::cur); // Skip payload

        if (count++ % stride == 0)
        {
            samples.push_back(key);
        }
    }

    std::sort(samples.begin(), samples.end());

    std::vector<uint64_t> splitters;
    if (samples.empty())
        return std::vector<uint64_t>(n_ranks - 1, 0);

    // Pick (P-1) splitters
    size_t step = samples.size() / n_ranks;
    for (int i = 1; i < n_ranks; ++i)
    {
        size_t idx = i * step;
        splitters.push_back(idx < samples.size() ? samples[idx] : samples.back());
    }
    return splitters;
}

// ==========================================
// === FIX 3: GENERATION WITH APPEND (Handles Fix)
// ==========================================

void generate_partitioned_runs(const std::string &input, uint64_t start, uint64_t end,
                               int my_rank, int n_ranks,
                               const std::vector<uint64_t> &splitters,
                               const std::string &temp_dir)
{

    // 1. Open ONE file per destination rank (Append Mode)
    std::vector<std::unique_ptr<std::ofstream>> out_files(n_ranks);
    std::vector<std::vector<char>> write_buffers(n_ranks);

    for (int i = 0; i < n_ranks; ++i)
    {
        // Filename: dest_<TARGET>_src_<MY_RANK>.dat
        std::string name = temp_dir + "/dest_" + std::to_string(i) + "_src_" + std::to_string(my_rank) + ".dat";
        // Append mode is crucial here
        out_files[i] = std::make_unique<std::ofstream>(name, std::ios::binary | std::ios::app);
        write_buffers[i].reserve(1024 * 1024); // 1MB buffer per file
    }

    // Input Reader
    std::ifstream in(input, std::ios::binary);
    in.seekg(start);
    recio::RecordReader rr(in, 32 * 1024 * 1024, recio::HARD_PAYLOAD_MAX);
    recio::RecordView rv;

    std::vector<char> payload_buf;
    std::vector<Meta> meta;
    std::vector<Meta> tmp; // For sorting

    uint64_t bytes_read_total = 0;
    uint64_t chunk_limit = end - start;

    auto flush_batch = [&]()
    {
        if (meta.empty())
            return;

        // A. Local Sort (OpenMP)
        if (tmp.size() < meta.size())
            tmp.resize(meta.size());
#pragma omp parallel
        {
#pragma omp single
            mergesort_task(meta, tmp, 0, meta.size());
        }

        // B. Partition and Append
        size_t current_idx = 0;
        for (int dest = 0; dest < n_ranks; ++dest)
        {
            // Identify range for this destination using splitters
            size_t end_idx;
            if (dest == n_ranks - 1)
            {
                end_idx = meta.size();
            }
            else
            {
                uint64_t split_val = splitters[dest];
                auto it = std::upper_bound(meta.begin() + current_idx, meta.end(), split_val,
                                           [](uint64_t val, const Meta &m)
                                           { return val < m.key; });
                end_idx = std::distance(meta.begin(), it);
            }

            // Append records to the specific destination file
            auto &buf = write_buffers[dest];
            auto &out = *out_files[dest];

            for (size_t i = current_idx; i < end_idx; ++i)
            {
                const auto &m = meta[i];
                size_t rec_sz = 12 + m.len;

                if (buf.size() + rec_sz > buf.capacity())
                {
                    out.write(buf.data(), buf.size());
                    buf.clear();
                }

                size_t old = buf.size();
                buf.resize(old + rec_sz);
                char *ptr = buf.data() + old;
                std::memcpy(ptr, &m.key, 8);
                std::memcpy(ptr + 8, &m.len, 4);
                std::memcpy(ptr + 12, payload_buf.data() + m.offset, m.len);
            }
            // Flush remaining buffer for this dest
            if (!buf.empty())
            {
                out.write(buf.data(), buf.size());
                buf.clear();
            }

            current_idx = end_idx;
        }

        meta.clear();
        payload_buf.clear();
    };

    while (bytes_read_total < chunk_limit && rr.next(rv))
    {
        size_t rec_sz = 12 + rv.len;

        // === FIX 2: MEMORY CHECK WITH META OVERHEAD ===
        // We must count the vector<Meta> size too!
        size_t meta_overhead = (meta.size() + 1) * sizeof(Meta);

        if (payload_buf.size() + rec_sz + meta_overhead > MEM_BUDGET_BYTES)
        {
            flush_batch();
        }

        uint64_t off = payload_buf.size();
        payload_buf.resize(off + rv.len);
        std::memcpy(payload_buf.data() + off, rv.payload, rv.len);
        meta.push_back({rv.key, off, (uint32_t)rv.len});

        bytes_read_total += rec_sz;
    }
    flush_batch();

    // Files close automatically via unique_ptr
}

// ==========================================
// === PHASE 3: K-WAY MERGE
// ==========================================

void merge_local_files(const std::string &temp_dir, int my_rank, int n_ranks, const std::string &output_file)
{
    // Identify my input files: dest_<MY_RANK>_src_<0..N>.dat
    std::vector<std::string> inputs;
    for (int i = 0; i < n_ranks; ++i)
    {
        std::string fname = temp_dir + "/dest_" + std::to_string(my_rank) + "_src_" + std::to_string(i) + ".dat";
        if (fs::exists(fname))
        {
            inputs.push_back(fname);
        }
    }

    if (inputs.empty())
    {
        std::ofstream out(output_file, std::ios::binary);
        return;
    }

    // Setup Heap
    struct Ctx
    {
        std::ifstream in;
        std::unique_ptr<recio::RecordReader> rr;
        recio::RecordView cur;
        bool has;
    };

    // Limit buffers to avoid OOM in merge phase
    // 24GB available / N inputs
    size_t ram_budget = 24ULL * 1024 * 1024 * 1024;
    size_t buf_per_file = ram_budget / (inputs.size() + 1); // +1 for output
    if (buf_per_file > 64 * 1024 * 1024)
        buf_per_file = 64 * 1024 * 1024; // Cap at 64MB
    if (buf_per_file < 1 * 1024 * 1024)
        buf_per_file = 1 * 1024 * 1024; // Min 1MB

    std::vector<Ctx> ctx(inputs.size());
    using Node = std::pair<uint64_t, int>;
    std::priority_queue<Node, std::vector<Node>, std::greater<Node>> heap;

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        ctx[i].in.open(inputs[i], std::ios::binary);
        ctx[i].rr = std::make_unique<recio::RecordReader>(ctx[i].in, buf_per_file, recio::HARD_PAYLOAD_MAX);
        ctx[i].has = ctx[i].rr->next(ctx[i].cur);
        if (ctx[i].has)
            heap.push({ctx[i].cur.key, (int)i});
    }

    std::ofstream out(output_file, std::ios::binary);
    std::vector<char> out_buf;
    out_buf.reserve(64 * 1024 * 1024);

    while (!heap.empty())
    {
        auto top = heap.top();
        heap.pop();
        int idx = top.second;
        auto &c = ctx[idx];

        // Buffer Output
        size_t rec_sz = 12 + c.cur.len;
        if (out_buf.size() + rec_sz > out_buf.capacity())
        {
            out.write(out_buf.data(), out_buf.size());
            out_buf.clear();
        }

        size_t old = out_buf.size();
        out_buf.resize(old + rec_sz);
        char *ptr = out_buf.data() + old;
        std::memcpy(ptr, &c.cur.key, 8);
        std::memcpy(ptr + 8, &c.cur.len, 4);
        std::memcpy(ptr + 12, c.cur.payload, c.cur.len);

        // Advance
        c.has = c.rr->next(c.cur);
        if (c.has)
            heap.push({c.cur.key, idx});
    }
    if (!out_buf.empty())
        out.write(out_buf.data(), out_buf.size());

    // Clean temp files
    for (const auto &f : inputs)
        fs::remove(f);
}

// ==========================================
// === MAIN DRIVER
// ==========================================

int main(int argc, char **argv)
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_FUNNELED, &provided);
    if (provided < MPI_THREAD_FUNNELED)
        MPI_Abort(MPI_COMM_WORLD, 1);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 3)
    {
        if (rank == 0)
            std::cerr << "Usage: mpirun ./mpi_distributed_sort <input> <output_prefix>\n";
        MPI_Finalize();
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_prefix = argv[2];
    std::string temp_dir = fs::path(output_prefix).parent_path().string() + "/mpi_partitions";

    // --- SETUP ---
    if (rank == 0)
    {
        if (fs::exists(temp_dir))
            fs::remove_all(temp_dir);
        fs::create_directories(temp_dir);
        fs::create_directories(fs::path(output_prefix).parent_path());
    }
    MPI_Barrier(MPI_COMM_WORLD);

    double t_start = MPI_Wtime();

    // --- PHASE 1: SPLITTERS (Header-Only) ---
    std::vector<uint64_t> file_offsets(size + 1);
    std::vector<uint64_t> splitters;

    if (rank == 0)
    {
        std::cout << "[Rank 0] Scanning offsets (Header-Only)...\n";
        file_offsets = find_file_offsets_fast(input_file, size);
        std::cout << "[Rank 0] Sampling splitters (Header-Only)...\n";
        splitters = find_splitters_fast(input_file, size);
    }

    MPI_Bcast(file_offsets.data(), size + 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    int n_splitters = size - 1;
    splitters.resize(n_splitters);
    MPI_Bcast(splitters.data(), n_splitters, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    // --- PHASE 2: GENERATE & PARTITION (Parallel) ---
    double t_map_start = MPI_Wtime();
    generate_partitioned_runs(input_file, file_offsets[rank], file_offsets[rank + 1],
                              rank, size, splitters, temp_dir);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0)
        std::cout << "Phase 2 (Partition) Time: " << (MPI_Wtime() - t_map_start) << "s\n";

    // --- PHASE 3: LOCAL MERGE (Parallel) ---
    double t_reduce_start = MPI_Wtime();
    std::string final_out = output_prefix + "rank_" + std::to_string(rank) + ".dat";
    merge_local_files(temp_dir, rank, size, final_out);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0)
    {
        std::cout << "Phase 3 (Merge) Time: " << (MPI_Wtime() - t_reduce_start) << "s\n";
        std::cout << "Total Time: " << (MPI_Wtime() - t_start) << "s\n";
    }

    MPI_Finalize();
    return 0;
}