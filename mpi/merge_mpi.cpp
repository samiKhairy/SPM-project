/**
 * mpi_distributed_sort.cpp
 * Distributed sort using MPI with OpenMP for local sorting.
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

#include "../tools/common_sort.hpp"

namespace fs = std::filesystem;

// --- Config ---

// ==========================================
// === OpenMP task sort
// ==========================================
#ifndef TASK_THRESHOLD
#define TASK_THRESHOLD (1 << 14)
#endif
static constexpr size_t TASK_THRESHOLD_VALUE = TASK_THRESHOLD;

static inline void merge_meta(std::vector<sortutil::Meta> &a, std::vector<sortutil::Meta> &tmp, size_t l, size_t m, size_t r)
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

static void mergesort_task(std::vector<sortutil::Meta> &a, std::vector<sortutil::Meta> &tmp, size_t l, size_t r)
{
    const size_t n = r - l;
    if (n <= TASK_THRESHOLD_VALUE)
    {
        std::sort(a.begin() + l, a.begin() + r, [](const sortutil::Meta &x, const sortutil::Meta &y)
                  { return x.key < y.key; });
        return;
    }
    const size_t m = l + n / 2;
#pragma omp task shared(a, tmp) if (n > TASK_THRESHOLD_VALUE)
    mergesort_task(a, tmp, l, m);
#pragma omp task shared(a, tmp) if (n > TASK_THRESHOLD_VALUE)
    mergesort_task(a, tmp, m, r);
#pragma omp taskwait
    merge_meta(a, tmp, l, m, r);
}

// ==========================================
// === Header-only scanners
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
// === Partitioned generation with append
// ==========================================

void generate_partitioned_runs(const std::string &input, uint64_t start, uint64_t end,
                               int my_rank, int n_ranks,
                               const std::vector<uint64_t> &splitters,
                               const std::string &temp_dir,
                               const sortutil::RunBufferPlan &plan,
                               uint32_t payload_max)
{

    // 1. Open ONE file per destination rank (Append Mode)
    std::vector<std::unique_ptr<std::ofstream>> out_files(n_ranks);
    std::vector<std::vector<char>> write_buffers(n_ranks);
    const size_t write_buf_size = std::min<size_t>(plan.out_buf_size, 4ull << 20);

    for (int i = 0; i < n_ranks; ++i)
    {
        // Filename: dest_<TARGET>_src_<MY_RANK>.dat
        std::string name = temp_dir + "/dest_" + std::to_string(i) + "_src_" + std::to_string(my_rank) + ".dat";
        // Append mode is crucial here
        out_files[i] = std::make_unique<std::ofstream>(name, std::ios::binary | std::ios::app);
        write_buffers[i].reserve(write_buf_size);
    }

    // Input Reader
    std::ifstream in(input, std::ios::binary);
    in.seekg(start);
    recio::RecordReader rr(in, plan.in_buf_size, payload_max);
    recio::RecordView rv;

    std::vector<char> payload_buf;
    std::vector<sortutil::Meta> meta;
    std::vector<sortutil::Meta> tmp;

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
                                           [](uint64_t val, const sortutil::Meta &m)
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

        const size_t meta_overhead = (meta.size() + 1) * sizeof(sortutil::Meta);
        if (payload_buf.size() + rec_sz + meta_overhead > plan.payload_budget)
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

void merge_local_files(const std::string &temp_dir,
                       int my_rank,
                       int n_ranks,
                       const std::string &output_file,
                       size_t mem_budget_bytes,
                       uint32_t payload_max)
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

    const sortutil::MergeBufferPlan plan = sortutil::plan_merge_buffers(mem_budget_bytes, inputs.size());
    sortutil::merge_k_runs(inputs, output_file, plan.in_buf_size, plan.out_buf_size, payload_max);

    // Clean temp files
    for (const auto &f : inputs)
        fs::remove(f);
}

static std::string make_tree_temp(const std::string &temp_dir, int rank, int step, const std::string &suffix)
{
    return temp_dir + "/tree_r" + std::to_string(rank) + "_s" + std::to_string(step) + "_" + suffix + ".dat";
}

static void merge_files(const std::vector<std::string> &inputs,
                        const std::string &output_file,
                        size_t mem_budget_bytes,
                        uint32_t payload_max)
{
    if (inputs.empty())
        throw std::runtime_error("merge_files: empty input list");
    if (inputs.size() == 1)
    {
        if (fs::absolute(inputs[0]) != fs::absolute(output_file))
            fs::rename(inputs[0], output_file);
        return;
    }

    const sortutil::MergeBufferPlan plan = sortutil::plan_merge_buffers(mem_budget_bytes, inputs.size());
    sortutil::merge_k_runs(inputs, output_file, plan.in_buf_size, plan.out_buf_size, payload_max);
}

static void send_file_nonblocking(const std::string &path, int dest, int tag_base)
{
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in)
        throw std::runtime_error("Cannot open file to send: " + path);
    const uint64_t total = static_cast<uint64_t>(in.tellg());
    in.seekg(0);

    MPI_Request size_req;
    MPI_Isend(&total, 1, MPI_UINT64_T, dest, tag_base, MPI_COMM_WORLD, &size_req);

    const size_t chunk = 8ULL * 1024 * 1024;
    std::vector<char> buf_a(chunk);
    std::vector<char> buf_b(chunk);
    MPI_Request reqs[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};

    uint64_t sent = 0;
    int buf_idx = 0;
    bool first = true;

    while (sent < total)
    {
        std::vector<char> &buf = (buf_idx == 0) ? buf_a : buf_b;
        const size_t to_read = static_cast<size_t>(std::min<uint64_t>(chunk, total - sent));
        in.read(buf.data(), static_cast<std::streamsize>(to_read));
        if (!in)
            throw std::runtime_error("Failed reading file for send: " + path);

        MPI_Request &req = reqs[buf_idx];
        MPI_Isend(buf.data(), static_cast<int>(to_read), MPI_BYTE, dest, tag_base + 1, MPI_COMM_WORLD, &req);

        if (!first)
        {
            const int prev = 1 - buf_idx;
            MPI_Wait(&reqs[prev], MPI_STATUS_IGNORE);
        }
        first = false;

        sent += to_read;
        buf_idx = 1 - buf_idx;
    }

    MPI_Wait(&reqs[0], MPI_STATUS_IGNORE);
    MPI_Wait(&reqs[1], MPI_STATUS_IGNORE);
    MPI_Wait(&size_req, MPI_STATUS_IGNORE);
}

static void recv_file_nonblocking(const std::string &path, int src, int tag_base)
{
    uint64_t total = 0;
    MPI_Request size_req;
    MPI_Irecv(&total, 1, MPI_UINT64_T, src, tag_base, MPI_COMM_WORLD, &size_req);
    MPI_Wait(&size_req, MPI_STATUS_IGNORE);

    std::ofstream out(path, std::ios::binary);
    if (!out)
        throw std::runtime_error("Cannot open file to receive: " + path);

    if (total == 0)
        return;

    const size_t chunk = 8ULL * 1024 * 1024;
    std::vector<char> buf_a(chunk);
    std::vector<char> buf_b(chunk);
    MPI_Request reqs[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
    size_t sizes[2] = {0, 0};

    uint64_t received = 0;
    int buf_idx = 0;
    bool first = true;

    while (received < total)
    {
        std::vector<char> &buf = (buf_idx == 0) ? buf_a : buf_b;
        const size_t to_recv = static_cast<size_t>(std::min<uint64_t>(chunk, total - received));
        MPI_Request &req = reqs[buf_idx];
        MPI_Irecv(buf.data(), static_cast<int>(to_recv), MPI_BYTE, src, tag_base + 1, MPI_COMM_WORLD, &req);
        sizes[buf_idx] = to_recv;

        if (!first)
        {
            const int prev = 1 - buf_idx;
            MPI_Wait(&reqs[prev], MPI_STATUS_IGNORE);
            std::vector<char> &prev_buf = (prev == 0) ? buf_a : buf_b;
            out.write(prev_buf.data(), static_cast<std::streamsize>(sizes[prev]));
        }
        first = false;

        received += to_recv;
        buf_idx = 1 - buf_idx;
    }

    const int last_idx = 1 - buf_idx;
    MPI_Wait(&reqs[last_idx], MPI_STATUS_IGNORE);
    if (sizes[last_idx] > 0)
    {
        std::vector<char> &last_buf = (last_idx == 0) ? buf_a : buf_b;
        out.write(last_buf.data(), static_cast<std::streamsize>(sizes[last_idx]));
    }

    out.close();
    if (!out.good())
        throw std::runtime_error("Failed to close received file: " + path);
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

    if (argc < 4)
    {
        if (rank == 0)
            std::cerr << "Usage: mpirun ./mpi_distributed_sort <input> <output_prefix> <mem_budget_mb> [payload_max]\n";
        MPI_Finalize();
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_prefix = argv[2];
    const uint64_t mem_budget_mb = std::stoull(argv[3]);
    uint32_t payload_max = recio::HARD_PAYLOAD_MAX;
    if (argc > 4)
        payload_max = static_cast<uint32_t>(std::stoul(argv[4]));
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

    const size_t mem_budget_bytes = mem_budget_mb * 1024ULL * 1024ULL;
    const sortutil::RunBufferPlan run_plan = sortutil::plan_run_buffers(mem_budget_bytes, payload_max);

    // --- PHASE 2: GENERATE & PARTITION (Parallel) ---
    double t_map_start = MPI_Wtime();
    generate_partitioned_runs(input_file, file_offsets[rank], file_offsets[rank + 1],
                              rank, size, splitters, temp_dir, run_plan, payload_max);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0)
        std::cout << "Phase 2 (Partition) Time: " << (MPI_Wtime() - t_map_start) << "s\n";

    // --- PHASE 3: LOCAL MERGE (Parallel) ---
    double t_reduce_start = MPI_Wtime();
    std::string local_out = output_prefix + "rank_" + std::to_string(rank) + ".dat";
    merge_local_files(temp_dir, rank, size, local_out, mem_budget_bytes, payload_max);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0)
    {
        std::cout << "Phase 3 (Merge) Time: " << (MPI_Wtime() - t_reduce_start) << "s\n";
    }

    // --- PHASE 4: TREE MERGE (Non-blocking transfers) ---
    double t_tree_start = MPI_Wtime();
    std::string current_file = local_out;
    bool current_is_temp = false;

    for (int step = 1; step < size; step *= 2)
    {
        const int tag_base = 1000 + step;

        if ((rank % (2 * step)) == 0)
        {
            const int partner = rank + step;
            if (partner < size)
            {
                const std::string recv_file = make_tree_temp(temp_dir, rank, step, "recv");
                recv_file_nonblocking(recv_file, partner, tag_base);

                const std::string merged_file = make_tree_temp(temp_dir, rank, step, "merged");
                merge_files({current_file, recv_file}, merged_file, mem_budget_bytes, payload_max);

                if (current_is_temp && fs::exists(current_file))
                    fs::remove(current_file);
                if (fs::exists(recv_file))
                    fs::remove(recv_file);

                current_file = merged_file;
                current_is_temp = true;
            }
        }
        else
        {
            const int dest = rank - step;
            send_file_nonblocking(current_file, dest, tag_base);
            if (current_is_temp && fs::exists(current_file))
                fs::remove(current_file);
            break;
        }
    }

    if (rank == 0)
    {
        const std::string final_out = output_prefix + "final.dat";
        if (fs::absolute(current_file) != fs::absolute(final_out))
            fs::rename(current_file, final_out);
        std::cout << "Phase 4 (Tree Merge) Time: " << (MPI_Wtime() - t_tree_start) << "s\n";
        std::cout << "Total Time: " << (MPI_Wtime() - t_start) << "s\n";
        std::cout << "Final Output: " << final_out << "\n";
    }

    MPI_Finalize();
    return 0;
}
