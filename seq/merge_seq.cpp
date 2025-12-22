    // merge_seq.cpp - FIXED VERSION
    #include <iostream>
    #include <fstream>
    #include <memory>
    #include <vector>
    #include <queue>
    #include <string>
    #include <cstring>
    #include <stdexcept>
    #include <chrono>
    #include <iomanip>
    #include <algorithm>

    #include "../tools/record_io.hpp"

    static inline double get_time()
    {
        using namespace std::chrono;
        return duration_cast<duration<double>>(high_resolution_clock::now().time_since_epoch()).count();
    }

    static inline void append_bytes(std::vector<char> &out, const void *src, size_t n)
    {
        const size_t old = out.size();
        out.resize(old + n);
        std::memcpy(out.data() + old, src, n);
    }

    struct RunCtx
    {
        std::ifstream in;
        std::unique_ptr<recio::RecordReader> rr;
        recio::RecordView cur;
        bool has = false;

        RunCtx(const std::string &fname, size_t buf_size, uint32_t payload_max)
            : in(fname, std::ios::binary)
        {
            if (!in)
                throw std::runtime_error("Cannot open run file: " + fname);
            rr = std::make_unique<recio::RecordReader>(in, buf_size, payload_max);
            has = rr->next(cur);
        }
    };

    struct HeapNode
    {
        uint64_t key;
        size_t run_idx;
    };

    struct HeapCmp
    {
        bool operator()(const HeapNode &a, const HeapNode &b) const
        {
            if (a.key != b.key)
                return a.key > b.key;
            // Tie-breaker: use run_idx to ensure deterministic, repeatable output
            return a.run_idx > b.run_idx;
        }
    };

    int main(int argc, char **argv)
    {
        
        if (argc < 5)
        {
            std::cerr << "Usage: ./merge_seq <run_prefix> <num_runs> <output_file> <mem_budget_gb> [payload_max]\n";
            std::cerr << "Example: ./merge_seq runs/run_ 10 sorted.dat 2 1048576\n";
            return 1;
        }

        const std::string run_prefix = argv[1];
        const size_t num_runs = std::stoull(argv[2]);
        const std::string output_file = argv[3];
        const uint64_t mem_budget_gb = std::stoull(argv[4]);

        uint32_t payload_max = (argc >= 6) ? static_cast<uint32_t>(std::stoul(argv[5])) : recio::HARD_PAYLOAD_MAX;

        // --- CONFIGURABLE MEMORY BUDGET ---
        const size_t MEM_BUDGET = mem_budget_gb * 1024ull * 1024ull * 1024ull;
        // Ensure all literals use size_t to match MEM_BUDGET
        const size_t OUT_BUF_SIZE = std::clamp(MEM_BUDGET / 10, (size_t)8 << 20, (size_t)64 << 20);
        const size_t SAFETY_GAP = MEM_BUDGET / 20;

        if (MEM_BUDGET <= OUT_BUF_SIZE + SAFETY_GAP)
        {
            throw std::runtime_error("Memory budget too small for merge buffers");
        }

        const size_t MIN_IN_BUF = (size_t)1 << 20;
        const size_t MAX_IN_BUF = (size_t)64 << 20;

        size_t remaining_for_inputs = MEM_BUDGET - OUT_BUF_SIZE - SAFETY_GAP;
        size_t in_buf_size = std::clamp(remaining_for_inputs / (num_runs > 0 ? num_runs : 1),
                                        MIN_IN_BUF,
                                        MAX_IN_BUF);

        if (num_runs * in_buf_size + OUT_BUF_SIZE > MEM_BUDGET)
        {
            throw std::runtime_error(
                "Too many runs for single-pass merge under memory budget; multi-pass merge required.");
        }

        // Alias for compatibility with rest of code
        size_t out_buf_size = OUT_BUF_SIZE;
        std::cout << "=== Sequential Merge Configuration ===\n";
        std::cout << "Memory budget: " << mem_budget_gb << " GB\n";
        std::cout << "Input files: " << num_runs << "\n";
        std::cout << "Buffer per input file: " << (in_buf_size / 1024.0 / 1024.0) << " MB\n";
        std::cout << "Output buffer: " << (out_buf_size / 1024.0 / 1024.0) << " MB\n";
        std::cout << "Total memory allocated: "
                << ((in_buf_size * num_runs + out_buf_size) / 1024.0 / 1024.0 / 1024.0) << " GB\n\n";

        double t_start = get_time();

        std::vector<std::unique_ptr<RunCtx>> runs;
        runs.reserve(num_runs);

        std::cout << "Opening " << num_runs << " run files...\n";

        try
        {
            for (size_t i = 0; i < num_runs; ++i)
            {
                const std::string fname = run_prefix + std::to_string(i) + ".dat";
                runs.push_back(std::make_unique<RunCtx>(fname, in_buf_size, payload_max));
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << "ERROR: Failed to initialize runs: " << e.what() << "\n";
            return 1;
        }

        std::priority_queue<HeapNode, std::vector<HeapNode>, HeapCmp> heap;
        for (size_t i = 0; i < runs.size(); ++i)
        {
            if (runs[i]->has)
                heap.push(HeapNode{runs[i]->cur.key, i});
        }

        std::ofstream out(output_file, std::ios::binary);
        if (!out)
        {
            std::cerr << "ERROR: Cannot create output file: " << output_file << "\n";
            return 1;
        }

        std::vector<char> out_buf;
        out_buf.reserve(out_buf_size);
        uint64_t out_count = 0;
        uint64_t bytes_written = 0;

        std::cout << "Merging...\n";

        while (!heap.empty())
        {
            const HeapNode top = heap.top();
            heap.pop();
            RunCtx &rc = *runs[top.run_idx];

            const size_t rec_size = 12 + static_cast<size_t>(rc.cur.len);
            if (out_buf.size() + rec_size > out_buf_size)
            {
                out.write(out_buf.data(), static_cast<std::streamsize>(out_buf.size()));
                if (!out.good())
                {
                    std::cerr << "ERROR: Write failed to output file\n";
                    return 1;
                }
                out_buf.clear();
            }
            append_bytes(out_buf, &rc.cur.key, 8);
            append_bytes(out_buf, &rc.cur.len, 4);
            append_bytes(out_buf, rc.cur.payload, rc.cur.len);

            bytes_written += rec_size;
            out_count++;

            // Progress indicator
            if (out_count % 1000000 == 0)
            {
                std::cout << "  Processed " << (out_count / 1000000) << "M records ("
                        << std::fixed << std::setprecision(1)
                        << (bytes_written / 1024.0 / 1024.0) << " MB)...\n";
            }

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
                return 1;
            }
        }

        out.close();
        if (!out.good())
        {
            std::cerr << "ERROR: Failed to close output file properly\n";
            return 1;
        }

        double total_time = get_time() - t_start;

        std::cout << "\n=== Sequential Merge Complete ===\n";
        std::cout << "Time: " << std::fixed << std::setprecision(2) << total_time << " s\n";
        std::cout << "Throughput: " << (bytes_written / 1024.0 / 1024.0 / total_time) << " MB/s\n";
        std::cout << "Records merged: " << out_count << "\n";
        std::cout << "Total bytes: " << (bytes_written / 1024.0 / 1024.0) << " MB\n";

        return 0;
    }