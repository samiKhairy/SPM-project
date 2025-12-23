#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "../tools/common_sort.hpp"

static inline double get_time()
{
    using namespace std::chrono;
    return duration_cast<duration<double>>(high_resolution_clock::now().time_since_epoch()).count();
}

int main(int argc, char **argv)
{
    if (argc < 5)
    {
        std::cerr << "Usage: ./merge_seq <run_prefix> <num_runs> <output_file> <mem_budget_mb> [payload_max]\n";
        std::cerr << "Example: ./merge_seq runs/run_ 10 sorted.dat 2048 1048576\n";
        return 1;
    }

    const std::string run_prefix = argv[1];
    const size_t num_runs = std::stoull(argv[2]);
    const std::string output_file = argv[3];
    const uint64_t mem_budget_mb = std::stoull(argv[4]);

    uint32_t payload_max = (argc >= 6) ? static_cast<uint32_t>(std::stoul(argv[5])) : recio::HARD_PAYLOAD_MAX;

    const size_t mem_budget = mem_budget_mb * 1024ull * 1024ull;
    const sortutil::MergeBufferPlan plan = sortutil::plan_merge_buffers(mem_budget, num_runs);
    const size_t in_buf_size = plan.in_buf_size;
    const size_t out_buf_size = plan.out_buf_size;

    if (num_runs * in_buf_size + out_buf_size + plan.safety_gap > mem_budget)
    {
        throw std::runtime_error(
            "Too many runs for single-pass merge under memory budget; multi-pass merge required.");
    }

    std::cout << "=== Sequential Merge Configuration ===\n";
    std::cout << "Memory budget: " << mem_budget_mb << " MB\n";
    std::cout << "Input files: " << num_runs << "\n";
    std::cout << "Buffer per input file: " << (in_buf_size / 1024.0 / 1024.0) << " MB\n";
    std::cout << "Output buffer: " << (out_buf_size / 1024.0 / 1024.0) << " MB\n";
    std::cout << "Total memory allocated: "
              << ((in_buf_size * num_runs + out_buf_size) / 1024.0 / 1024.0 / 1024.0) << " GB\n\n";

    std::vector<std::string> inputs;
    inputs.reserve(num_runs);
    for (size_t i = 0; i < num_runs; ++i)
    {
        inputs.push_back(run_prefix + std::to_string(i) + ".dat");
    }

    const double t_start = get_time();
    uint64_t out_bytes = 0;

    try
    {
        out_bytes = sortutil::merge_k_runs(inputs, output_file, in_buf_size, out_buf_size, payload_max);
    }
    catch (const std::exception &e)
    {
        std::cerr << "ERROR: Merge failed: " << e.what() << "\n";
        return 1;
    }

    const double total_time = get_time() - t_start;
    std::cout << "\n=== Sequential Merge Complete ===\n";
    std::cout << "Time: " << std::fixed << std::setprecision(2) << total_time << " s\n";
    std::cout << "Throughput: " << (out_bytes / 1024.0 / 1024.0 / total_time) << " MB/s\n";
    std::cout << "Total bytes: " << (out_bytes / 1024.0 / 1024.0) << " MB\n";

    return 0;
}
