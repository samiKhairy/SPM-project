/**
 * gen_records_fast.cpp
 * Record generator for SPM Project 2 (MergeSort)
 *
 * Binary record format (little-endian, x86_64):
 * [key:8][len:4][payload:len]
 *
 * Design goals:
 * - Deterministic (seeded RNG)
 * - Variable-length payloads: 8 <= len <= payload_max
 * - Hard safety cap: payload_max <= HARD_PAYLOAD_MAX
 * - High-throughput buffered I/O
 */

#include <iostream>
#include <fstream>
#include <random>
#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>

static constexpr uint32_t MIN_PAYLOAD_LEN = 8;
static constexpr uint32_t HARD_PAYLOAD_MAX = 1u << 20; // 1 MB (design cap)

struct Args
{
    std::string output;
    uint64_t records = 0;
    uint32_t payload_max = 0;
    uint64_t seed = 0;
};

// Minimal CLI parsing (intentional)
bool parse_args(int argc, char **argv, Args &a)
{
    for (int i = 1; i < argc; ++i)
    {
        if (strcmp(argv[i], "--output") == 0 && i + 1 < argc)
        {
            a.output = argv[++i];
        }
        else if (strcmp(argv[i], "--records") == 0 && i + 1 < argc)
        {
            a.records = std::stoull(argv[++i]);
        }
        else if (strcmp(argv[i], "--payload-max") == 0 && i + 1 < argc)
        {
            a.payload_max = std::stoul(argv[++i]);
        }
        else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc)
        {
            a.seed = std::stoull(argv[++i]);
        }
    }
    return !a.output.empty() &&
           a.records > 0 &&
           a.payload_max >= MIN_PAYLOAD_LEN &&
           a.payload_max <= HARD_PAYLOAD_MAX;
}

int main(int argc, char **argv)
{
    Args args;
    if (!parse_args(argc, argv, args))
    {
        std::cerr
            << "Usage: ./gen_records_fast "
            << "--output FILE "
            << "--records N "
            << "--payload-max P (8 <= P <= 1MB) "
            << "--seed S\n";
        return 1;
    }

    std::ofstream out(args.output, std::ios::binary);
    if (!out)
    {
        std::cerr << "Error: cannot open output file " << args.output << "\n";
        return 1;
    }

    // RNG (deterministic)
    std::mt19937_64 rng(args.seed);
    std::uniform_int_distribution<uint64_t> key_dist;
    std::uniform_int_distribution<uint32_t> len_dist(MIN_PAYLOAD_LEN, args.payload_max);

    // Pre-generated noise buffer for payloads (fast copy)
    const size_t noise_size = std::max<size_t>(1024 * 1024, args.payload_max * 2ull);
    std::vector<char> noise(noise_size);
    std::uniform_int_distribution<uint16_t> byte_dist(0, 255);

    for (size_t i = 0; i < noise.size(); ++i)
        noise[i] = static_cast<char>(byte_dist(rng));

    // Buffered output (4MB)
    static constexpr size_t IO_BUFFER_SIZE = 4 * 1024 * 1024;
    std::vector<char> io_buf;
    io_buf.reserve(IO_BUFFER_SIZE);

    size_t noise_idx = 0;

    std::cout << "Generating " << args.records
              << " records (payload_max=" << args.payload_max
              << ") -> " << args.output << "\n";

    for (uint64_t i = 0; i < args.records; ++i)
    {
        const uint64_t key = key_dist(rng);
        const uint32_t len = len_dist(rng);
        const size_t rec_size = 12 + len;

        if (io_buf.size() + rec_size > IO_BUFFER_SIZE)
        {
            out.write(io_buf.data(), io_buf.size());
            io_buf.clear();
        }

        // Serialize (native little-endian)
        io_buf.insert(io_buf.end(),
                      reinterpret_cast<const char *>(&key),
                      reinterpret_cast<const char *>(&key) + 8);

        io_buf.insert(io_buf.end(),
                      reinterpret_cast<const char *>(&len),
                      reinterpret_cast<const char *>(&len) + 4);

        if (noise_idx + len > noise.size())
            noise_idx = 0;

        io_buf.insert(io_buf.end(),
                      noise.begin() + noise_idx,
                      noise.begin() + noise_idx + len);

        noise_idx += (len + 1);
    }

    if (!io_buf.empty())
        out.write(io_buf.data(), io_buf.size());

    out.close();
    std::cout << "Done.\n";
    return 0;
}
