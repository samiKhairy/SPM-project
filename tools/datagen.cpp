#include <iostream>
#include <vector>
#include <fstream>
#include <random>
#include <string>
#include <cstdint>
#include <filesystem>

// Usage: ./datagen <filename_base> <num_records> <payload_max> <num_splits>
// num_splits = 1 -> creates <filename_base> (For Seq, OMP, FF)
// num_splits = 4 -> creates <filename_base>_0 ... <filename_base>_3 (For MPI)

void generateFile(const std::string &filename, size_t count, uint32_t payloadMax, int seedOffset)
{
    std::ofstream outFile(filename, std::ios::binary);
    if (!outFile)
    {
        std::cerr << "Error creating file: " << filename << std::endl;
        exit(1);
    }

    std::mt19937_64 rng(time(NULL) + seedOffset); // distinct seed per split
    std::uniform_int_distribution<uint64_t> distKey;
    std::uniform_int_distribution<uint32_t> distLen(8, payloadMax);

    std::vector<char> buffer; // Reusable buffer

    for (size_t i = 0; i < count; ++i)
    {
        uint64_t key = distKey(rng);
        uint32_t len = distLen(rng);

        outFile.write(reinterpret_cast<char *>(&key), 8);
        outFile.write(reinterpret_cast<char *>(&len), 4);

        buffer.resize(len);
        // Fill payload with deterministically random junk (faster than rand())
        for (size_t j = 0; j < len; ++j)
            buffer[j] = 'A' + ((i + j) % 26);

        outFile.write(buffer.data(), len);
    }
    std::cout << "[DataGen] Generated " << filename << " (" << count << " records)" << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc < 4)
    {
        std::cerr << "Usage: ./datagen <base_filename> <total_records> <payload_max> [splits]" << std::endl;
        return 1;
    }

    std::string baseName = argv[1];
    size_t totalRecords = std::stoull(argv[2]);
    uint32_t payloadMax = std::stoul(argv[3]);
    int splits = (argc > 4) ? std::stoi(argv[4]) : 1;

    if (splits < 1)
        splits = 1;

    if (splits == 1)
    {
        // Single file mode (Seq, OMP, FF)
        generateFile(baseName, totalRecords, payloadMax, 0);
    }
    else
    {
        // Split mode (MPI)
        size_t recordsPerSplit = totalRecords / splits;
        for (int i = 0; i < splits; ++i)
        {
            std::string name = baseName + "_" + std::to_string(i);
            // Handle remainder on last file
            size_t count = (i == splits - 1) ? (totalRecords - i * recordsPerSplit) : recordsPerSplit;
            generateFile(name, count, payloadMax, i);
        }
    }

    return 0;
}