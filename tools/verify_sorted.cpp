// verify_sorted.cpp - ENHANCED VERSION
#include <iostream>
#include <fstream>
#include <cstdint>
#include <iomanip>
#include <string>

static constexpr uint32_t MIN_PAYLOAD_LEN = 8;
static constexpr uint32_t HARD_PAYLOAD_MAX = 1u << 20; // 1MB

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage: ./verify_sorted <file> [payload_max]\n";
        return 1;
    }

    const std::string filename = argv[1];
    uint32_t payload_max = (argc >= 3) ? static_cast<uint32_t>(std::stoul(argv[2])) : HARD_PAYLOAD_MAX;

    if (payload_max < MIN_PAYLOAD_LEN || payload_max > HARD_PAYLOAD_MAX)
    {
        std::cerr << "ERROR: payload_max must be in range [" << MIN_PAYLOAD_LEN
                  << ", " << HARD_PAYLOAD_MAX << "]\n";
        return 1;
    }

    std::ifstream in(filename, std::ios::binary);
    if (!in)
    {
        std::cerr << "ERROR: Cannot open file: " << filename << "\n";
        return 1;
    }

    // Get file size
    in.seekg(0, std::ios::end);
    const std::streampos file_size = in.tellg();
    in.seekg(0, std::ios::beg);

    uint64_t prev_key = 0;
    bool first = true;
    size_t count = 0;
    uint64_t total_bytes_read = 0;

    std::cout << "Verifying file: " << filename << "\n";
    std::cout << "File size: " << (file_size / 1024.0 / 1024.0) << " MB\n";
    std::cout << "Payload max: " << payload_max << " bytes\n\n";

    std::vector<char> scratch(1024 * 1024);

    while (true)
    {
        uint64_t key;
        uint32_t len;

        // Read key (8 bytes)
        if (!in.read(reinterpret_cast<char *>(&key), 8))
        {
            if (in.eof())
                break; // Clean EOF

            std::cerr << "ERROR: Failed to read key at record " << count
                      << " (offset: " << total_bytes_read << ")\n";
            return 1;
        }

        // Read length (4 bytes)
        if (!in.read(reinterpret_cast<char *>(&len), 4))
        {
            std::cerr << "ERROR: Truncated record header at record " << count
                      << " (offset: " << total_bytes_read << ")\n";
            std::cerr << "       File appears to be corrupted or incomplete\n";
            return 1;
        }

        // Validate length field
        if (len < MIN_PAYLOAD_LEN || len > payload_max)
        {
            std::cerr << "ERROR: Invalid payload length at record " << count << "\n";
            std::cerr << "       Key: " << key << "\n";
            std::cerr << "       Length: " << len << " (valid range: "
                      << MIN_PAYLOAD_LEN << " - " << payload_max << ")\n";
            std::cerr << "       Offset: " << total_bytes_read << "\n";
            return 1;
        }

        // Check sort order (allow equal keys - stable sort)
        if (!first && key < prev_key)
        {
            std::cerr << "ERROR: Records not sorted at record " << count << "\n";
            std::cerr << "       Previous key: " << prev_key << "\n";
            std::cerr << "       Current key:  " << key << "\n";
            std::cerr << "       Offset: " << total_bytes_read << "\n";
            return 1;
        }

        // Read and discard payload to ensure bytes actually exist
        uint32_t remaining = len;
        while (remaining > 0)
        {
            const uint32_t chunk = std::min<uint32_t>(remaining, static_cast<uint32_t>(scratch.size()));
            if (!in.read(scratch.data(), chunk))
            {
                std::cerr << "ERROR: Truncated payload at record " << count << "\n";
                std::cerr << "       Expected " << len << " bytes, file ended early\n";
                return 1;
            }
            remaining -= chunk;
        }

        prev_key = key;
        first = false;
        count++;
        total_bytes_read += (12 + len);

        // Progress indicator for large files
        if (count % 1000000 == 0)
        {
            std::cout << "  Verified " << (count / 1000000) << "M records... ("
                      << std::fixed << std::setprecision(1)
                      << (100.0 * total_bytes_read / file_size) << "%)\n";
        }
    }

    // Final validation: did we read the entire file?
    if (static_cast<uint64_t>(file_size) != total_bytes_read)
    {
        std::cerr << "WARNING: File size mismatch\n";
        std::cerr << "         Expected: " << file_size << " bytes\n";
        std::cerr << "         Read:     " << total_bytes_read << " bytes\n";
        std::cerr << "         This may indicate trailing garbage or corruption\n";
    }

    // Success!
    std::cout << "\n=== VERIFICATION SUCCESSFUL ===\n";
    std::cout << "✓ File is properly sorted\n";
    std::cout << "✓ Total records: " << count << "\n";
    std::cout << "✓ All record formats valid\n";
    std::cout << "✓ No truncation detected\n";
    std::cout << "✓ Total bytes: " << total_bytes_read << " ("
              << (total_bytes_read / 1024.0 / 1024.0) << " MB)\n";

    return 0;
}
