#ifndef VERIFIER_HPP
#define VERIFIER_HPP

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>

inline void verifyOutput(const std::string &filename, size_t expectedCount)
{
    std::cout << "--- [Verification] Checking " << filename << " ---" << std::endl;

    std::ifstream f(filename, std::ios::binary);
    if (!f)
    {
        std::cerr << "[Verify] FAIL: Could not open output file!" << std::endl;
        return;
    }

    uint64_t lastKey = 0, currentKey;
    uint32_t len;
    size_t count = 0;
    bool sorted = true;

    // Read first record to initialize lastKey
    if (f.read(reinterpret_cast<char *>(&currentKey), 8))
    {
        if (!f.read(reinterpret_cast<char *>(&len), 4))
        {
            std::cerr << "[Verify] FAIL: Truncated header at record 0" << std::endl;
            return;
        }
        f.seekg(len, std::ios::cur); // Skip payload
        lastKey = currentKey;
        count++;
    }

    while (f.read(reinterpret_cast<char *>(&currentKey), 8))
    {
        if (!f.read(reinterpret_cast<char *>(&len), 4))
            break;
        f.seekg(len, std::ios::cur);

        if (currentKey < lastKey)
        {
            std::cerr << "[Verify] FAIL: Order violation at index " << count
                      << ". Prev: " << lastKey << " > Curr: " << currentKey << std::endl;
            sorted = false;
            break; // Stop at first error
        }
        lastKey = currentKey;
        count++;
    }

    if (sorted && count == expectedCount)
    {
        std::cout << "[Verify] SUCCESS: " << count << " records sorted correctly." << std::endl;
    }
    else if (sorted)
    {
        std::cerr << "[Verify] FAIL: Count mismatch. Expected " << expectedCount
                  << ", Got " << count << std::endl;
    }
}

#endif