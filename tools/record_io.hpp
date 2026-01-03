#ifndef RECORD_IO_HPP
#define RECORD_IO_HPP

#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <vector>

namespace recordio
{
    constexpr size_t kHeaderSize = sizeof(uint64_t) + sizeof(uint32_t);

    inline uint64_t keyAt(const std::vector<char> &buffer, size_t offset)
    {
        uint64_t k;
        std::memcpy(&k, buffer.data() + offset, sizeof(k));
        return k;
    }

    inline uint32_t lenAt(const std::vector<char> &buffer, size_t offset)
    {
        uint32_t l;
        std::memcpy(&l, buffer.data() + offset + sizeof(uint64_t), sizeof(l));
        return l;
    }

    inline size_t recordSize(const std::vector<char> &buffer, size_t offset)
    {
        return kHeaderSize + lenAt(buffer, offset);
    }

    inline size_t appendRecord(std::vector<char> &buffer, uint64_t key, uint32_t len, std::istream &source)
    {
        const size_t start = buffer.size();
        buffer.resize(start + kHeaderSize + len);

        std::memcpy(buffer.data() + start, &key, sizeof(uint64_t));
        std::memcpy(buffer.data() + start + sizeof(uint64_t), &len, sizeof(uint32_t));
        source.read(buffer.data() + start + kHeaderSize, len);

        return start;
    }

    inline void writeAt(std::ostream &out, const std::vector<char> &buffer, size_t offset)
    {
        const uint32_t len = lenAt(buffer, offset);
        out.write(buffer.data() + offset, (std::streamsize)(kHeaderSize + len));
    }

    inline bool readRecord(std::istream &in, uint64_t &key, uint32_t &len, std::vector<char> &payload)
    {
        if (!in.read(reinterpret_cast<char *>(&key), sizeof(uint64_t)))
            return false;
        if (!in.read(reinterpret_cast<char *>(&len), sizeof(uint32_t)))
            return false;
        payload.resize(len);
        return static_cast<bool>(in.read(payload.data(), len));
    }

    inline void writeRecord(std::ostream &out, uint64_t key, uint32_t len, const std::vector<char> &payload)
    {
        out.write(reinterpret_cast<const char *>(&key), sizeof(uint64_t));
        out.write(reinterpret_cast<const char *>(&len), sizeof(uint32_t));
        out.write(payload.data(), (std::streamsize)len);
    }
}

#endif // RECORD_IO_HPP
