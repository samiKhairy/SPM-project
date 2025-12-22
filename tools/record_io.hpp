//record_io.hpp
#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <string>
#include <limits>

namespace recio
{

    // Project record layout (binary, little-endian on x86_64 nodes).
    // [key:8][len:4][payload:len]
    static constexpr uint32_t MIN_PAYLOAD_LEN = 8;
    static constexpr uint32_t HARD_PAYLOAD_MAX = 1u << 20; // 1MB safety cap (design guardrail)

    // A "view" onto a record. payload pointer is valid until next() is called again.
    struct RecordView
    {
        uint64_t key = 0;
        uint32_t len = 0;
        const char *payload = nullptr;
    };

    // Fail-fast exception (better than silent corruption).
    class RecordFormatError : public std::runtime_error
    {
    public:
        explicit RecordFormatError(const std::string &msg) : std::runtime_error(msg) {}
    };

    inline void append_bytes(std::vector<char> &buf, const void *src, size_t n)
    {
        const size_t old = buf.size();
        buf.resize(old + n);
        std::memcpy(buf.data() + old, src, n);
    }

    class RecordReader
    {
    public:
        // buf_size: how many bytes to use for buffered I/O (e.g., 8MB or 64MB).
        // payload_max: runtime cap (<= HARD_PAYLOAD_MAX). Useful for experiments.
        RecordReader(std::ifstream &in, size_t buf_size, uint32_t payload_max = HARD_PAYLOAD_MAX)
            : in_(in), buf_(buf_size), buf_size_(buf_size), payload_max_(payload_max)
        {
            if (!in_)
                throw std::runtime_error("RecordReader: input stream not open/ready");

            if (buf_size_ < 12)
            {
                throw std::runtime_error("RecordReader: buffer too small (< 12 bytes)");
            }
            if (payload_max_ < MIN_PAYLOAD_LEN || payload_max_ > HARD_PAYLOAD_MAX)
            {
                throw std::runtime_error("RecordReader: payload_max out of allowed range");
            }
            if (buf_size_ < 12 + static_cast<size_t>(payload_max_))
            {
                throw std::runtime_error("RecordReader: buffer too small for payload_max (" +
                                         std::to_string(payload_max_) + " bytes)");
            }
            refill_(); // prime the buffer
        }

        // Reads the next full record. Returns false on clean EOF (no more bytes).
        bool next(RecordView &out)
        {
            while (true)
            {
                // If we have no valid bytes left, it's EOF.
                if (valid_ == 0)
                    return false;

                // Ensure we have at least header (12 bytes).
                if (pos_ + 12 > valid_)
                {
                    // preserve leftover bytes
                    compact_();
                    if (!refill_())
                        return false;
                    continue;
                }

                // Parse header
                uint64_t key;
                uint32_t len;
                std::memcpy(&key, buf_.data() + pos_, 8);
                std::memcpy(&len, buf_.data() + pos_ + 8, 4);

                // Validate len against project constraints
                if (len < MIN_PAYLOAD_LEN || len > payload_max_)
                {
                    throw RecordFormatError("Invalid record len=" + std::to_string(len) +
                                            " (allowed: 8.." + std::to_string(payload_max_) + ")");
                }

                // Safe compute record size
                const uint64_t rec_bytes_64 = 12ULL + static_cast<uint64_t>(len);
                if (rec_bytes_64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max()))
                {
                    throw RecordFormatError("Record too large for size_t");
                }
                const size_t rec_bytes = static_cast<size_t>(rec_bytes_64);

                // Ensure full record is in buffer; if not, refill preserving leftovers.
                if (pos_ + rec_bytes > valid_)
                {
                    compact_();
                    if (!refill_())
                    {
                        // EOF in the middle of a record => corrupted/truncated input
                        throw RecordFormatError("Unexpected EOF while reading record payload");
                    }
                    continue;
                }

                // Full record available => return view
                out.key = key;
                out.len = len;
                out.payload = buf_.data() + pos_ + 12;

                // Advance position
                pos_ += rec_bytes;
                return true;
            }
        }

    private:
        std::ifstream &in_;
        std::vector<char> buf_;
        size_t buf_size_ = 0;

        size_t pos_ = 0;   // current parse position
        size_t valid_ = 0; // number of valid bytes in buf_
        uint32_t payload_max_ = HARD_PAYLOAD_MAX;

        // Move leftover bytes [pos_, valid_) to the beginning of the buffer.
        void compact_()
        {
            if (pos_ == 0)
                return;
            if (pos_ < valid_)
            {
                const size_t leftover = valid_ - pos_;
                std::memmove(buf_.data(), buf_.data() + pos_, leftover);
                valid_ = leftover;
            }
            else
            {
                valid_ = 0;
            }
            pos_ = 0;
        }

        // Read more bytes after current valid_ (must have compacted first).
        // Returns false if nothing read (EOF).
        bool refill_()
        {
            if (!in_.good() && in_.eof())
            {
                return false;
            }

            // If buffer already full and we still can't parse a full record, the record is bigger than buffer.
            // But since payload_max <= 1MB and buf_size should be >= 12 + payload_max in the worst case,
            // we enforce a minimum practical buffer at call sites. Still, we guard here.
            if (valid_ == buf_size_)
            {
                throw RecordFormatError("Buffer too small to fit a full record (increase input buffer size)");
            }

            in_.read(buf_.data() + valid_, static_cast<std::streamsize>(buf_size_ - valid_));
            const std::streamsize got = in_.gcount();
            if (got <= 0)
            {
                // No new bytes; keep valid_ as-is.
                return false;
            }
            valid_ += static_cast<size_t>(got);
            return true;
        }
    };

} // namespace recio
