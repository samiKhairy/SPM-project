#ifndef MERGER_HPP
#define MERGER_HPP

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

static inline size_t clamp_sz(size_t v, size_t lo, size_t hi) {
    return std::min(std::max(v, lo), hi);
}

// K-way merge may have hundreds of runs => MIN must be small.
static constexpr size_t MIN_RUN_BUF  = 256ull * 1024;        // 256KB
static constexpr size_t MAX_RUN_BUF  = 64ull  * 1024 * 1024; // 64MB

struct RunStream {
    std::ifstream file;
    std::vector<char> buf;
    size_t pos = 0, valid = 0;
    bool finished = false;

    uint64_t key = 0;
    uint32_t len = 0;
    const char* payload = nullptr;

    RunStream(const std::string& fname, size_t bufBytes)
        : buf(clamp_sz(bufBytes, MIN_RUN_BUF, MAX_RUN_BUF))
    {
        file.open(fname, std::ios::binary);
        if (!file) { finished = true; return; }
        refill_preserve();
        advance();
    }

private:
    inline bool refill_preserve() {
        const size_t rem = valid - pos;
        if (rem) std::memmove(buf.data(), buf.data() + pos, rem);

        file.read(buf.data() + rem, (std::streamsize)(buf.size() - rem));
        const size_t rd = (size_t)file.gcount();

        pos = 0;
        valid = rem + rd;

        if (valid == 0) { finished = true; return false; }
        return true;
    }

public:
    inline void advance() {
        if (finished) return;

        while (pos + 12 > valid)
            if (!refill_preserve()) return;

        std::memcpy(&key, buf.data() + pos, 8);
        std::memcpy(&len, buf.data() + pos + 8, 4);

        while (pos + 12ull + (size_t)len > valid)
            if (!refill_preserve()) return;

        payload = buf.data() + pos + 12;
        pos += 12ull + (size_t)len;
    }
};

struct MergeNode { uint64_t key; int idx; };
struct MergeCmp {
    bool operator()(const MergeNode& a, const MergeNode& b) const { return a.key > b.key; }
};

// K-way merge (local finalize)
inline void mergeFiles(const std::vector<std::string>& inputFiles,
                       const std::string& outputName,
                       size_t totalReadBudgetBytes,
                       size_t outBufBytes)
{
    if (inputFiles.empty()) return;

    const size_t k = inputFiles.size();

    // Clamp global budgets (not per run)
    totalReadBudgetBytes = clamp_sz(totalReadBudgetBytes, MIN_RUN_BUF, 1024ull * 1024 * 1024);
    outBufBytes          = clamp_sz(outBufBytes,          256ull * 1024, 256ull * 1024 * 1024);

    // Per-run buffer derived from total budget, bounded by MIN/MAX.
    const size_t perRun = clamp_sz(totalReadBudgetBytes / std::max<size_t>(1, k),
                                   MIN_RUN_BUF, MAX_RUN_BUF);

    std::vector<std::unique_ptr<RunStream>> runs;
    runs.reserve(k);

    std::priority_queue<MergeNode, std::vector<MergeNode>, MergeCmp> pq;

    for (const auto& f : inputFiles) {
        auto rs = std::make_unique<RunStream>(f, perRun);
        if (!rs->finished) {
            pq.push({rs->key, (int)runs.size()});
            runs.push_back(std::move(rs));
        }
    }

    std::ofstream out(outputName, std::ios::binary);
    if (!out) throw std::runtime_error("mergeFiles: cannot open output: " + outputName);

    std::vector<char> outBuf(outBufBytes);
    size_t outPos = 0;

    auto flush = [&]() {
        if (outPos) {
            out.write(outBuf.data(), (std::streamsize)outPos);
            outPos = 0;
        }
    };

    while (!pq.empty()) {
        const auto top = pq.top(); pq.pop();
        RunStream* rs = runs[top.idx].get();

        const size_t recSize = 12ull + (size_t)rs->len;
        if (outPos + recSize > outBuf.size()) flush();

        std::memcpy(outBuf.data() + outPos,       &rs->key, 8);
        std::memcpy(outBuf.data() + outPos + 8,   &rs->len, 4);
        std::memcpy(outBuf.data() + outPos + 12,  rs->payload, rs->len);
        outPos += recSize;

        rs->advance();
        if (!rs->finished) pq.push({rs->key, top.idx});
    }

    flush();
    out.close();

    for (const auto& f : inputFiles)
        std::filesystem::remove(f);
}

// Fast 2-way merge (MPI tree)
inline void mergeTwoFiles(const std::string& A,
                          const std::string& B,
                          const std::string& outName,
                          size_t readBudgetBytes,
                          size_t outBufBytes)
{
    readBudgetBytes = clamp_sz(readBudgetBytes, 2ull * MIN_RUN_BUF, 1024ull * 1024 * 1024);
    outBufBytes     = clamp_sz(outBufBytes,     256ull * 1024,       256ull * 1024 * 1024);

    // Force large per-run buffers for 2-way merge.
    const size_t perRun = clamp_sz(readBudgetBytes / 2, 4ull<<20, MAX_RUN_BUF);

    RunStream a(A, perRun);
    RunStream b(B, perRun);

    std::ofstream out(outName, std::ios::binary);
    if (!out) throw std::runtime_error("mergeTwoFiles: cannot open output: " + outName);

    std::vector<char> outBuf(outBufBytes);
    size_t outPos = 0;

    auto flush = [&]() {
        if (outPos) {
            out.write(outBuf.data(), (std::streamsize)outPos);
            outPos = 0;
        }
    };

    auto emit = [&](RunStream& rs) {
        const size_t recSize = 12ull + (size_t)rs.len;
        if (outPos + recSize > outBuf.size()) flush();

        std::memcpy(outBuf.data() + outPos,      &rs.key, 8);
        std::memcpy(outBuf.data() + outPos + 8,  &rs.len, 4);
        std::memcpy(outBuf.data() + outPos + 12, rs.payload, rs.len);
        outPos += recSize;

        rs.advance();
    };

    while (!a.finished && !b.finished) {
        if (a.key <= b.key) emit(a);
        else                emit(b);
    }
    while (!a.finished) emit(a);
    while (!b.finished) emit(b);

    flush();
    out.close();

    std::filesystem::remove(A);
    std::filesystem::remove(B);
}

#endif
