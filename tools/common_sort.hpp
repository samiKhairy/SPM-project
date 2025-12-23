// common_sort.hpp
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

#include "record_io.hpp"

namespace sortutil
{
    struct Meta
    {
        uint64_t key = 0;
        uint64_t offset = 0;
        uint32_t len = 0;
    };

    struct RunCtx
    {
        std::ifstream in;
        std::unique_ptr<recio::RecordReader> rr;
        recio::RecordView cur;
        bool has = false;

        RunCtx() = default;
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
        uint64_t key = 0;
        size_t run_idx = 0;
    };

    struct HeapCmp
    {
        bool operator()(const HeapNode &a, const HeapNode &b) const
        {
            if (a.key != b.key)
                return a.key > b.key;
            return a.run_idx > b.run_idx;
        }
    };

    inline void append_bytes(std::vector<char> &buf, size_t &pos, const void *src, size_t n)
    {
        if (pos + n > buf.size())
        {
            throw std::runtime_error("Output buffer overflow");
        }
        std::memcpy(buf.data() + pos, src, n);
        pos += n;
    }

    struct RunBufferPlan
    {
        size_t in_buf_size = 0;
        size_t out_buf_size = 0;
        size_t safety_gap = 0;
        size_t payload_budget = 0;
    };

    struct MergeBufferPlan
    {
        size_t in_buf_size = 0;
        size_t out_buf_size = 0;
        size_t safety_gap = 0;
    };

    inline size_t clamp_size(size_t v, size_t lo, size_t hi)
    {
        if (v < lo)
            return lo;
        if (v > hi)
            return hi;
        return v;
    }

    inline RunBufferPlan plan_run_buffers(size_t total_bytes, uint32_t payload_max)
    {
        const size_t in_buf = clamp_size(total_bytes / 4, 8ull << 20, 128ull << 20);
        const size_t out_buf = clamp_size(total_bytes / 10, 8ull << 20, 64ull << 20);
        const size_t safety = total_bytes / 20;

        if (total_bytes <= in_buf + out_buf + safety + payload_max)
        {
            throw std::runtime_error("Memory budget too small for buffers + safety");
        }

        RunBufferPlan plan;
        plan.in_buf_size = in_buf;
        plan.out_buf_size = out_buf;
        plan.safety_gap = safety;
        plan.payload_budget = total_bytes - in_buf - out_buf - safety - payload_max;
        return plan;
    }

    inline MergeBufferPlan plan_merge_buffers(size_t total_bytes, size_t streams)
    {
        const size_t out_buf = clamp_size(total_bytes / 10, 8ull << 20, 64ull << 20);
        const size_t safety = total_bytes / 20;
        if (total_bytes <= out_buf + safety)
        {
            throw std::runtime_error("Memory budget too small for merge buffers");
        }

        const size_t remaining = total_bytes - out_buf - safety;
        const size_t denom = streams > 0 ? streams : 1;
        const size_t in_buf = clamp_size(remaining / denom, 1ull << 20, 64ull << 20);

        MergeBufferPlan plan;
        plan.in_buf_size = in_buf;
        plan.out_buf_size = out_buf;
        plan.safety_gap = safety;
        return plan;
    }

    inline uint64_t merge_k_runs(const std::vector<std::string> &inputs,
                                 const std::string &output,
                                 size_t in_buf_size,
                                 size_t out_buf_size,
                                 uint32_t payload_max)
    {
        if (inputs.empty())
        {
            throw std::runtime_error("merge_k_runs: empty input set");
        }

        std::vector<RunCtx> runs(inputs.size());
        std::priority_queue<HeapNode, std::vector<HeapNode>, HeapCmp> heap;

        for (size_t i = 0; i < inputs.size(); ++i)
        {
            runs[i] = RunCtx(inputs[i], in_buf_size, payload_max);
            if (runs[i].has)
            {
                heap.push(HeapNode{runs[i].cur.key, i});
            }
        }

        std::ofstream out(output, std::ios::binary);
        if (!out)
        {
            throw std::runtime_error("Cannot create output: " + output);
        }

        std::vector<char> out_buf(out_buf_size);
        size_t out_pos = 0;
        uint64_t bytes_written = 0;

        while (!heap.empty())
        {
            const HeapNode top = heap.top();
            heap.pop();
            RunCtx &rc = runs[top.run_idx];

            const size_t rec_size = 12 + static_cast<size_t>(rc.cur.len);
            if (out_pos + rec_size > out_buf.size())
            {
                out.write(out_buf.data(), static_cast<std::streamsize>(out_pos));
                if (!out.good())
                {
                    throw std::runtime_error("Write failed to output: " + output);
                }
                out_pos = 0;
            }

            append_bytes(out_buf, out_pos, &rc.cur.key, 8);
            append_bytes(out_buf, out_pos, &rc.cur.len, 4);
            append_bytes(out_buf, out_pos, rc.cur.payload, rc.cur.len);
            bytes_written += rec_size;

            rc.has = rc.rr->next(rc.cur);
            if (rc.has)
                heap.push(HeapNode{rc.cur.key, top.run_idx});
        }

        if (out_pos > 0)
        {
            out.write(out_buf.data(), static_cast<std::streamsize>(out_pos));
            if (!out.good())
            {
                throw std::runtime_error("Final write failed to output: " + output);
            }
        }

        out.close();
        if (!out.good())
        {
            throw std::runtime_error("Failed to close output: " + output);
        }

        return bytes_written;
    }
} // namespace sortutil
