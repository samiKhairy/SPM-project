/**
 * merge_ff.cpp
 * Phase 2: Divide & Conquer merge using FastFlow ff_DC
 *
 * Usage:
 * ./merge_ff <run_prefix> <final_out> <K> <TOTAL_MEM_GB> <workers> [payload_max]
 *
 * Example:
 * ./merge_ff ff/results/run_ ff/results/final_ff.dat 8 8 4
 */

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>
#include <atomic>

// FastFlow
#include <ff/ff.hpp>
#include <ff/dc.hpp>

// Project IO
#include "../tools/record_io.hpp"

namespace fs = std::filesystem;

// ------------------ utilities ------------------

static inline uint64_t gb_to_bytes(uint64_t gb)
{
    return gb * 1024ULL * 1024ULL * 1024ULL;
}

static inline void append_bytes(std::vector<char> &buf, size_t &pos, const void *src, size_t n)
{
    if (pos + n > buf.size())
    {
        throw std::runtime_error("Output buffer overflow");
    }
    std::memcpy(buf.data() + pos, src, n);
    pos += n;
}

// Robustly parse run id from filename: base + digits + ".dat"
// This preserves your existing logic to correctly find "run_0.dat" vs "run_10.dat"
static bool parse_run_id(const std::string &base, const fs::path &p, uint64_t &out_id)
{
    const std::string name = p.filename().string();
    if (name.size() < base.size() + 1 + 4)
        return false;
    if (name.rfind(base, 0) != 0)
        return false;
    if (p.extension() != ".dat")
        return false;

    const std::string mid = name.substr(base.size(), name.size() - base.size() - 4);
    if (mid.empty())
        return false;
    for (char c : mid)
        if (c < '0' || c > '9')
            return false;

    try
    {
        out_id = std::stoull(mid);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

static std::vector<std::string> list_runs_from_prefix(const std::string &prefix, const std::string &final_out_abs)
{
    fs::path pref(prefix);
    fs::path dir = pref.parent_path();
    if (dir.empty())
        dir = ".";
    const std::string base = pref.filename().string();

    if (!fs::exists(dir) || !fs::is_directory(dir))
        throw std::runtime_error("Run directory not found: " + dir.string());

    struct Item
    {
        uint64_t id;
        std::string path;
    };
    std::vector<Item> items;

    for (auto const &e : fs::directory_iterator(dir))
    {
        if (!e.is_regular_file())
            continue;
        uint64_t id = 0;
        if (!parse_run_id(base, e.path(), id))
            continue;

        const std::string abs = fs::absolute(e.path()).string();
        if (abs == final_out_abs)
            continue;
        items.push_back({id, abs});
    }

    std::sort(items.begin(), items.end(), [](auto const &a, auto const &b)
              { return a.id < b.id; });

    std::vector<std::string> files;
    files.reserve(items.size());
    for (auto const &it : items)
        files.push_back(it.path);
    return files;
}

// ------------------ merge core ------------------

struct RunCtx
{
    std::ifstream in;
    std::unique_ptr<recio::RecordReader> rr;
    recio::RecordView cur;
    bool has = false;
};

struct HeapNode
{
    uint64_t key;
    size_t run_idx;
    // Min-heap: greater comparison
    bool operator>(const HeapNode &o) const
    {
        if (key != o.key)
            return key > o.key;
        return run_idx > o.run_idx; // stability tie-breaker
    }
};

static void merge_k_runs(const std::vector<std::string> &inputs,
                         const std::string &output,
                         size_t inbuf_bytes,
                         size_t outbuf_bytes,
                         uint32_t payload_max)
{
    if (inputs.empty())
        throw std::runtime_error("merge_k_runs: empty input set");

    std::vector<RunCtx> runs(inputs.size());
    std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<HeapNode>> heap;

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        runs[i].in.open(inputs[i], std::ios::binary);
        if (!runs[i].in)
            throw std::runtime_error("Cannot open input run: " + inputs[i]);

        runs[i].rr = std::make_unique<recio::RecordReader>(runs[i].in, inbuf_bytes, payload_max);
        runs[i].has = runs[i].rr->next(runs[i].cur);
        if (runs[i].has)
            heap.push({runs[i].cur.key, i});
    }

    std::ofstream out(output, std::ios::binary);
    if (!out)
        throw std::runtime_error("Cannot open output: " + output);

    std::vector<char> out_buf(outbuf_bytes);
    size_t out_pos = 0;

    while (!heap.empty())
    {
        HeapNode top = heap.top();
        heap.pop();
        RunCtx &rc = runs[top.run_idx];

        const size_t rec_sz = 12 + static_cast<size_t>(rc.cur.len);
        if (out_pos + rec_sz > outbuf_bytes)
        {
            out.write(out_buf.data(), static_cast<std::streamsize>(out_pos));
            out_pos = 0;
        }
        append_bytes(out_buf, out_pos, &rc.cur.key, 8);
        append_bytes(out_buf, out_pos, &rc.cur.len, 4);
        append_bytes(out_buf, out_pos, rc.cur.payload, rc.cur.len);

        rc.has = rc.rr->next(rc.cur);
        if (rc.has)
            heap.push({rc.cur.key, top.run_idx});
    }

    if (out_pos > 0)
    {
        out.write(out_buf.data(), static_cast<std::streamsize>(out_pos));
    }

    out.close();
    if (!out.good())
        throw std::runtime_error("Failed to close output cleanly: " + output);
}

// ------------------ divide & conquer ------------------

struct DcConfig
{
    size_t leaf_k = 2;
    size_t inbuf = 0;
    size_t outbuf = 0;
    uint32_t payload_max = 0;
    std::string temp_dir;
    std::string final_out;
    std::atomic<uint64_t> *temp_id = nullptr;
};

struct MergeTask
{
    std::vector<std::string> inputs;
    std::string output;
    bool owns_output = false;
    const DcConfig *cfg = nullptr;
};

static std::string make_temp_path(const DcConfig &cfg)
{
    const uint64_t id = cfg.temp_id->fetch_add(1, std::memory_order_relaxed);
    fs::path dir(cfg.temp_dir.empty() ? "." : cfg.temp_dir);
    return (dir / ("ffdc_tmp_" + std::to_string(id) + ".dat")).string();
}

static void dc_divide(MergeTask *t, std::vector<MergeTask *> &children)
{
    const size_t mid = t->inputs.size() / 2;
    std::vector<std::string> left(t->inputs.begin(), t->inputs.begin() + mid);
    std::vector<std::string> right(t->inputs.begin() + mid, t->inputs.end());

    children.push_back(new MergeTask{std::move(left), make_temp_path(*t->cfg), true, t->cfg});
    children.push_back(new MergeTask{std::move(right), make_temp_path(*t->cfg), true, t->cfg});
}

static bool dc_cond(MergeTask *t)
{
    return t->inputs.size() <= t->cfg->leaf_k;
}

static void dc_base(MergeTask *t)
{
    if (t->inputs.empty())
        throw std::runtime_error("dc_base: empty input list");

    if (t->inputs.size() == 1)
    {
        const std::string &single = t->inputs.front();
        if (fs::absolute(single) == fs::absolute(t->output))
        {
            t->owns_output = false;
            return;
        }
        if (t->output == t->cfg->final_out)
        {
            fs::rename(single, t->output);
            t->owns_output = false;
            return;
        }
        t->output = single;
        t->owns_output = false;
        return;
    }

    merge_k_runs(t->inputs, t->output, t->cfg->inbuf, t->cfg->outbuf, t->cfg->payload_max);
}

static void dc_combine(MergeTask *t, std::vector<MergeTask *> &children)
{
    if (children.empty())
        return;

    std::vector<std::string> outputs;
    outputs.reserve(children.size());
    for (const auto *c : children)
        outputs.push_back(c->output);

    merge_k_runs(outputs, t->output, t->cfg->inbuf, t->cfg->outbuf, t->cfg->payload_max);

    for (auto *c : children)
    {
        if (c->owns_output && fs::absolute(c->output) != fs::absolute(t->cfg->final_out))
        {
            std::error_code ec;
            fs::remove(c->output, ec);
        }
        delete c;
    }
    children.clear();
}

// ------------------ main orchestration ------------------

int main(int argc, char **argv)
{
    try
    {
        if (argc < 6)
        {
            std::cerr << "Usage: ./merge_ff <run_prefix> <final_out> <K> <TOTAL_MEM_GB> <workers> [payload_max]\n";
            return 1;
        }

        const std::string prefix = argv[1];
        const std::string final_out = argv[2];
        const int K = std::stoi(argv[3]);
        const uint64_t total_mem_gb = std::stoull(argv[4]);
        const int workers = std::stoi(argv[5]);

        uint32_t payload_max = recio::HARD_PAYLOAD_MAX;
        if (argc > 6)
            payload_max = static_cast<uint32_t>(std::stoul(argv[6]));

        if (K <= 1)
            throw std::runtime_error("K must be >= 2");
        if (workers <= 0)
            throw std::runtime_error("workers must be > 0");

        const std::string final_abs = fs::absolute(final_out).string();
        std::vector<std::string> files = list_runs_from_prefix(prefix, final_abs);

        if (files.empty())
        {
            throw std::runtime_error("No input runs found for prefix: " + prefix);
        }

        // ----- Memory Sizing (Updated for safety) -----
        const uint64_t total_bytes = gb_to_bytes(total_mem_gb);
        // Use 75% of total memory to be safe
        const size_t safe_mem = (total_bytes * 3) / 4;

        // Calculate max active input streams (Workers * K files per worker)
        size_t active_streams = static_cast<size_t>(workers * K);
        if (active_streams == 0)
            active_streams = 1;

        // Buffer per stream
        size_t inbuf = safe_mem / active_streams;
        const size_t outbuf = 16ULL * 1024 * 1024; // 16MB output buffer fixed

        // Clamp to sane limits (4MB - 64MB)
        if (inbuf < 4 * 1024 * 1024)
            inbuf = 4 * 1024 * 1024;
        if (inbuf > 64 * 1024 * 1024)
            inbuf = 64 * 1024 * 1024;

        std::cout << "FastFlow Merge configuration\n"
                  << "  prefix:      " << prefix << "\n"
                  << "  final_out:   " << final_out << "\n"
                  << "  runs:        " << files.size() << "\n"
                  << "  K:           " << K << "\n"
                  << "  workers:     " << workers << "\n"
                  << "  mem_total:   " << total_mem_gb << " GB\n"
                  << "  inbuf/run:   " << (inbuf / 1024 / 1024) << " MB\n"
                  << "  outbuf:      " << (outbuf / 1024 / 1024) << " MB\n";

        std::atomic<uint64_t> temp_id{0};
        fs::path pref(prefix);
        fs::path dir = pref.parent_path();
        if (dir.empty())
            dir = ".";

        DcConfig cfg{
            static_cast<size_t>(K),
            inbuf,
            outbuf,
            payload_max,
            dir.string(),
            final_out,
            &temp_id};

        MergeTask root{files, final_out, false, &cfg};

        ff::ff_DC<MergeTask> dc(dc_divide, dc_combine, dc_cond, dc_base, workers);
        if (dc.run_then_freeze() < 0)
            throw std::runtime_error("Failed to start ff_DC");
        dc.offload(&root);
        dc.offload((MergeTask *)FF_EOS);
        dc.wait();

        std::cout << "Merge complete -> " << final_out << "\n";
        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 1;
    }
}
