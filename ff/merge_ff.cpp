/**
 * merge_ff.cpp
 * Phase 2: Divide & Conquer merge using FastFlow ff_DC
 *
 * Usage:
 * ./merge_ff <run_prefix> <final_out> <K> <mem_budget_mb> <workers> [payload_max]
 *
 * Example:
 * ./merge_ff ff/results/run_ ff/results/final_ff.dat 8 8192 4
 */

#include <algorithm>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>
#include <atomic>

// FastFlow
#include <ff/ff.hpp>
#include <ff/dc.hpp>

// Project IO
#include "../tools/common_sort.hpp"

namespace fs = std::filesystem;

// ------------------ utilities ------------------

static inline uint64_t mb_to_bytes(uint64_t mb)
{
    return mb * 1024ULL * 1024ULL;
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

    sortutil::merge_k_runs(t->inputs, t->output, t->cfg->inbuf, t->cfg->outbuf, t->cfg->payload_max);
}

static void dc_combine(MergeTask *t, std::vector<MergeTask *> &children)
{
    if (children.empty())
        return;

    std::vector<std::string> outputs;
    outputs.reserve(children.size());
    for (const auto *c : children)
        outputs.push_back(c->output);

    sortutil::merge_k_runs(outputs, t->output, t->cfg->inbuf, t->cfg->outbuf, t->cfg->payload_max);

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
            std::cerr << "Usage: ./merge_ff <run_prefix> <final_out> <K> <mem_budget_mb> <workers> [payload_max]\n";
            return 1;
        }

        const std::string prefix = argv[1];
        const std::string final_out = argv[2];
        const int K = std::stoi(argv[3]);
        const uint64_t mem_budget_mb = std::stoull(argv[4]);
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

        const uint64_t total_bytes = mb_to_bytes(mem_budget_mb);
        size_t active_streams = static_cast<size_t>(workers * K);
        if (active_streams == 0)
            active_streams = 1;

        const sortutil::MergeBufferPlan plan = sortutil::plan_merge_buffers(total_bytes, active_streams);
        const size_t inbuf = plan.in_buf_size;
        const size_t outbuf = plan.out_buf_size;

        std::cout << "FastFlow Merge configuration\n"
                  << "  prefix:      " << prefix << "\n"
                  << "  final_out:   " << final_out << "\n"
                  << "  runs:        " << files.size() << "\n"
                  << "  K:           " << K << "\n"
                  << "  workers:     " << workers << "\n"
                  << "  mem_total:   " << mem_budget_mb << " MB\n"
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
