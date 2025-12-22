/**
 * run_generation_ff.cpp
 * Phase 1: Run generation using FastFlow (Pipeline + Farm)
 *
 * Architecture:
 * [Reader] ---> [Farm (Workers) -> Collector] ---> [Writer]
 *
 * FIX: 'ff_node' is abstract. We created a concrete 'Identity' node
 * to act as the Collector.
 */

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// FastFlow Includes
#include <ff/ff.hpp>
#include <ff/farm.hpp>
#include <ff/pipeline.hpp>

#include "../tools/record_io.hpp"

// --- Data Structures ---
struct Meta
{
    uint64_t key;
    uint64_t offset;
    uint32_t len;
};

struct Task
{
    uint64_t run_id;
    std::vector<char> payload_buffer;
    std::vector<Meta> meta;
};

static inline void append_bytes_local(std::vector<char> &buf, const void *src, size_t n)
{
    const size_t old = buf.size();
    buf.resize(old + n);
    std::memcpy(buf.data() + old, src, n);
}

// ---------------------- Helper Nodes ----------------------

// FIX: Concrete Identity Node for the Collector
struct Identity : ff::ff_node
{
    void *svc(void *task) override
    {
        return task; // Just pass the pointer through
    }
};

// ---------------------- Reader (Source) ----------------------
struct Reader : ff::ff_node
{
    std::string input_path;
    uint64_t task_budget;
    uint32_t payload_max;

    Reader(std::string path, uint64_t budget, uint32_t pmax)
        : input_path(std::move(path)), task_budget(budget), payload_max(pmax) {}

    void *svc(void *) override
    {
        try
        {
            std::ifstream in(input_path, std::ios::binary);
            if (!in)
            {
                std::cerr << "Reader: cannot open input file: " << input_path << "\n";
                return NULL;
            }

            recio::RecordReader rr(in, 64 * 1024 * 1024, payload_max);
            recio::RecordView rv;

            bool has_stash = false;
            recio::RecordView stash;
            std::vector<char> stash_payload;

            uint64_t run_id = 0;

            while (true)
            {
                Task *t = new Task();
                t->run_id = run_id++;

                const uint64_t safe_reserve = std::min<uint64_t>(task_budget, 512ULL * 1024 * 1024);
                t->payload_buffer.reserve(safe_reserve);
                t->meta.reserve(safe_reserve / 100);

                auto memory_used = [&]() -> uint64_t
                {
                    return t->payload_buffer.size() + t->meta.size() * sizeof(Meta);
                };

                auto consume_record = [&](const recio::RecordView &rec)
                {
                    uint64_t off = t->payload_buffer.size();
                    t->payload_buffer.resize(off + rec.len);
                    std::memcpy(t->payload_buffer.data() + off, rec.payload, rec.len);
                    t->meta.push_back({rec.key, off, rec.len});
                };

                if (has_stash)
                {
                    consume_record(stash);
                    has_stash = false;
                    stash_payload.clear();
                }

                while (true)
                {
                    if (!t->meta.empty() && memory_used() >= task_budget)
                        break;
                    if (!rr.next(rv))
                        break; // EOF

                    uint64_t projected = memory_used() + rv.len + sizeof(Meta);
                    if (!t->meta.empty() && projected > task_budget)
                    {
                        stash_payload.resize(rv.len);
                        std::memcpy(stash_payload.data(), rv.payload, rv.len);
                        stash.key = rv.key;
                        stash.len = rv.len;
                        stash.payload = stash_payload.data();
                        has_stash = true;
                        break;
                    }
                    consume_record(rv);
                }

                if (t->meta.empty())
                {
                    delete t;
                    return NULL; // EOS
                }
                ff_send_out((void *)t);
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << "Reader Exception: " << e.what() << "\n";
            return NULL;
        }
        return NULL;
    }
};

// ---------------------- Worker (Farm) ----------------------
struct Worker : ff::ff_node
{
    void *svc(void *task) override
    {
        Task *t = (Task *)task;
        try
        {
            std::sort(t->meta.begin(), t->meta.end(),
                      [](const Meta &a, const Meta &b)
                      {
                          return a.key < b.key;
                      });
            return (void *)t;
        }
        catch (...)
        {
            delete t;
            return NULL;
        }
    }
};

// ---------------------- Writer (Sink) ----------------------
struct Writer : ff::ff_node
{
    std::string run_prefix;
    std::vector<char> out_buffer;

    Writer(std::string prefix) : run_prefix(std::move(prefix))
    {
        out_buffer.reserve(16 * 1024 * 1024);
    }

    void *svc(void *task) override
    {
        Task *t = (Task *)task;
        try
        {
            std::string fname = run_prefix + std::to_string(t->run_id) + ".dat";
            std::ofstream out(fname, std::ios::binary);
            if (!out)
                throw std::runtime_error("Cannot create file: " + fname);

            out_buffer.clear();
            const size_t OUT_LIMIT = 16 * 1024 * 1024;

            for (const auto &m : t->meta)
            {
                size_t rec_size = 12 + m.len;
                if (out_buffer.size() + rec_size > OUT_LIMIT)
                {
                    out.write(out_buffer.data(), out_buffer.size());
                    out_buffer.clear();
                }
                append_bytes_local(out_buffer, &m.key, 8);
                append_bytes_local(out_buffer, &m.len, 4);
                append_bytes_local(out_buffer, t->payload_buffer.data() + m.offset, m.len);
            }

            if (!out_buffer.empty())
            {
                out.write(out_buffer.data(), out_buffer.size());
            }

            std::cout << "Wrote run: " << fname << " (" << t->meta.size() << " recs)\n";
            delete t;
            return GO_ON;
        }
        catch (const std::exception &e)
        {
            std::cerr << "Writer Exception: " << e.what() << "\n";
            delete t;
            return NULL;
        }
    }
};
// ... imports and structs remain the same ...

int main(int argc, char **argv)
{
    // UPDATE usage message
    if (argc < 4)
    {
        std::cerr << "Usage: ./run_gen_ff <input> <MEM_BUDGET_MB> <run_prefix> [n_workers] [payload_max]\n";
        return 1;
    }

    const std::string input = argv[1];
    // FIX: Read as total MB budget (matching seq/omp)
    const uint64_t mem_budget_mb = std::stoull(argv[2]);
    const std::string prefix = argv[3];
    const int n_workers = (argc > 4) ? std::stoi(argv[4]) : 4;

    uint32_t payload_max = recio::HARD_PAYLOAD_MAX;
    if (argc > 5)
        payload_max = (uint32_t)std::stoul(argv[5]);

    if (n_workers <= 0)
    {
        std::cerr << "Error: workers must be > 0\n";
        return 1;
    }

    const uint64_t total_budget_bytes = mem_budget_mb * 1024ULL * 1024ULL;
    const uint64_t min_task_budget = 16ULL * 1024ULL * 1024ULL; // 16MB minimum preferred
    uint64_t task_budget = total_budget_bytes / static_cast<uint64_t>(n_workers);

    if (total_budget_bytes < static_cast<uint64_t>(n_workers) * min_task_budget)
    {
        std::cerr << "ERROR: Total budget is too small for the requested worker count.\n"
                  << "       Required minimum: " << (static_cast<uint64_t>(n_workers) * min_task_budget / 1024.0 / 1024.0)
                  << " MB (" << min_task_budget / 1024.0 / 1024.0 << " MB per worker)\n";
        return 1;
    }

    task_budget = std::max(task_budget, min_task_budget);

    std::cout << "FastFlow RunGen Config:\n"
              << "  Total Budget:  " << mem_budget_mb << " MB\n"
              << "  Workers:       " << n_workers << "\n"
              << "  Budget/Worker: " << (task_budget / 1024.0 / 1024.0) << " MB\n";

    Reader reader(input, task_budget, payload_max);
    Writer writer(prefix);

    // ... The rest of the pipeline setup remains exactly the same ...
    std::vector<ff::ff_node *> workers;
    workers.reserve(n_workers);
    for (int i = 0; i < n_workers; ++i)
    {
        workers.push_back(new Worker());
    }

    ff::ff_farm farm;
    farm.add_workers(workers);
    farm.add_collector(new Identity());
    farm.set_scheduling_ondemand();

    ff::ff_pipeline pipe;
    pipe.add_stage(&reader);
    pipe.add_stage(&farm);
    pipe.add_stage(&writer);

    if (pipe.run_and_wait_end() < 0)
    {
        std::cerr << "FastFlow pipeline runtime error.\n";
        return 1;
    }

    std::cout << "FastFlow Phase 1 Complete.\n";
    return 0;
}
