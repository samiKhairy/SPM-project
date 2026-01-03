// mergesort_mpi_omp.cpp - MPI + OpenMP hybrid out-of-core mergesort
//
// Design:
//  Phase 0: rank0 computes record-aligned byte splits, broadcasts [start,end) offsets.
//  Phase 1: each rank reads ONLY its byte-range, uses OpenMP task mergesort to generate local runs,
//           then merges local runs -> one sorted file per rank.
//  Phase 2: MPI reduction tree: ranks send their sorted file to partner; receiver merges -> new sorted file.
//           rank0 ends with final file and copies to requested output.

#include <mpi.h>
#include <omp.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "../tools/verifier.hpp"
#include "../tools/record_io.hpp"
#include "../tools/merger.hpp"

namespace fs = std::filesystem;

// ---- NEW: sanity caps for boundary validation ----
static constexpr uint32_t HARD_MAX_LEN = 1u << 20; // 1MB safety cap
static constexpr uint32_t MIN_LEN      = 8;        // set to 8 if your generator guarantees len>=8

static inline double now_sec() {
    using clk = std::chrono::high_resolution_clock;
    return std::chrono::duration<double>(clk::now().time_since_epoch()).count();
}

// ---------------- OpenMP sort-on-offsets (unchanged) ----------------
static inline bool offLess(const std::vector<char>& raw, size_t a, size_t b) {
    return recordio::keyAt(raw, a) < recordio::keyAt(raw, b);
}

static void merge_offsets(std::vector<size_t>& v, std::vector<size_t>& tmp,
                          int l, int m, int r,
                          const std::vector<char>& raw) {
    int i = l, j = m, k = l;
    while (i < m && j < r) {
        if (offLess(raw, v[i], v[j])) tmp[k++] = v[i++];
        else                          tmp[k++] = v[j++];
    }
    while (i < m) tmp[k++] = v[i++];
    while (j < r) tmp[k++] = v[j++];
    for (int x = l; x < r; ++x) v[x] = tmp[x];
}

static void mergesort_task(std::vector<size_t>& v, std::vector<size_t>& tmp,
                           int l, int r,
                           const std::vector<char>& raw,
                           int cutoff) {
    if (r - l <= cutoff) {
        std::sort(v.begin() + l, v.begin() + r,
                  [&](size_t a, size_t b) { return offLess(raw, a, b); });
        return;
    }
    int m = l + (r - l) / 2;

    #pragma omp taskgroup
    {
        #pragma omp task shared(v,tmp,raw) firstprivate(l,m,cutoff)
        mergesort_task(v, tmp, l, m, raw, cutoff);

        #pragma omp task shared(v,tmp,raw) firstprivate(m,r,cutoff)
        mergesort_task(v, tmp, m, r, raw, cutoff);
    }
    merge_offsets(v, tmp, l, m, r, raw);
}

static void parallel_sort_offsets(std::vector<size_t>& offsets,
                                  const std::vector<char>& raw,
                                  int nthreads,
                                  int cutoff) {
    std::vector<size_t> tmp(offsets.size());
    #pragma omp parallel num_threads(nthreads)
    {
        #pragma omp single
        mergesort_task(offsets, tmp, 0, (int)offsets.size(), raw, cutoff);
    }
}

// ---- NEW: robust boundary validator (K consecutive records) ----
static bool validate_chain(const std::vector<char>& buf, size_t p, uint32_t payload_max, int k) {
    size_t cur = p;
    for (int i = 0; i < k; ++i) {
        if (cur + 12 > buf.size()) return false;

        uint32_t len = recordio::lenAt(buf, cur);

        if (len < MIN_LEN) return false;
        if (len > payload_max) return false;
        if (len > HARD_MAX_LEN) return false;

        size_t recSize = 12ull + (size_t)len;
        if (cur + recSize > buf.size()) return false;

        cur += recSize;
    }
    return true;
}

// ---------------- Phase 0: compute record-aligned splits (rank0, window-based) ----------------
static std::vector<uint64_t> compute_splits_rank0(const std::string& file, int P, uint32_t payload_max) {
    std::ifstream in(file, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open input file");

    in.seekg(0, std::ios::end);
    uint64_t fsz = (uint64_t)in.tellg();

    std::vector<uint64_t> splits(P + 1, 0);
    splits[0] = 0;
    splits[P] = fsz;

    const uint64_t WINDOW = 16ull * 1024 * 1024; // 16MB search window
    std::vector<char> buf((size_t)WINDOW);

    const int K = 8; // validate 8 consecutive records

    for (int i = 1; i < P; ++i) {
        uint64_t target = (fsz * (uint64_t)i) / (uint64_t)P;
        uint64_t start  = (target > WINDOW/2) ? (target - WINDOW/2) : 0;
        uint64_t end    = std::min<uint64_t>(fsz, start + WINDOW);

        uint64_t want = end - start;
        in.seekg((std::streamoff)start, std::ios::beg);
        in.read(buf.data(), (std::streamsize)want);
        size_t got = (size_t)in.gcount();
        if (got < 12) { splits[i] = splits[i-1]; continue; }

        size_t off0 = (size_t)std::min<uint64_t>((uint64_t)got - 12, target - start);
        size_t found = (size_t)-1;

        for (size_t p = off0; p + 12 <= got; ++p) {
            if (validate_chain(buf, p, payload_max, K)) { found = p; break; }
        }

        splits[i] = (found == (size_t)-1) ? splits[i-1] : (start + (uint64_t)found);
    }

    // enforce non-decreasing
    for (int i = 1; i < P; ++i) {
        if (splits[i] < splits[i-1]) splits[i] = splits[i-1];
        if (splits[i] == splits[i-1]) {
            std::cerr << "[rank0] WARNING: empty range for rank " << i-1 << " -> " << i << "\n";
        }
    }
    splits[P] = fsz;

    // ---- NEW: print splits on rank0 ----
    std::cerr << "[rank0] splits:\n";
    for (int i = 0; i <= P; ++i) std::cerr << "split[" << i << "]=" << splits[i] << "\n";

    return splits;
}

// ---------------- Read a byte-range into blocks, generate local run files ----------------
struct LocalRunsResult {
    int runs = 0;
    double t_read = 0.0;
    double t_sort = 0.0;
    double t_write = 0.0;
};

static LocalRunsResult generateRuns_slice(const std::string& inputPath,
                                         uint64_t startOff,
                                         uint64_t endOff,
                                         size_t memoryLimitBytes,
                                         int nthreads,
                                         int cutoff,
                                         const fs::path& outDir) {
    LocalRunsResult R;

    std::ifstream in(inputPath, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open input file");

    in.seekg((std::streamoff)startOff, std::ios::beg);
    uint64_t cur = startOff;

    std::vector<char> rawBlock;
    std::vector<size_t> offsets;
    rawBlock.reserve(memoryLimitBytes);
    offsets.reserve(std::max<size_t>(1, memoryLimitBytes / 64));

    int runID = 0;

    while (cur < endOff) {
        double t0 = now_sec();

        uint64_t want = std::min<uint64_t>((uint64_t)memoryLimitBytes, endOff - cur);
        rawBlock.resize((size_t)want);
        in.read(rawBlock.data(), (std::streamsize)want);
        size_t got = (size_t)in.gcount();
        if (got == 0) break;
        rawBlock.resize(got);

        // trim to full records
        size_t pos = 0, lastValidEnd = 0;
        while (pos + 12 <= rawBlock.size()) {
            uint32_t len = recordio::lenAt(rawBlock, pos);
            size_t recSize = 12ull + (size_t)len;
            if (pos + recSize > rawBlock.size()) break;
            pos += recSize;
            lastValidEnd = pos;
        }
        rawBlock.resize(lastValidEnd);

        uint64_t consumed = (uint64_t)lastValidEnd;
        uint64_t unread = (uint64_t)got - consumed;
        if (unread > 0) in.seekg(-(std::streamoff)unread, std::ios::cur);
        cur += consumed;

        double t1 = now_sec();
        R.t_read += (t1 - t0);

        if (rawBlock.empty()) break;

        offsets.clear();
        pos = 0;
        while (pos + 12 <= rawBlock.size()) {
            uint32_t len = recordio::lenAt(rawBlock, pos);
            size_t recSize = 12ull + (size_t)len;
            if (pos + recSize > rawBlock.size()) break;
            offsets.push_back(pos);
            pos += recSize;
        }

        double t2 = now_sec();
        if (offsets.size() > 1) parallel_sort_offsets(offsets, rawBlock, nthreads, cutoff);
        double t3 = now_sec();
        R.t_sort += (t3 - t2);

        double t4 = now_sec();
        fs::path runPath = outDir / ("run_" + std::to_string(runID++) + ".bin");
        std::ofstream out(runPath, std::ios::binary);
        if (!out) throw std::runtime_error("cannot open run output");
        for (size_t off : offsets) recordio::writeAt(out, rawBlock, off);
        out.close();
        double t5 = now_sec();
        R.t_write += (t5 - t4);
    }

    R.runs = runID;
    return R;
}

static constexpr size_t MPI_CHUNK = 32ull * 1024 * 1024;

// ---------------- MPI file transfer helpers ----------------
static void send_file_stream(const fs::path& file, int dest, int tagBase, MPI_Comm comm) {
    uint64_t fsz = (uint64_t)fs::file_size(file);
    MPI_Send(&fsz, 1, MPI_UINT64_T, dest, tagBase + 0, comm);

    std::ifstream in(file, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open file to send: " + file.string());

    std::vector<char> buf(MPI_CHUNK);

    uint64_t sent = 0;
    while (sent < fsz) {
        uint64_t chunk = std::min<uint64_t>((uint64_t)buf.size(), fsz - sent);
        in.read(buf.data(), (std::streamsize)chunk);
        std::streamsize got = in.gcount();

        if (got != (std::streamsize)chunk) {
            throw std::runtime_error("short read while sending: " + file.string());
        }

        MPI_Send(buf.data(), (int)got, MPI_BYTE, dest, tagBase + 1, comm);
        sent += (uint64_t)got;
    }
}

static fs::path recv_file_stream(const fs::path& outFile, int src, int tagBase, MPI_Comm comm) {
    uint64_t fsz = 0;
    MPI_Recv(&fsz, 1, MPI_UINT64_T, src, tagBase + 0, comm, MPI_STATUS_IGNORE);

    std::ofstream out(outFile, std::ios::binary);
    if (!out) throw std::runtime_error("cannot open file to write: " + outFile.string());

    std::vector<char> buf(MPI_CHUNK);

    uint64_t recvd = 0;
    while (recvd < fsz) {
        uint64_t want = std::min<uint64_t>((uint64_t)buf.size(), fsz - recvd);

        MPI_Status st;
        MPI_Recv(buf.data(), (int)want, MPI_BYTE, src, tagBase + 1, comm, &st);

        int got = 0;
        MPI_Get_count(&st, MPI_BYTE, &got);

        if (got <= 0) throw std::runtime_error("recv got 0 bytes (protocol broken)");
        if ((uint64_t)got != want) throw std::runtime_error("short recv (protocol broken)");

        out.write(buf.data(), got);
        recvd += (uint64_t)got;
    }

    out.close();
    return outFile;
}

// ---------------- main ----------------
int main(int argc, char** argv) {
    int required = MPI_THREAD_FUNNELED, provided = 0;
    MPI_Init_thread(&argc, &argv, required, &provided);

    int rank = 0, P = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &P);

    if (argc < 8) {
        if (rank == 0)
            std::cerr << "Usage: mergesort_mpi_omp <file> <N> <PAYLOAD> <MemMB> <threads> <cutoff> <out_final>\n";
        MPI_Finalize();
        return 1;
    }

    std::string file = argv[1];
    uint64_t nRec = std::stoull(argv[2]);
    (void)nRec;

    // ---- NEW: actually use PAYLOAD as max len for validation ----
    uint32_t payload_max = (uint32_t)std::stoul(argv[3]);

    size_t memBytes = (size_t)std::stoull(argv[4]) * 1024ull * 1024ull;
    int nthreads = std::stoi(argv[5]);
    int cutoff   = std::stoi(argv[6]);
    std::string outFinal = argv[7];
    if (cutoff < 1024) cutoff = 1024;

    const char* user = std::getenv("USER");
    std::string u = user ? user : "user";
    fs::path scratch = fs::path("/scratch") / u / ("mpi_omp_r" + std::to_string(rank));
    fs::create_directories(scratch);
    
    // cleanup control (KEEP_SCRATCH=1 to keep files for debugging)
    bool do_cleanup = true;
    if (const char* keep = std::getenv("KEEP_SCRATCH")) {
        if (std::string(keep) == "1") do_cleanup = false;
    }	

    double T0 = now_sec();

    double t_split = 0.0;
    double t_copy_in_total = 0.0; // stays 0 (no copy inside timed section)
    double t_copy_after = 0.0;    // post-copy time (rank0 only)

    std::vector<uint64_t> splits;
    if (rank != 0) splits.resize(P + 1);

    double ts0 = now_sec();
    if (rank == 0) splits = compute_splits_rank0(file, P, payload_max);
    double ts1 = now_sec();
    if (rank == 0) t_split = ts1 - ts0;

    MPI_Bcast(splits.data(), (int)splits.size(), MPI_UINT64_T, 0, MPI_COMM_WORLD);

    uint64_t startOff = splits[rank];
    uint64_t endOff   = splits[rank + 1];

    // ---- Phase 1 (barrier-timed) ----
    MPI_Barrier(MPI_COMM_WORLD);
    double t_phase1_start = now_sec();

    fs::path runDir = scratch / "runs";
    fs::create_directories(runDir);

    auto LR = generateRuns_slice(file, startOff, endOff, memBytes, nthreads, cutoff, runDir);

    std::vector<std::string> runFiles;
    runFiles.reserve(LR.runs);
    for (int i = 0; i < LR.runs; ++i)
        runFiles.push_back((runDir / ("run_" + std::to_string(i) + ".bin")).string());

    fs::path localSorted = scratch / ("sorted_r" + std::to_string(rank) + ".bin");

    size_t mergeReadBudget = std::max<size_t>(256ull * 1024, memBytes / 2);
    size_t mergeOutBuf     = 8ull * 1024 * 1024;
    mergeFiles(runFiles, localSorted.string(), mergeReadBudget, mergeOutBuf);

    MPI_Barrier(MPI_COMM_WORLD);
    double t_phase1_end = now_sec();
    double local_phase1 = t_phase1_end - t_phase1_start;

    // ---- Phase 2 (barrier-timed) ----
    MPI_Barrier(MPI_COMM_WORLD);
    double t_phase2_start = now_sec();

    fs::path curFile = localSorted;

    for (int step = 1; step < P; step *= 2) {
        if (rank % (2 * step) == 0) {
            int partner = rank + step;
            if (partner < P) {
                fs::path recvTmp = scratch / ("recv_from_" + std::to_string(partner) + ".bin");
                recv_file_stream(recvTmp, partner, 1000 + step, MPI_COMM_WORLD);

                fs::path merged = scratch / ("merged_s" + std::to_string(step) + ".bin");
                std::vector<std::string> two = {curFile.string(), recvTmp.string()};
                mergeFiles(two, merged.string(), mergeReadBudget, mergeOutBuf);

                curFile = merged;
            }
        } else {
            int partner = rank - step;
            send_file_stream(curFile, partner, 1000 + step, MPI_COMM_WORLD);
            break;
        }
    }
MPI_Barrier(MPI_COMM_WORLD);
double t_phase2_end = now_sec();
double local_phase2 = t_phase2_end - t_phase2_start;

// end timing here (NO COPY INCLUDED)
double T1 = now_sec();
double local_total = T1 - T0;

// ---- reductions ----
double max_phase1 = 0, max_phase2 = 0, max_total = 0;
MPI_Reduce(&local_phase1, &max_phase1, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
MPI_Reduce(&local_phase2, &max_phase2, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
MPI_Reduce(&local_total,  &max_total,  1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

double max_split = 0, max_copy_in_total = 0;
MPI_Reduce(&t_split, &max_split, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
MPI_Reduce(&t_copy_in_total, &max_copy_in_total, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

// ---- report + verify local (rank0) ----
if (rank == 0) {
    std::cout << "\n=== [MPI+OMP] RESULTS (copy excluded) ===\n";
    std::cout << "Ranks:            " << P << "\n";
    std::cout << "Threads/rank:     " << nthreads << "\n";
    std::cout << "MemMB/rank:       " << (memBytes / (1024ull * 1024ull)) << "\n";
    std::cout << "Cutoff:           " << cutoff << "\n";
    std::cout << "Split(rank0):     " << max_split << " s\n";
    std::cout << "Copy(in total):   " << max_copy_in_total << " s\n";
    std::cout << "Phase1 MAX:       " << max_phase1 << " s\n";
    std::cout << "Phase2 MAX:       " << max_phase2 << " s\n";
    std::cout << "Total  MAX:       " << max_total  << " s\n";
    std::cout << "Unaccounted:      " << (max_total - (max_split + max_phase1 + max_phase2 + max_copy_in_total)) << " s\n";
    std::cout << "Final(local):     " << curFile.string() << "\n";

    verifyOutput(curFile.string(), (size_t)std::stoull(argv[2]));
}

// ---- copy AFTER timing + AFTER verification (rank0 only, no barriers) ----
// ---- copy AFTER timing + AFTER verification (rank0 only) ----
if (rank == 0) {
    double tc0 = now_sec();
    fs::copy_file(curFile, outFinal, fs::copy_options::overwrite_existing);
    double tc1 = now_sec();
    t_copy_after = tc1 - tc0;

    std::cout << "[Post] Copied final output to: " << outFinal << "\n";
    std::cout << "[Post] Copy time (excluded):   " << t_copy_after << " s\n";
}

// Make sure nobody is still reading/writing scratch before deleting
MPI_Barrier(MPI_COMM_WORLD);

// Each rank deletes its own scratch directory
if (do_cleanup) {
    std::error_code ec;
    fs::remove_all(scratch, ec);
    if (ec) {
        std::cerr << "[rank" << rank << "] WARN: cleanup failed: " << ec.message() << "\n";
    }
}

MPI_Finalize();
return 0;
}