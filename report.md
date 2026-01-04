# Project 1 Report: External Merge Sort Variants

## 1. Goals and Dataset Format
- Task: build an external merge sort for large record files and explore parallel/distributed variants.
- Record layout: 8-byte key, 4-byte payload length, variable payload bytes (generator bounds payload length).【tools/record_io.hpp†L10-L49】
- Provided utilities: data generator, verification, and k-way/2-way merge helpers for run merging.【tools/datagen.cpp†L1-L73】【tools/verifier.hpp†L1-L59】【tools/merger.hpp†L1-L117】
- Metrics captured: wall-clock breakdown (read, sort, write, merge) and validation counts across implementations.

## 2. Sequential Baseline
- File: `seq/mergesort_seq.cpp` implements single-threaded external mergesort.
- Phase 1: read chunks up to the memory budget, trim to full records, index offsets, std::sort by key, and write each run sequentially to disk.【seq/mergesort_seq.cpp†L16-L104】
- Phase 2: reuse shared `mergeFiles` helper with configurable read/output buffers, then verify output ordering and count.【seq/mergesort_seq.cpp†L106-L141】
- Timing: accumulates per-stage wall-clock and reports total wall time.

## 3. OpenMP Version (Intra-run Parallelism)
- File: `omp/mergesort_omp.cpp` keeps the sequential pipeline but parallelizes the run sorting step with OpenMP tasks.
- Uses task-based mergesort on offsets with a configurable cutoff to avoid tiny tasks; tasks share preallocated temporary buffers to reduce allocations.【omp/mergesort_omp.cpp†L20-L86】
- Run generation mirrors the sequential logic (record-safe buffering, trimming, indexing) but calls `parallel_sort_offsets` when offsets exceed one element.【omp/mergesort_omp.cpp†L88-L169】
- Merge phase intentionally remains sequential to isolate intra-run speedups; timing includes thread count, cutoff, and per-stage durations.【omp/mergesort_omp.cpp†L171-L223】

## 4. FastFlow Version (Pipeline Parallelism on One Node)
- File: `ff/mergesort_ff.cpp` builds an emitter–worker–collector farm.
- Emitter: reads fixed-size blocks (capped to memory budget), enforces record boundaries, and builds task objects containing raw bytes and offsets while timing read overhead.【ff/mergesort_ff.cpp†L26-L102】
- Workers: sort offsets with `std::sort` and write each run to disk, tracking local sort/write contributions so total work can be summed post-run.【ff/mergesort_ff.cpp†L104-L160】【ff/mergesort_ff.cpp†L222-L242】
- Collector: counts completed runs; farm output feeds the sequential k-way merge for finalization.【ff/mergesort_ff.cpp†L162-L176】【ff/mergesort_ff.cpp†L244-L271】
- Metrics distinguish overlapped wall-clock (Phase 1) from accumulated worker work, clarifying pipeline behavior.

## 5. Distributed MPI + OpenMP Version
- File: `mpi/mergesort_mpi_omp.cpp` couples record-aligned input partitioning, intra-rank OpenMP sorting, and MPI tree reduction.
- Phase 0 (rank 0): scans the file in sliding windows to choose partition boundaries that start on valid record chains, then broadcasts byte offsets to ranks.【mpi/mergesort_mpi_omp.cpp†L22-L120】【mpi/mergesort_mpi_omp.cpp†L185-L229】
- Phase 1: each rank reads its byte slice, trims partial tails, indexes offsets, sorts with task-based mergesort, writes per-run files, and merges them locally using shared merger utilities.【mpi/mergesort_mpi_omp.cpp†L122-L196】【mpi/mergesort_mpi_omp.cpp†L231-L301】
- Phase 2: binary-reduction tree where senders stream files via MPI, receivers merge pairs with large buffers to minimize passes; the final file resides on rank 0 for verification and optional copy-out.【mpi/mergesort_mpi_omp.cpp†L303-L422】【mpi/mergesort_mpi_omp.cpp†L424-L497】
- Safety: payload length validation caps, empty-range warnings, scratch-space cleanup guards, and optional KEEP_SCRATCH flag for debugging.【mpi/mergesort_mpi_omp.cpp†L14-L59】【mpi/mergesort_mpi_omp.cpp†L497-L525】

## 6. Build and Data Tooling
- `tools/datagen.cpp` produces synthetic datasets with controllable record counts, payload caps, and optional splits for MPI inputs.【tools/datagen.cpp†L1-L73】
- `tools/merger.hpp` centralizes k-way merging with buffered RunStream readers and optional 2-way merge for MPI reduction; used by all variants.【tools/merger.hpp†L1-L177】
- `scripts/common.sh` packages helper functions for SLURM runs: workload presets, dataset creation, scratch management, and build of datagen.【scripts/common.sh†L1-L42】
- Each implementation directory contains `new.sh` examples and `bin/` targets (not modified here) for cluster submission.

## 7. Performance Highlights (Completed Experiments)
All experiments used 20M records unless stated otherwise; CPU-bound uses 16-byte payloads, IO-bound uses 128-byte payloads.
- Thread scaling (IO-bound 20M, 128B payload): OpenMP speedup rises from 1.26× (1 thread) to 1.83× (8 threads); FastFlow improves from 1.11× to 2.57× due to pipeline overlap.【plots and csv files/n-20m-payload-128-i-o-bound-speedups-.csv†L1-L6】
- Fixed-size throughput (CPU-bound 20M, 16B payload): OpenMP at 16 threads reaches 6.11s (1.59×), while FastFlow at 16 workers reaches 4.64s (2.10×) versus 9.73s sequential.【plots and csv files/20M,payload-16- CPU-bound-Vary N.csv†L1-L4】
- Fixed-size throughput (IO-bound 20M, 128B payload): OpenMP at 16 threads yields 10.69s (1.21×); FastFlow achieves 7.30s (1.77×).【plots and csv files/20M,payload-128- IO-bound-Vary N.csv†L1-L4】
- Record-count scaling (CPU-bound, 16B payload, 16 threads/workers): FastFlow maintains ~2× speedup across 10–30M records, while OpenMP varies 1.52–1.68×, showing pipeline resilience when run counts grow.【plots and csv files/n-10m-20m-30m-nbsp-payload-16-cpu-bound-speedups-.csv†L1-L4】
- Record-count efficiency (IO-bound, 128B payload, 16 threads/workers): efficiency remains 0.10–0.20 as N grows (OpenMP modest, FastFlow higher but still IO-limited).【plots and csv files/n-10m-20m-30m-payload-128-i-o-bound-efficiency.csv†L1-L4】
- Payload sensitivity: increasing payload from 16B to 128B shifts bottlenecks toward IO; pipeline overlap in FastFlow mitigates impact more effectively than OpenMP’s intra-run parallelism (see above fixed-size runs). Strong/weak scaling for MPI+OMP remains pending.

## 8. Bottlenecks, Challenges, and Remedies
- **Record alignment under bounded buffers:** Each reader trims partial trailing records and repositions the stream to preserve alignment, avoiding corrupt offsets across all implementations.【seq/mergesort_seq.cpp†L28-L63】【ff/mergesort_ff.cpp†L41-L77】【mpi/mergesort_mpi_omp.cpp†L153-L196】
- **Task granularity vs. overhead (OpenMP):** Added `cutoff` to limit task spawning and fallback to serial sort for small segments; defaults avoid tiny tasks but remain tunable per workload.【omp/mergesort_omp.cpp†L52-L86】
- **Pipeline back-pressure (FastFlow):** Bounded block size (min of mem budget and 128MB cap) prevents emitter from overwhelming disks and balances worker throughput.【ff/mergesort_ff.cpp†L186-L211】
- **Distributed partition correctness:** Rank 0 performs windowed boundary search with payload validation to ensure each rank starts on a valid record chain, preventing key overlap and supporting variable payloads.【mpi/mergesort_mpi_omp.cpp†L40-L120】
- **Merge fan-in cost:** Shared `mergeFiles` uses per-run buffering derived from budgets to cap memory while limiting refill frequency; MPI two-way merge forces larger per-run buffers to reduce passes.【tools/merger.hpp†L13-L117】【tools/merger.hpp†L119-L177】
- **Verification coverage:** All variants call the same verifier to check sorted order and record count, catching boundary or merging regressions quickly.【seq/mergesort_seq.cpp†L138-L141】【omp/mergesort_omp.cpp†L213-L223】【ff/mergesort_ff.cpp†L262-L271】【mpi/mergesort_mpi_omp.cpp†L436-L475】

## 9. Distributed Cost Model (MPI + OpenMP)
Let P be ranks, T threads per rank, M the per-rank memory budget, R total records, and B average payload size.
- **Local run generation (Phase 1):** Each rank scans ~R/P records. Cost ≈ O((R/P) log(R/P)/T) compute plus O(R/P · B / disk_bw) IO. Task cutoff bounds scheduling overhead to O(R/P ÷ cutoff).
- **Local merge:** k-way merge where k ≈ (R/P · B)/M. Cost ≈ O((R/P) log k) with buffer hits amortizing disk refills; out-buffer size (8MB) keeps flush frequency low.
- **MPI tree reduction (Phase 2):** log₂P rounds of 2-way merges. Communication cost per round ≈ message_volume/BW + latency, where message_volume halves each level. Overlap is limited because merges start after full file receipt; however, stream-based receive/send avoids extra copies and allows merge to start immediately after each recv completes.
- **Bottlenecks:** slowest rank in Phase 1 (disk variability or large k) dictates barrier exit; Phase 2 bounded by network bandwidth when payloads are large. Payload validation and stream transfers add small constant overheads.
- **Optimization levers:** increase per-run buffers in Phase 2 to reduce passes, tune cutoff and threads per rank to balance CPU vs disk, and pre-create scratch on local SSD to avoid shared storage contention.

## 10. Status and Next Steps
- Completed: sequential baseline, OpenMP intra-run parallelism, FastFlow pipeline parallelism, MPI+OMP distributed merge tree, dataset tooling, and verification.
- Pending: strong-scaling and weak-scaling experiments for MPI+OMP; potential extension to overlap communication with merging (e.g., chunked receive-to-merge) and adaptive block sizing per workload.
- Suggested validation: run `verifyOutput` on final files and capture per-stage timers to track regressions when adding further optimizations.
