#!/bin/bash
#SBATCH --job-name=omp_bench_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=omp_bench_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/project || exit 1
export OMP_PROC_BIND=close
export OMP_PLACES=cores

# === CONFIG ===
INPUT=inputs/input_10gb.dat
RUN_DIR=omp/results_bench_sweep
FINAL_OUT=omp/results_bench_sweep/final.dat
MEM_GEN=32         # 32MB -> ~320 runs
MEM_MERGE_GB=24    # Total RAM for merge
K=32               # 32-way merge
THREADS_LIST=(1 2 4 8 16)

mkdir -p "$RUN_DIR"

generate_runs() {
    echo ">>> [OMP] Phase 1: Generation (Budget: ${MEM_GEN}MB, Threads: $OMP_NUM_THREADS)"
    rm -rf "$RUN_DIR"/*
    time -p ./omp/run_gen_omp "$INPUT" "$MEM_GEN" "$RUN_DIR/run_"
    sync
}

for threads in "${THREADS_LIST[@]}"; do
    export OMP_NUM_THREADS="$threads"
    generate_runs

    echo ">>> [OMP] Phase 2: Merge (K=$K, Threads=$OMP_NUM_THREADS)"
    time -p ./omp/merge_omp "$RUN_DIR/run_" "$FINAL_OUT" "$K" "$MEM_MERGE_GB"

    ./tools/verify_sorted "$FINAL_OUT"
done