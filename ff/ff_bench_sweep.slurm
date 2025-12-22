#!/bin/bash
#SBATCH --job-name=ff_bench_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=ff_bench_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/project || exit 1

# === CONFIG ===
INPUT=inputs/input_10gb.dat
RUN_DIR=ff/results_bench_sweep
FINAL_OUT=ff/results_bench_sweep/final.dat
MEM_GEN=32         # 32MB per worker task
MEM_MERGE_GB=24    # Total RAM for merge
K=32
WORKERS_LIST=(1 2 4 8 16)

mkdir -p "$RUN_DIR"

for workers in "${WORKERS_LIST[@]}"; do
    echo ">>> [FF] Phase 1: Generation (Budget: ${MEM_GEN}MB, Workers: $workers)"
    rm -rf "$RUN_DIR"/*
    time -p ./ff/bin/run_gen_ff "$INPUT" "$MEM_GEN" "$RUN_DIR/run_" "$workers"
    sync

    echo ">>> [FF] Phase 2: Merge (K=$K, Workers=$workers)"
    time -p ./ff/bin/merge_ff "$RUN_DIR/run_" "$FINAL_OUT" "$K" "$MEM_MERGE_GB" "$workers"

    ./tools/verify_sorted "$FINAL_OUT"
done