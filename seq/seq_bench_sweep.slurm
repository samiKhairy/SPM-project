#!/bin/bash
#SBATCH --job-name=seq_bench_sweep
#SBATCH --output=seq_bench_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/project || exit 1

# === CONFIG ===
INPUT=inputs/input_10gb.dat
RUN_DIR=seq/results_bench_sweep
FINAL_OUT=seq/results_bench_sweep/final.dat
MEM_GEN=32  # 32MB chunks -> ~320 runs

mkdir -p "$RUN_DIR"

echo ">>> [Seq] Phase 1: Generation (Budget: ${MEM_GEN}MB)"
rm -rf "$RUN_DIR"/*
time -p ./seq/bin/run_gen_seq "$INPUT" "$MEM_GEN" "$RUN_DIR/run_"

NUM_RUNS=$(ls -1 "$RUN_DIR"/run_*.dat | wc -l)
echo "    Generated $NUM_RUNS runs"
sync

echo ">>> [Seq] Phase 2: Merge ($NUM_RUNS runs)"
# Seq merges all runs in one pass (K = NUM_RUNS)
time -p ./seq/bin/merge_seq "$RUN_DIR/run_" "$NUM_RUNS" "$FINAL_OUT"

./tools/verify_sorted "$FINAL_OUT"