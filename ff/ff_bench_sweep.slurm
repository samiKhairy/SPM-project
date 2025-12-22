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
INPUTS=(
  inputs/input_10gb_p256.dat
  inputs/input_10gb_p8192.dat
  inputs/input_20gb_p256.dat
  inputs/input_20gb_p8192.dat
)
MEM_GEN_LIST=(1024 512) # MB per worker task: 1GB primary, 512MB backup
RUN_ROOT=ff/results_bench_sweep
MEM_MERGE_GB=64
K=32
WORKERS_LIST=(1 2 4 8 16)

mkdir -p "$RUN_ROOT"

for input in "${INPUTS[@]}"; do
    input_base=$(basename "$input" .dat)
    for mem_gen in "${MEM_GEN_LIST[@]}"; do
        for workers in "${WORKERS_LIST[@]}"; do
            run_dir="$RUN_ROOT/$input_base/mem_${mem_gen}mb/w${workers}"
            final_out="$run_dir/final.dat"

            mkdir -p "$run_dir"
            echo ">>> [FF] Phase 1: Generation (Input: $input_base, Budget: ${mem_gen}MB, Workers: $workers)"
            rm -rf "$run_dir"/*
            time -p ./ff/bin/run_gen_ff "$input" "$mem_gen" "$run_dir/run_" "$workers"
            sync

            echo ">>> [FF] Phase 2: Merge (K=$K, Workers=$workers)"
            time -p ./ff/bin/merge_ff "$run_dir/run_" "$final_out" "$K" "$MEM_MERGE_GB" "$workers"

            ./tools/verify_sorted "$final_out"
        done
    done
done
