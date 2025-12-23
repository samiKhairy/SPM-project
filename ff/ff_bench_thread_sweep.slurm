#!/bin/bash
#SBATCH --job-name=ff_thread_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=ff_thread_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/projectcopy || exit 1

# === CONFIG ===
INPUTS=(
  inputs/input_10gb_p256.dat
  inputs/input_10gb_p8192.dat
)
MEM_GEN_MB=1024
MEM_MERGE_GB=16
K=16
WORKERS_LIST=(1 2 4 8 16)
RUN_ROOT=ff/results_thread_sweep

mkdir -p "$RUN_ROOT"

for input in "${INPUTS[@]}"; do
    if [[ ! -f "$input" ]]; then
        echo ">>> [FF] Skipping missing input: $input"
        continue
    fi
    input_base=$(basename "$input" .dat)
    for workers in "${WORKERS_LIST[@]}"; do
        run_dir="$RUN_ROOT/$input_base/w${workers}"
        final_out="$run_dir/final_k${K}_mem${MEM_MERGE_GB}gb.dat"

        mkdir -p "$run_dir"
        echo ">>> [FF] Phase 1: Generation (Input: $input_base, Budget: ${MEM_GEN_MB}MB, Workers: $workers)"
        rm -rf "$run_dir"/*
        time -p ./ff/bin/run_gen_ff "$input" "$MEM_GEN_MB" "$run_dir/run_" "$workers"
        sync

        echo ">>> [FF] Phase 2: Merge (K=$K, Mem=${MEM_MERGE_GB}GB, Workers=$workers)"
        time -p ./ff/bin/merge_ff "$run_dir/run_" "$final_out" "$K" "$MEM_MERGE_GB" "$workers"
        ./tools/bin/verify_sorted "$final_out"
    done
done
