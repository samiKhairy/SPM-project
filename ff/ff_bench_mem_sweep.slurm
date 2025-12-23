#!/bin/bash
#SBATCH --job-name=ff_mem_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=ff_mem_sweep_%j.out
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
MEM_MERGE_LIST=(8 16 32)
K=16
WORKERS=16
RUN_ROOT=ff/results_mem_sweep

mkdir -p "$RUN_ROOT"

for input in "${INPUTS[@]}"; do
    if [[ ! -f "$input" ]]; then
        echo ">>> [FF] Skipping missing input: $input"
        continue
    fi
    input_base=$(basename "$input" .dat)
    run_dir="$RUN_ROOT/$input_base/w${WORKERS}"
    final_out="$run_dir/final_k${K}_mem${MEM_MERGE_LIST[0]}gb.dat"

    mkdir -p "$run_dir"
    echo ">>> [FF] Phase 1: Generation (Input: $input_base, Budget: ${MEM_GEN_MB}MB, Workers: $WORKERS)"
    rm -rf "$run_dir"/*
    time -p ./ff/bin/run_gen_ff "$input" "$MEM_GEN_MB" "$run_dir/run_" "$WORKERS"
    sync

    for mem_merge in "${MEM_MERGE_LIST[@]}"; do
        final_out="$run_dir/final_k${K}_mem${mem_merge}gb.dat"
        echo ">>> [FF] Phase 2: Merge (K=$K, Mem=${mem_merge}GB, Workers=$WORKERS)"
        time -p ./ff/bin/merge_ff "$run_dir/run_" "$final_out" "$K" "$mem_merge" "$WORKERS"
        ./tools/bin/verify_sorted "$final_out"
    done
done
