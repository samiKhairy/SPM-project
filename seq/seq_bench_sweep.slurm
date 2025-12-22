#!/bin/bash
#SBATCH --job-name=seq_bench_sweep
#SBATCH --output=seq_bench_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/projectcopy || exit 1

# === CONFIG ===
INPUTS=(
  inputs/input_10gb_p256.dat
  inputs/input_10gb_p8192.dat
  inputs/input_20gb_p256.dat
  inputs/input_20gb_p8192.dat
)
MEM_GEN_LIST=(1024 512) # MB per run: 1GB primary, 512MB backup
RUN_ROOT=seq/results_bench_sweep

mkdir -p "$RUN_ROOT"

for input in "${INPUTS[@]}"; do
  input_base=$(basename "$input" .dat)
  for mem_gen in "${MEM_GEN_LIST[@]}"; do
    run_dir="$RUN_ROOT/$input_base/mem_${mem_gen}mb"
    final_out="$run_dir/final.dat"

    mkdir -p "$run_dir"

    echo ">>> [Seq] Phase 1: Generation (Input: $input_base, Budget: ${mem_gen}MB)"
    rm -rf "$run_dir"/*
    time -p ./seq/bin/run_gen_seq "$input" "$mem_gen" "$run_dir/run_"

    num_runs=$(ls -1 "$run_dir"/run_*.dat | wc -l)
    echo "    Generated $num_runs runs"
    sync

    echo ">>> [Seq] Phase 2: Merge ($num_runs runs)"
    # Seq merges all runs in one pass (K = num_runs)
    time -p ./seq/bin/merge_seq "$run_dir/run_" "$num_runs" "$final_out"

    ./tools/bin/verify_sorted "$final_out"
  done
done