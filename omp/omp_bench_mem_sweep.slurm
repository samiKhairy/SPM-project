#!/bin/bash
#SBATCH --job-name=omp_mem_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=omp_mem_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/projectcopy || exit 1
export OMP_PROC_BIND=close
export OMP_PLACES=cores

# === CONFIG ===
INPUTS=(
  inputs/input_10gb_p256.dat
  inputs/input_10gb_p8192.dat
)
MEM_GEN_MB=1024
MEM_MERGE_LIST=(8 16 32)
K=16
THREADS=16
RUN_ROOT=omp/results_mem_sweep

mkdir -p "$RUN_ROOT"

generate_runs() {
    local input="$1"
    local run_dir="$2"
    local input_base
    input_base=$(basename "$input" .dat)

    echo ">>> [OMP] Phase 1: Generation (Input: $input_base, Budget: ${MEM_GEN_MB}MB, Threads: $OMP_NUM_THREADS)"
    rm -rf "$run_dir"/*
    time -p ./omp/bin/run_gen_omp "$input" "$MEM_GEN_MB" "$run_dir/run_"
    sync
}

export OMP_NUM_THREADS="$THREADS"

for input in "${INPUTS[@]}"; do
    if [[ ! -f "$input" ]]; then
        echo ">>> [OMP] Skipping missing input: $input"
        continue
    fi
    input_base=$(basename "$input" .dat)
    run_dir="$RUN_ROOT/$input_base/t${THREADS}"
    mkdir -p "$run_dir"
    generate_runs "$input" "$run_dir"

    for mem_merge in "${MEM_MERGE_LIST[@]}"; do
        final_out="$run_dir/final_k${K}_mem${mem_merge}gb.dat"
        echo ">>> [OMP] Phase 2: Merge (K=$K, Mem=${mem_merge}GB, Threads=$OMP_NUM_THREADS)"
        time -p ./omp/bin/merge_omp "$run_dir/run_" "$final_out" "$K" "$mem_merge"
        ./tools/bin/verify_sorted "$final_out"
    done
done
