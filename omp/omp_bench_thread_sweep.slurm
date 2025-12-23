#!/bin/bash
#SBATCH --job-name=omp_thread_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=omp_thread_sweep_%j.out
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
MEM_MERGE_GB=16
K=16
THREADS_LIST=(1 2 4 8 16)
RUN_ROOT=omp/results_thread_sweep

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

for input in "${INPUTS[@]}"; do
    if [[ ! -f "$input" ]]; then
        echo ">>> [OMP] Skipping missing input: $input"
        continue
    fi
    input_base=$(basename "$input" .dat)
    for threads in "${THREADS_LIST[@]}"; do
        export OMP_NUM_THREADS="$threads"
        run_dir="$RUN_ROOT/$input_base/t${threads}"
        mkdir -p "$run_dir"
        generate_runs "$input" "$run_dir"

        final_out="$run_dir/final_k${K}_mem${MEM_MERGE_GB}gb.dat"
        echo ">>> [OMP] Phase 2: Merge (K=$K, Mem=${MEM_MERGE_GB}GB, Threads=$OMP_NUM_THREADS)"
        time -p ./omp/bin/merge_omp "$run_dir/run_" "$final_out" "$K" "$MEM_MERGE_GB"
        ./tools/bin/verify_sorted "$final_out"
    done
done
