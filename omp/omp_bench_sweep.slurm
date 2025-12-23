#!/bin/bash
#SBATCH --job-name=omp_bench_sweep
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=omp_bench_sweep_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

set -euo pipefail

cd $HOME/projectcopy || exit 1
export OMP_PROC_BIND=true
export OMP_PLACES=cores

# === CONFIG ===
INPUTS=(
  inputs/input_10gb_p256.dat
  inputs/input_10gb_p8192.dat
)
MEM_GEN_LIST=(1024 512) # MB per run: 1GB primary, 512MB backup
RUN_ROOT=omp/results_bench_sweep
MEM_MERGE_LIST=(8 16 32)
K_LIST=(4 16 64)
THREADS_LIST=(1 2 4 8 16)

mkdir -p "$RUN_ROOT"

generate_runs() {
    local input="$1"
    local mem_gen="$2"
    local run_dir="$3"
    local input_base
    input_base=$(basename "$input" .dat)

    echo ">>> [OMP] Phase 1: Generation (Input: $input_base, Budget: ${mem_gen}MB, Threads: $OMP_NUM_THREADS)"
    rm -rf "$run_dir"/*
    time -p ./omp/bin/run_gen_omp "$input" "$mem_gen" "$run_dir/run_"
    sync
}

for input in "${INPUTS[@]}"; do
    input_base=$(basename "$input" .dat)
    for mem_gen in "${MEM_GEN_LIST[@]}"; do
        for threads in "${THREADS_LIST[@]}"; do
            export OMP_NUM_THREADS="$threads"
            run_dir="$RUN_ROOT/$input_base/mem_${mem_gen}mb/t${threads}"
            final_out="$run_dir/final.dat"

            mkdir -p "$run_dir"
            generate_runs "$input" "$mem_gen" "$run_dir"

            for mem_merge in "${MEM_MERGE_LIST[@]}"; do
                for k_val in "${K_LIST[@]}"; do
                    final_out="$run_dir/final_k${k_val}_mem${mem_merge}gb.dat"
                    echo ">>> [OMP] Phase 2: Merge (K=$k_val, Mem=${mem_merge}GB, Threads=$OMP_NUM_THREADS)"
                    time -p ./omp/bin/merge_omp "$run_dir/run_" "$final_out" "$k_val" "$mem_merge"
                    ./tools/bin/verify_sorted "$final_out"
                done
            done
        done
    done
done
