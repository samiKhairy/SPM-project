#!/bin/bash
#SBATCH --job-name=omp_small_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=omp_test_%j.out
#SBATCH --time=00:30:00
#SBATCH --exclusive

set -euo pipefail

cd $HOME/projectcopy || exit 1

# Performance environment variables
export OMP_PROC_BIND=close
export OMP_PLACES=cores
export OMP_NUM_THREADS=16

# Configuration matching the sequential test for fairness
INPUT="inputs/input_10gb_p256.dat"
MEM_GEN_MB=1024
MEM_MERGE_GB=16
K=16
RUN_ROOT="omp/results_10gb_256"

mkdir -p "$RUN_ROOT"
input_base=$(basename "$INPUT" .dat)
run_dir="$RUN_ROOT/$input_base"
final_out="$run_dir/final_omp.dat"

mkdir -p "$run_dir"

echo ">>> [OpenMP] Phase 1: Generating Runs (Threads: $OMP_NUM_THREADS, Input: $INPUT)"
rm -rf "$run_dir"/*
./omp/bin/run_gen_omp "$INPUT" "$MEM_GEN_MB" "$run_dir/run_"
sync

echo ">>> [OpenMP] Phase 2: Merging (K=$K, Threads=$OMP_NUM_THREADS)"
# OpenMP merge uses task-parallelism and a branching factor K
time -p ./omp/bin/merge_omp "$run_dir/run_" "$final_out" "$K" "$MEM_MERGE_GB"

# Verification
./tools/bin/verify_sorted "$final_out"