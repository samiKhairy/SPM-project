#!/bin/bash
#SBATCH --job-name=omp_2gb_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --output=omp_2gb_%j.out
#SBATCH --time=00:20:00
#SBATCH --exclusive

set -euo pipefail

cd $HOME/projectcopy || exit 1

# Affinity and Parallelism Settings
export OMP_PROC_BIND=close
export OMP_PLACES=cores
export OMP_NUM_THREADS=4

# Configuration matching Sequential for fair comparison
INPUT="inputs/input_2gb_p256.dat"
PAYLOAD_MAX=256
MEM_GEN_MB=1024     # Constrained to force multiple runs
MEM_MERGE_GB=1     # Constrained to force small stream buffers
K=8              # take the k twice the threads
RUN_ROOT="omp/results_2gb_test"

mkdir -p "$RUN_ROOT"
input_base=$(basename "$INPUT" .dat)
run_dir="$RUN_ROOT/$input_base"
final_out="$run_dir/final_omp.dat"

mkdir -p "$run_dir"

echo ">>> [OpenMP] Phase 1: Generating Runs (Threads: $OMP_NUM_THREADS, K = ${K} ,Budget: ${MEM_GEN_MB}MB, Inputfile : ${INPUT})"
rm -rf "$run_dir"/*
# Using parallel generator to speed up sorting
time -p ./omp/bin/run_gen_omp "$INPUT" "$MEM_GEN_MB" "$run_dir/run_"
sync

echo ">>> [OpenMP] Phase 2: Merging (K=$K, Threads=$OMP_NUM_THREADS, RAM: ${MEM_MERGE_GB}GB)"
# Parallel merge using the K-way task-based approach
time -p ./omp/bin/merge_omp "$run_dir/run_" "$final_out" "$K" "$MEM_MERGE_GB" "$PAYLOAD_MAX"

# Verification
./tools/bin/verify_sorted "$final_out"