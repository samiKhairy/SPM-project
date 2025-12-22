#omp_bench_35g.slurm
#!/bin/bash
#SBATCH --job-name=omp_35g
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --output=omp_35g_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

cd $HOME/project || exit 1
export OMP_PROC_BIND=close
export OMP_PLACES=cores

INPUT=inputs/input_35gb.dat
RUN_DIR=omp/results_35g
FINAL_OUT=omp/results_35g/final.dat
MEM_GEN=512
MEM_MERGE_GB=20  # Safe budget
K=32

mkdir -p $RUN_DIR

# === Phase 1 ===
echo ">>> [OMP] Phase 1: Gen (Budget: ${MEM_GEN}MB)"
rm -rf $RUN_DIR/*
export OMP_NUM_THREADS=8
time -p ./omp/run_gen_omp $INPUT $MEM_GEN $RUN_DIR/run_
sync

# === Phase 2 ===
echo ">>> [OMP] Phase 2: Merge (K=$K, Threads=8)"
# Note: Ensure your merge_omp.cpp has the SAFETY FIX applied!
time -p ./omp/merge_omp $RUN_DIR/run_ $FINAL_OUT $K $MEM_MERGE_GB

./tools/verify_sorted $FINAL_OUT