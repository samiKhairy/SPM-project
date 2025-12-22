#omp_bench.slurm
#!/bin/bash
#SBATCH --job-name=omp_bench
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --output=omp_bench_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

cd $HOME/project || exit 1
export OMP_PROC_BIND=close
export OMP_PLACES=cores

# === CONFIG ===
INPUT=inputs/input_10gb.dat
RUN_DIR=omp/results_bench
FINAL_OUT=omp/results_bench/final.dat
MEM_GEN=32         # 32MB -> ~320 runs
MEM_MERGE_GB=24    # Give 24GB total RAM to the merge phase
K=32               # 32-way merge

mkdir -p $RUN_DIR

# === HELPER ===
generate_runs() {
    echo ">>> [OMP] Phase 1: Generation (Budget: ${MEM_GEN}MB, Threads: $OMP_NUM_THREADS)"
    rm -rf $RUN_DIR/*
    # Run generation uses OMP task parallelism
    time -p ./omp/run_gen_omp $INPUT $MEM_GEN $RUN_DIR/run_
    sync
}

# === EXPERIMENT: 8 Threads, K=32 ===
export OMP_NUM_THREADS=8

generate_runs

echo ">>> [OMP] Phase 2: Merge (K=$K, Threads=$OMP_NUM_THREADS)"
# ./merge_omp <prefix> <final_out> <K> <TOTAL_MEM_GB>
time -p ./omp/merge_omp $RUN_DIR/run_ $FINAL_OUT $K $MEM_MERGE_GB

./tools/verify_sorted $FINAL_OUT