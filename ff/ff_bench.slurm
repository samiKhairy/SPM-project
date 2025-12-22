ff_bench.slurm
#!/bin/bash
#SBATCH --job-name=ff_bench
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --output=ff_bench_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

cd $HOME/project || exit 1

# === CONFIG ===
INPUT=inputs/input_10gb.dat
RUN_DIR=ff/results_bench
FINAL_OUT=ff/results_bench/final.dat
MEM_GEN=32         # 32MB per worker task
MEM_MERGE_GB=24    # 24GB Total for Merge
K=32
WORKERS=8          # Use full parallelism

mkdir -p $RUN_DIR

echo ">>> [FF] Phase 1: Generation (Budget: ${MEM_GEN}MB, Workers: $WORKERS)"
rm -rf $RUN_DIR/*
# FF Pipeline: Reader -> Farm(8) -> Writer
time -p ./ff/bin/run_gen_ff $INPUT $MEM_GEN $RUN_DIR/run_ $WORKERS

sync

echo ">>> [FF] Phase 2: Merge (K=$K, Workers=$WORKERS)"
# ./merge_ff <prefix> <final_out> <K> <TOTAL_MEM_GB> <WORKERS>
time -p ./ff/bin/merge_ff $RUN_DIR/run_ $FINAL_OUT $K $MEM_MERGE_GB $WORKERS

./tools/verify_sorted $FINAL_OUT