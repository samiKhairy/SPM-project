#ff_bench_35g.slurm
#!/bin/bash
#SBATCH --job-name=ff_35g
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --output=ff_35g_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

cd $HOME/project || exit 1

INPUT=inputs/input_35gb.dat
RUN_DIR=ff/results_35g
FINAL_OUT=ff/results_35g/final.dat
MEM_GEN=512
MEM_MERGE_GB=20
K=32
WORKERS=8

mkdir -p $RUN_DIR

# === Phase 1 ===
echo ">>> [FF] Phase 1: Gen (Budget: ${MEM_GEN}MB)"
rm -rf $RUN_DIR/*
# Ensure run_generation_ff.cpp has the correct MB -> Byte conversion!
time -p ./ff/bin/run_gen_ff $INPUT $MEM_GEN $RUN_DIR/run_ $WORKERS
sync

# === Phase 2 ===
echo ">>> [FF] Phase 2: Merge (K=$K)"
time -p ./ff/bin/merge_ff $RUN_DIR/run_ $FINAL_OUT $K $MEM_MERGE_GB $WORKERS

./tools/verify_sorted $FINAL_OUT