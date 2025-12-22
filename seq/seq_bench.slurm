#seq_bench.slurm
#!/bin/bash
#SBATCH --job-name=seq_bench
#SBATCH --output=seq_bench_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

cd $HOME/project || exit 1

# === CONFIG ===
INPUT=inputs/input_10gb.dat
RUN_DIR=seq/results_bench
FINAL_OUT=seq/results_bench/final.dat
MEM_GEN=32  # 32MB chunks -> ~320 runs

mkdir -p $RUN_DIR

echo ">>> [Seq] Phase 1: Generation (Budget: ${MEM_GEN}MB)"
rm -rf $RUN_DIR/*
time -p ./seq/bin/run_gen_seq $INPUT $MEM_GEN $RUN_DIR/run_

NUM_RUNS=$(ls -1 $RUN_DIR/run_*.dat | wc -l)
echo "    Generated $NUM_RUNS runs"
sync

echo ">>> [Seq] Phase 2: Merge ($NUM_RUNS runs)"
# Seq merges all runs in one pass (K = NUM_RUNS)
time -p ./seq/bin/merge_seq $RUN_DIR/run_ $NUM_RUNS $FINAL_OUT

./tools/verify_sorted $FINAL_OUT