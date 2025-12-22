#seq_bench_35g.slurm
#!/bin/bash
#SBATCH --job-name=seq_35g
#SBATCH --output=seq_35g_%j.out
#SBATCH --exclusive
#SBATCH --time=00:30:00

cd $HOME/project || exit 1

INPUT=inputs/input_35gb.dat
RUN_DIR=seq/results_35g
FINAL_OUT=seq/results_35g/final.dat
MEM_GEN=512  # 128MB chunks -> ~280 runs

mkdir -p $RUN_DIR

echo ">>> [Seq] Phase 1: Gen (Budget: ${MEM_GEN}MB)"
rm -rf $RUN_DIR/*
time -p ./seq/bin/run_gen_seq $INPUT $MEM_GEN $RUN_DIR/run_

NUM_RUNS=$(ls -1 $RUN_DIR/run_*.dat | wc -l)
sync

echo ">>> [Seq] Phase 2: Merge ($NUM_RUNS runs)"
time -p ./seq/bin/merge_seq $RUN_DIR/run_ $NUM_RUNS $FINAL_OUT

./tools/verify_sorted $FINAL_OUT