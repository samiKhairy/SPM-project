#!/bin/bash
#SBATCH --job-name=seq_small_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=seq_test_%j.out
#SBATCH --time=00:30:00
#SBATCH --exclusive

set -euo pipefail

# Navigation to the project directory
cd $HOME/projectcopy || exit 1

# Configuration for a small test
INPUT="inputs/input_10gb_p256.dat"
MEM_GEN_MB=1024
MEM_MERGE_GB=16
RUN_ROOT="seq/results_small_test"

mkdir -p "$RUN_ROOT"
input_base=$(basename "$INPUT" .dat)
run_dir="$RUN_ROOT/$input_base"
final_out="$run_dir/final_seq.dat"

mkdir -p "$run_dir"

echo ">>> [Sequential] Phase 1: Generating Runs"
# Clean previous runs and generate new ones
rm -rf "$run_dir"/*
./seq/bin/run_gen_seq "$INPUT" "$MEM_GEN_MB" "$run_dir/run_"
sync

# Count generated runs for the merge phase
num_runs=$(ls -1 "$run_dir"/run_*.dat | wc -l)

echo ">>> [Sequential] Phase 2: Merging $num_runs runs"
# Sequential merge uses all runs in a single pass
time -p ./seq/bin/merge_seq "$run_dir/run_" "$num_runs" "$final_out" "$MEM_MERGE_GB"

# Verification
./tools/bin/verify_sorted "$final_out"