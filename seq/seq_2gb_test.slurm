#!/bin/bash
#SBATCH --job-name=seq_2gb_test
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=seq_2gb_%j.out
#SBATCH --time=00:20:00
#SBATCH --exclusive

set -euo pipefail

# Navigation to the project directory
cd $HOME/projectcopy || exit 1

# Configuration for 2GB Out-of-Core Test
INPUT="inputs/input_2gb_p256.dat"
PAYLOAD_MAX=256
MEM_GEN_MB=512      # Constrained to force multiple runs (~4-6 runs for 2GB)
MEM_MERGE_GB=1      # Constrained to test buffered merge performance
RUN_ROOT="seq/results_2gb_test"

mkdir -p "$RUN_ROOT"
input_base=$(basename "$INPUT" .dat)
run_dir="$RUN_ROOT/$input_base"
final_out="$run_dir/final_seq.dat"

mkdir -p "$run_dir"

echo ">>> [Sequential] Phase 1: Generating Runs (Budget: ${MEM_GEN_MB}MB)"
rm -rf "$run_dir"/*
# Generating runs with sequential generator
time -p ./seq/bin/run_gen_seq "$INPUT" "$MEM_GEN_MB" "$run_dir/run_"
sync

# Count runs
num_runs=$(ls -1 "$run_dir"/run_*.dat | wc -l)

echo ">>> [Sequential] Phase 2: Merging $num_runs runs (RAM: ${MEM_MERGE_GB}GB)"
# Sequential merge handles all runs in one pass
time -p ./seq/bin/merge_seq "$run_dir/run_" "$num_runs" "$final_out" "$MEM_MERGE_GB" "$PAYLOAD_MAX"

# Verification
./tools/bin/verify_sorted "$final_out"