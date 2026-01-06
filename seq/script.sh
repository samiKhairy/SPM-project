#!/bin/bash
#SBATCH --job-name=bench_seq
#SBATCH --output=/dev/null
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00

set -euo pipefail
SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$(pwd)}"
WORKLOAD="${1:-cpu}"

source "$SUBMIT_DIR/../scripts/common.sh"
workload_params "$WORKLOAD"
setup_logging "$SUBMIT_DIR/results" "seq_${WORKLOAD}"

DATA_FILE="$SUBMIT_DIR/../datasets/data_N${RECORDS}_P${PAYLOAD}.bin"

echo "=== [SEQ] ($WORKLOAD) ==="
echo "JobID: $JOBID  Host: $(hostname)  Date: $(date)"
echo "N=$RECORDS PAYLOAD=$PAYLOAD MEM=$MEM"
echo "Dataset: $DATA_FILE"
echo "-------------------------"

echo "[Setup] Compiling..."
mkdir -p "$SUBMIT_DIR/bin"
g++ -O3 -DNDEBUG -std=c++20 "$SUBMIT_DIR/mergesort_seq.cpp" -o "$SUBMIT_DIR/bin/mergesort_seq"
build_datagen "$SUBMIT_DIR"
ensure_dataset "$SUBMIT_DIR" "$DATA_FILE"

SCRATCH="/scratch/$USER/seq_${WORKLOAD}_job${JOBID}"
mk_scratch_and_trap "$SCRATCH"

echo "[Run] Starting..."
srun "$SUBMIT_DIR/bin/mergesort_seq" "$DATA_FILE" "$RECORDS" "$PAYLOAD" "$MEM"
echo "DONE."
