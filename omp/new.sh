#!/bin/bash
#SBATCH --job-name=bench_omp
#SBATCH --output=/dev/null
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --time=00:30:00

set -euo pipefail
SUBMIT_DIR="${SLURM_SUBMIT_DIR:-$(pwd)}"
WORKLOAD="${1:-cpu}"

source "$SUBMIT_DIR/../scripts/common.sh"
workload_params "$WORKLOAD"

# for Thread Scaling (uncomment below)
#THREAD_LIST=(1 2 4 8 16)

# for N scaling ( uncomment below )
THREAD_LIST=(16)

CUTOFF=32768

setup_logging "$SUBMIT_DIR/results" "omp_${WORKLOAD}_cut${CUTOFF}"

DATA_FILE="$SUBMIT_DIR/../datasets/data_N${RECORDS}_P${PAYLOAD}.bin"

echo "=== [OMP] ($WORKLOAD) ==="
echo "JobID: $JOBID  Host: $(hostname)  Date: $(date)"
echo "N=$RECORDS PAYLOAD=$PAYLOAD MEM=$MEM CUTOFF=$CUTOFF"
echo "Threads: ${THREAD_LIST[*]}"
echo "Dataset: $DATA_FILE"
echo "-------------------------"

echo "[Setup] Compiling..."
mkdir -p "$SUBMIT_DIR/bin"
g++ -O3 -DNDEBUG -std=c++20 -fopenmp "$SUBMIT_DIR/mergesort_omp.cpp" -o "$SUBMIT_DIR/bin/mergesort_omp"
build_datagen "$SUBMIT_DIR"
ensure_dataset "$SUBMIT_DIR" "$DATA_FILE"

SCRATCH="/scratch/$USER/omp_${WORKLOAD}_job${JOBID}"
mk_scratch_and_trap "$SCRATCH"

echo "[Run] Starting..."
for T in "${THREAD_LIST[@]}"; do

  rm -f run_*.bin output.bin || true	
  echo "--- OMP_NUM_THREADS=$T ---"
  sync
  sleep 2
  export OMP_NUM_THREADS="$T"
  srun "$SUBMIT_DIR/bin/mergesort_omp" "$DATA_FILE" "$RECORDS" "$PAYLOAD" "$MEM" "$T" "$CUTOFF"
  sync
  sleep 2
done
echo "DONE."
