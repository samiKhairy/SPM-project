#!/bin/bash
#SBATCH --job-name=bench_ff
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
setup_logging "$SUBMIT_DIR/results" "ff_${WORKLOAD}"

# for Thread Scaling (uncomment below)
#WORKERS_LIST=(1 2 4 8 16)

# for N scaling ( uncomment below )
WORKERS_LIST=(16)


DATA_FILE="$SUBMIT_DIR/../datasets/data_N${RECORDS}_P${PAYLOAD}.bin"

echo "=== [FF] ($WORKLOAD) ==="
echo "JobID: $JOBID  Host: $(hostname)  Date: $(date)"
echo "N=$RECORDS PAYLOAD=$PAYLOAD MEM=$MEM"
echo "Workers: ${WORKERS_LIST[*]}"
echo "Dataset: $DATA_FILE"
echo "-------------------------"

echo "[Setup] Compiling..."
mkdir -p "$SUBMIT_DIR/bin"
g++ -O3 -DNDEBUG -std=c++20 -pthread -I"$HOME/fastflow" -DNO_DEFAULT_MAPPING \
  "$SUBMIT_DIR/mergesort_ff.cpp" -o "$SUBMIT_DIR/bin/mergesort_ff"

build_datagen "$SUBMIT_DIR"
ensure_dataset "$SUBMIT_DIR" "$DATA_FILE"

SCRATCH="/scratch/$USER/ff_${WORKLOAD}_job${JOBID}"
mk_scratch_and_trap "$SCRATCH"

echo "[Run] Starting..."
for W in "${WORKERS_LIST[@]}"; do
  rm -f run_*.bin output.bin || true
  echo "--- FF workers=$W ---"
  sync
  sleep 2	
  srun "$SUBMIT_DIR/bin/mergesort_ff" "$DATA_FILE" "$RECORDS" "$PAYLOAD" "$MEM" "$W"
  sync
  sleep 2
	
done

echo "DONE."
