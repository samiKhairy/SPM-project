#!/bin/bash
#SBATCH --job-name=manual_run
#SBATCH --output=results/%x_%j.log
#SBATCH --partition=normal
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=16   
#SBATCH --cpus-per-task=1       
#SBATCH --time=00:30:00
#SBATCH --mem=0
#SBATCH --exclusive

set -euo pipefail

THREADS=8


WORKLOAD="${1:-cpu}"   # cpu or io
CORES_PER_NODE=16      # Your machine has 16 physical cores

if [[ "$WORKLOAD" == "cpu" ]]; then
  RECORDS=30000000; PAYLOAD=16; MEM=64
elif [[ "$WORKLOAD" == "io" ]]; then
  RECORDS=30000000; PAYLOAD=128; MEM=256
else
  echo "Usage: sbatch script.sh [cpu|io]"; exit 1
fi

CUTOFF=32768
DATA_FILE="${SLURM_SUBMIT_DIR}/../datasets/data_N${RECORDS}_P${PAYLOAD}.bin"

cd "${SLURM_SUBMIT_DIR}"
mkdir -p "results/bin"

echo "=== MANUAL RUN: ${WORKLOAD} with THREADS=${THREADS} ==="


mpicxx -O3 -DNDEBUG -std=c++20 -fopenmp \
  "mergesort_mpi_omp.cpp" -o "bin/mergesort_mpi_omp"


for NODES in 1 2 4 8; do
  if (( NODES > SLURM_JOB_NUM_NODES )); then continue; fi

  # --- AUTOMATIC CALCULATION ---
  # We have 16 cores. We divide them by your requested threads.
  # Example: If THREADS=4, then RANKS_PER_NODE = 16 / 4 = 4.
  RANKS_PER_NODE=$(( CORES_PER_NODE / THREADS ))
  
  # Total Ranks = Nodes * Ranks_Per_Node
  TOTAL_RANKS=$(( NODES * RANKS_PER_NODE ))

  OUT_FILE="results/bin/out_${WORKLOAD}_N${NODES}_T${THREADS}.bin"

  echo
  echo "--- RUN: Nodes=${NODES} | Ranks/Node=${RANKS_PER_NODE} | Threads=${THREADS} ---"

  export OMP_NUM_THREADS="${THREADS}"

  srun --mpi=pmix \
       -N "${NODES}" \
       -n "${TOTAL_RANKS}" \
       -c "${THREADS}" \
       --cpu-bind=cores \
       "bin/mergesort_mpi_omp" \
       "${DATA_FILE}" "${RECORDS}" "${PAYLOAD}" "${MEM}" "${THREADS}" "${CUTOFF}" "${OUT_FILE}"

done

echo "DONE."