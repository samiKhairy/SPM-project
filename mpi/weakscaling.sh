#!/bin/bash
#SBATCH --job-name=weak_scaling
#SBATCH --output=results/%x_%j.log
#SBATCH --partition=normal
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=16    
#SBATCH --cpus-per-task=1       
#SBATCH --time=00:30:00
#SBATCH --mem=0
#SBATCH --exclusive

set -euo pipefail

# --- CONFIGURATION ---
THREADS=16             # Best config (Hybrid) for weak scaling
WORKLOAD="${1:-cpu}"   # cpu or io
CORES_PER_NODE=16      

# 1. SET BASE RECORDS (Records PER NODE)
# We start with 10M per node so 8 nodes = 80M (manageable size)
if [[ "$WORKLOAD" == "cpu" ]]; then
  BASE_RECORDS=10000000; PAYLOAD=16; MEM=64
elif [[ "$WORKLOAD" == "io" ]]; then
  BASE_RECORDS=10000000; PAYLOAD=128; MEM=256
else
  echo "Usage: sbatch weak_new.sh [cpu|io]"; exit 1
fi

CUTOFF=32768
cd "${SLURM_SUBMIT_DIR}"
mkdir -p "results/bin"

echo "=== WEAK SCALING: ${WORKLOAD} (Base=${BASE_RECORDS}/node) ==="

# Compile
mpicxx -O3 -DNDEBUG -std=c++20 -fopenmp \
  "mergesort_mpi_omp.cpp" -o "bin/mergesort_mpi_omp"

# --- THE LOOP: Scale Nodes 1 to 8 ---
for NODES in 1 2 4 8; do
  if (( NODES > SLURM_JOB_NUM_NODES )); then continue; fi

  # Standard Math from your script
  RANKS_PER_NODE=$(( CORES_PER_NODE / THREADS ))
  TOTAL_RANKS=$(( NODES * RANKS_PER_NODE ))

  # ---  CALCULATE NEW TOTAL RECORDS ---
  # Weak Scaling = Base Records * Number of Nodes
  CURRENT_RECORDS=$(( BASE_RECORDS * NODES ))
  
  # --- SELECT THE CORRECT FILE ---
  # The file name changes every loop (10M, 20M, 40M, 80M...)
  DATA_FILE="${SLURM_SUBMIT_DIR}/../datasets/data_N${CURRENT_RECORDS}_P${PAYLOAD}.bin"

  # Check if file exists (Critical!)
  if [[ ! -f "${DATA_FILE}" ]]; then
     echo "SKIP: Dataset ${DATA_FILE} missing. Run ./gen_data first."
     continue
  fi

  OUT_FILE="results/bin/out_weak_${WORKLOAD}_N${NODES}.bin"

  echo
  echo "--- WEAK RUN: Nodes=${NODES} | Total Records=${CURRENT_RECORDS} ---"

  export OMP_NUM_THREADS="${THREADS}"

  # Pass CURRENT_RECORDS instead of static RECORDS
  srun --mpi=pmix \
       -N "${NODES}" \
       -n "${TOTAL_RANKS}" \
       -c "${THREADS}" \
       --cpu-bind=cores \
       "bin/mergesort_mpi_omp" \
       "${DATA_FILE}" "${CURRENT_RECORDS}" "${PAYLOAD}" "${MEM}" "${THREADS}" "${CUTOFF}" "${OUT_FILE}"

done

echo "DONE."