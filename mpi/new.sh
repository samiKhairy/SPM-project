#!/bin/bash
#SBATCH --job-name=mpi_omp
#SBATCH --output=results/%x_%j.log
#SBATCH --partition=normal
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --time=00:30:00
#SBATCH --mem=0

set -euo pipefail

cd "${SLURM_SUBMIT_DIR}"
SUBMIT_DIR="${SLURM_SUBMIT_DIR}"

WORKLOAD="${1:-cpu}"   # cpu or io

# ---- workload params (match your datasets) ----
if [[ "$WORKLOAD" == "cpu" ]]; then
  RECORDS=30000000
  PAYLOAD=16
  MEM=64
elif [[ "$WORKLOAD" == "io" ]]; then
  RECORDS=30000000
  PAYLOAD=128	
  MEM=256
else
  echo "Usage: sbatch new.sh [cpu|io]"
  exit 1
fi

CUTOFF=32768
THREADS="${SLURM_CPUS_PER_TASK}"

DATA_FILE="${SUBMIT_DIR}/../datasets/data_N${RECORDS}_P${PAYLOAD}.bin"

mkdir -p "${SUBMIT_DIR}/bin" "${SUBMIT_DIR}/results" "${SUBMIT_DIR}/results/bin"

echo "=== [MPI+OMP SWEEP] ($WORKLOAD) ==="
echo "SubmitDir: ${SUBMIT_DIR}"
echo "Dataset:   ${DATA_FILE}"
echo "Records:   ${RECORDS}"
echo "Payload:   ${PAYLOAD}"
echo "MemMB/r:   ${MEM}"
echo "Threads:   ${THREADS}"
echo "Cutoff:    ${CUTOFF}"
echo "Nodes max: ${SLURM_JOB_NUM_NODES}"
echo "-----------------------------------"

if [[ ! -f "${DATA_FILE}" ]]; then
  echo "FATAL: dataset not found: ${DATA_FILE}"
  ls -l "${SUBMIT_DIR}/../datasets" || true
  exit 2
fi

echo "[Build]"
mpicxx -O3 -DNDEBUG -std=c++20 -fopenmp \
  "${SUBMIT_DIR}/mergesort_mpi_omp.cpp" \
  -o "${SUBMIT_DIR}/bin/mergesort_mpi_omp"

export OMP_NUM_THREADS="${THREADS}"
export OMP_PROC_BIND=true
export OMP_PLACES=cores


# Strong scaling sweep: 1,2,4,8 nodes (one rank/node)
for NODES in 1 2 4 8; do
  if (( NODES > SLURM_JOB_NUM_NODES )); then
    echo "[Skip] NODES=${NODES} > allocated=${SLURM_JOB_NUM_NODES}"
    continue
  fi

  RANKS="${NODES}"
  
	
  OUT_FILE="${SUBMIT_DIR}/results/bin/out_${WORKLOAD}_nodes${NODES}.bin"

  echo
  echo "----- RUN: nodes=${NODES} ranks=${RANKS} threads/rank=${THREADS} -----"
  echo "OUT: ${OUT_FILE}"

  args=(
    "${DATA_FILE}" "${RECORDS}" "${PAYLOAD}" "${MEM}" "${THREADS}" "${CUTOFF}" "${OUT_FILE}"
  )

  srun --mpi=pmix -N "${NODES}" -n "${RANKS}" \
    "${SUBMIT_DIR}/bin/mergesort_mpi_omp" "${args[@]}"
done

echo "DONE."
