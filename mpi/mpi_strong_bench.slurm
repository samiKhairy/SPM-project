#!/bin/bash
#SBATCH --job-name=mpi_strong_bench
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:30:00
#SBATCH --output=mpi/mpi_strong_%j.out
#SBATCH --exclusive

set -euo pipefail

# --- 1. SET UP ENVIRONMENT (OpenMPI 5) ---
export MPI_ROOT=/opt/ohpc/pub/mpi/openmpi5-gnu12/5.0.3
export UCX_ROOT=/opt/ohpc/pub/mpi/ucx-ohpc/1.14.0

export PATH=$MPI_ROOT/bin:$PATH
export LD_LIBRARY_PATH=$MPI_ROOT/lib:$UCX_ROOT/lib:${LD_LIBRARY_PATH:-}
export LIBRARY_PATH=$UCX_ROOT/lib:${LIBRARY_PATH:-}

cd $HOME/projectcopy || exit 1

export OMP_PROC_BIND=true
export OMP_PLACES=cores

# === CONFIG ===
TOTAL_GB=16
OUT_PREFIX=mpi/results_strong
TOTAL_CORES=32
RANKS_LIST=(32 16 8 4 2)
THREADS_LIST=(1 2 4 8 16)
PAYLOADS=(p256 p8192 pskew)
INPUT_TEMPLATE=inputs/input_%sgb_%s.dat

mkdir -p "$OUT_PREFIX"

echo ">>> [MPI Strong] Total input: ${TOTAL_GB}GB"
which mpirun

for payload in "${PAYLOADS[@]}"; do
  input_file=$(printf "$INPUT_TEMPLATE" "$TOTAL_GB" "$payload")
  if [[ ! -f "$input_file" ]]; then
    echo ">>> [MPI Strong] Skipping missing input: $input_file"
    continue
  fi
  for i in "${!RANKS_LIST[@]}"; do
    ranks="${RANKS_LIST[$i]}"
    threads="${THREADS_LIST[$i]}"
    if (( ranks * threads != TOTAL_CORES )); then
      echo ">>> [MPI Strong] Skipping invalid shape ranks=$ranks threads=$threads"
      continue
    fi
    export OMP_NUM_THREADS="$threads"
    echo ">>> [MPI Strong] ranks=$ranks threads=$threads input=$input_file"
    time -p mpirun -np "$ranks" --map-by "node:PE=${threads}" ./mpi/bin/merge_mpi "$input_file" "$OUT_PREFIX"
  done
done
