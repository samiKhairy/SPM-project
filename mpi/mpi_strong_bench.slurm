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

export OMP_PLACES=cores
export OMP_PROC_BIND=close

# === CONFIG ===
TOTAL_GB=16
OUT_PREFIX=mpi/results_strong
NODES_LIST=(1 2 4 8)
THREADS_LIST=(4 8)
PAYLOADS=(p256 p8192)
INPUT_TEMPLATE=inputs/input_%sgb_%s.dat

mkdir -p "$OUT_PREFIX"

echo ">>> [MPI Strong] Total input: ${TOTAL_GB}GB"
which mpirun

for nodes in "${NODES_LIST[@]}"; do
  for payload in "${PAYLOADS[@]}"; do
    input_file=$(printf "$INPUT_TEMPLATE" "$TOTAL_GB" "$payload")
    for threads in "${THREADS_LIST[@]}"; do
      export OMP_NUM_THREADS="$threads"
      echo ">>> [MPI Strong] nodes=$nodes threads=$threads input=$input_file"
      time -p mpirun -np "$nodes" --map-by "node:PE=${threads}" ./mpi/bin/merge_mpi "$input_file" "$OUT_PREFIX"
    done
  done
done