#!/bin/bash
#SBATCH --job-name=mpi_sort
#SBATCH --nodes=4                # Number of nodes
#SBATCH --ntasks-per-node=1      # 1 MPI rank per node (Hybrid approach)
#SBATCH --cpus-per-task=8        # 8 OpenMP threads per rank
#SBATCH --time=00:30:00
#SBATCH --output=mpi/mpi_%j.out
#SBATCH --exclusive

# --- 1. SET UP ENVIRONMENT (Critical for OpenMPI 5) ---
# We must re-define these because compute nodes start fresh!
export MPI_ROOT=/opt/ohpc/pub/mpi/openmpi5-gnu12/5.0.3
export UCX_ROOT=/opt/ohpc/pub/mpi/ucx-ohpc/1.14.0

export PATH=$MPI_ROOT/bin:$PATH
export LD_LIBRARY_PATH=$MPI_ROOT/lib:$UCX_ROOT/lib:$LD_LIBRARY_PATH
export LIBRARY_PATH=$UCX_ROOT/lib:$LIBRARY_PATH

# --- 2. Config ---
cd $HOME/project
INPUT=inputs/input_10gb.dat
OUT_PREFIX=mpi/results/

# --- 3. Execution ---
export OMP_NUM_THREADS=8
export OMP_PLACES=cores
export OMP_PROC_BIND=close

echo "Running MPI Sort on $SLURM_JOB_NUM_NODES nodes..."
which mpirun  # Verify it is picking up the correct version

# Use full path to binary just to be safe, though relative works if cd is correct
time mpirun -np 4 --map-by node:PE=8 ./mpi/bin/merge_mpi $INPUT $OUT_PREFIX