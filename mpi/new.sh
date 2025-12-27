#!/bin/bash
#SBATCH --job-name=bench_mpi
#SBATCH --output=results_mpi.log
#SBATCH --nodes=2                # Number of physical nodes (Example: 2 nodes)
#SBATCH --ntasks-per-node=1      # 1 MPI Process per node (Hybrid approach)
#SBATCH --cpus-per-task=16       # 16 CPUs per MPI process (For FastFlow threads)
#SBATCH --time=00:30:00

# CONFIG
RECORDS=20000000
PAYLOAD=256
MEM=1
FILE="data_mpi.bin"
WORKERS=13                       # FastFlow Workers per Node

# Load MPI Module (Adjust based on your cluster, e.g., openmpi or intel-mpi)
# module load openmpi

echo "=== [HYBRID MPI+FF] BENCHMARK ($SLURM_NNODES Nodes, $WORKERS Workers/Node , RECORDS : $RECORDS ,PAYLOAD : $PAYLOAD , MEMORY :$MEM  ) ==="

# 1. COMPILE TOOLS & CODE
# Using mpicxx wrapper as required for MPI compilation
echo "[Setup] Compiling..."
mkdir -p bin
mpicxx -O3 -DNDEBUG -DNO_DEFAULT_MAPPING -std=c++20 -pthread -I$HOME/fastflow mergesort_mpi.cpp -o bin/mergesort_mpi

# 2. GENERATE INPUT
# We generate one large file; MPI Ranks will seek/read their parts.
echo "[Setup] Generating Data..."
../tools/bin/datagen $FILE $RECORDS $PAYLOAD 1

# 3. RUN BENCHMARK
# Using srun to launch the MPI application [cite: 171]
# We pass the same arguments. Note that 'WORKERS' applies to *each* rank.
echo "[Run] Starting Sort..."
mpirun --bind-to none --report-bindings -n $SLURM_NPROCS ./bin/mergesort_mpi $FILE $RECORDS $PAYLOAD $MEM $WORKERS