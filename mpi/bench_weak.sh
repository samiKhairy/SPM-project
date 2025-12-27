#!/bin/bash
#SBATCH --job-name=bench_weak
#SBATCH --output=results_weak.log
#SBATCH --nodes=2                # UPDATED: Max 2 Nodes
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --time=00:20:00          # Reduced time
#SBATCH --exclusive

# CONFIG
PAYLOAD=256
MEM=128
WORKERS=13
REC_PER_NODE=3000000

echo "=== WEAK SCALABILITY BENCHMARK (2 NODES) ==="

# 1. COMPILE
echo "[Setup] Compiling..."
mkdir -p bin
mpicxx -O3 -DNDEBUG -DNO_DEFAULT_MAPPING -std=c++20 -pthread -I$HOME/fastflow mergesort_mpi.cpp -o bin/mergesort_mpi

# 2. RUN LOOP (Only 1 and 2)
for np in 1 2; do
    # Calculate total records (3M * N)
    TOTAL_REC=$((REC_PER_NODE * np))
    FILE_WEAK="data_weak_${np}nodes.bin"
    
    echo ">>> Running Weak Scaling on $np Nodes ($TOTAL_REC records)..."
    
    # A. Generate
    ../tools/bin/datagen $FILE_WEAK $TOTAL_REC $PAYLOAD 1
    
    # B. Run
    mpirun -np $np --map-by node --bind-to none ./bin/mergesort_mpi $FILE_WEAK $TOTAL_REC $PAYLOAD $MEM $WORKERS
    
    # C. Cleanup
    rm $FILE_WEAK
done

echo "=== WEAK BENCHMARK COMPLETE ==="