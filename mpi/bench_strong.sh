#!/bin/bash
#SBATCH --job-name=bench_strong
#SBATCH --output=results_strong.log
#SBATCH --nodes=2                # Request 4 nodes (Change to 8 if you want to test 8)
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --time=00:30:00
#SBATCH --exclusive

# CONFIG
PAYLOAD=256
MEM=128
WORKERS=13
RECORDS_STRONG=20000000
DATA_STRONG="data_strong.bin"

echo "=== STRONG SCALABILITY BENCHMARK ==="

# 1. COMPILE
echo "[Setup] Compiling..."
mkdir -p bin
mpicxx -O3 -DNDEBUG -DNO_DEFAULT_MAPPING -std=c++20 -pthread -I$HOME/fastflow mergesort_mpi.cpp -o bin/mergesort_mpi

# 2. GENERATE DATA
echo "[Strong] Generating $DATA_STRONG ($RECORDS_STRONG records)..."
../tools/bin/datagen $DATA_STRONG $RECORDS_STRONG $PAYLOAD 1

# 3. RUN LOOP (1, 2, 4 Nodes)
# Added '8' to the list, it will only run if you allocated 8 nodes above
for np in 1 2 4 8; do
    if [ "$np" -le "$SLURM_NNODES" ]; then
        echo ">>> Running Strong Scaling on $np Nodes..."
        mpirun -np $np --map-by node --bind-to none ./bin/mergesort_mpi $DATA_STRONG $RECORDS_STRONG $PAYLOAD $MEM $WORKERS
    fi
done

# Cleanup
rm $DATA_STRONG
echo "=== STRONG BENCHMARK COMPLETE ==="