#!/bin/bash
#SBATCH --job-name=bench_omp
#SBATCH --output=results_omp.log
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --time=00:30:00

# CONFIG
RECORDS=1000000
PAYLOAD=4096
MEM=128
FILE="data_omp.bin"  # Unique name to prevent conflicts
THREADS=16

echo "=== [2] OPENMP BENCHMARK ($THREADS Threads , RECORDS : $RECORDS ,PAYLOAD : $PAYLOAD , MEMORY :$MEM  ) ==="

# 1. COMPILE TOOLS & CODE
echo "[Setup] Compiling..."
mkdir -p bin
g++ -O3 -DNDEBUG -fopenmp -std=c++20 mergesort_omp.cpp -o bin/mergesort_omp

# 2. GENERATE INPUT
echo "[Setup] Generating Data..."
../tools/bin/datagen $FILE $RECORDS $PAYLOAD 1

# 3. RUN BENCHMARK
echo "--- I/O BOUND TEST (Records=$RECORDS, Payload=$PAYLOAD) ---"
srun ./bin/mergesort_omp $FILE $RECORDS $PAYLOAD $MEM $THREADS
echo "=== Done ==="