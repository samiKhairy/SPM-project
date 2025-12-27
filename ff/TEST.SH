#!/bin/bash
#SBATCH --job-name=bench_ff
#SBATCH --output=results_ff.log
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --time=00:30:00

# CONFIG
RECORDS=1000000
PAYLOAD=4096
MEM=128
WORKERS=13
FILE="data_ff.bin"

echo "=== [3] FASTFLOW BENCHMARK (RECORDS : $RECORDS ,PAYLOAD : $PAYLOAD , MEMORY :$MEM  ) ==="

# 1. COMPILE TOOLS & CODE
echo "[Setup] Compiling..."
mkdir -p bin
g++ -O3 -DNDEBUG -DNO_DEFAULT_MAPPING -std=c++20 -I$HOME/fastflow -pthread mergesort_ff.cpp -o bin/mergesort_ff

# 2. GENERATE INPUT
echo "[Setup] Generating Data..."
../tools/bin/datagen $FILE $RECORDS $PAYLOAD 1

# 3. RUN BENCHMARK
echo "[Run] Starting Sort..."
echo "--- I/O BOUND TEST (N=10M, Payload=16) ---"
srun ./bin/mergesort_ff $FILE $RECORDS $PAYLOAD $MEM $WORKERS


echo "=== Done ==="