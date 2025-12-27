#!/bin/bash
#SBATCH --job-name=bench_seq
#SBATCH --output=results_seq.log
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00

# CONFIG
RECORDS=5000000
PAYLOAD=256
MEM=128
FILE="data_seq.bin"

echo "=== [1] SEQUENTIAL BENCHMARK === RECORDS : $RECORDS ,PAYLOAD : $PAYLOAD , MEMORY :$MEM  "

# 1. COMPILE TOOLS & CODE
echo "[Setup] Compiling..."
mkdir -p bin
g++ -O3 -DNDEBUG -std=c++20 mergesort_seq.cpp -o bin/mergesort_seq

# 2. GENERATE INPUT (Excluded from benchmark time)
echo "[Setup] Generating Data..."
g++ -O3 -std=c++20 ../tools/datagen.cpp -o ../tools/bin/datagen
../tools/bin/datagen $FILE $RECORDS $PAYLOAD 1

# 3. RUN BENCHMARK
echo "[Run] Starting Sort..."
srun ./bin/mergesort_seq $FILE $RECORDS $PAYLOAD $MEM
