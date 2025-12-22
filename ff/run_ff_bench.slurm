#run_ff_bench.slurm
#!/bin/bash
#SBATCH --job-name=ff_bench
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:20:00
#SBATCH --output=ff_bench_%j.out
#SBATCH --error=ff_bench_%j.err
#SBATCH --exclusive

# --- SETUP ---
cd $HOME/project || exit 1

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export OMP_PROC_BIND=close
export OMP_PLACES=cores

echo "=========================================="
echo "Node: $(hostname)"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Date: $(date)"
echo "=========================================="

# --- CONFIGURATION ---
INPUT=inputs/input_10gb.dat
OUTDIR=ff/results
MEM=8

mkdir -p $OUTDIR

# --- BENCHMARKS ---

echo ">>> BENCHMARK 1: FastFlow (Buf=4) | Workers=2"
rm -f $OUTDIR/run_*.dat
# Using standard bash 'time'
time -p ./ff/bin/run_gen_ff_buf4 $INPUT $MEM $OUTDIR/run_ 2

echo ">>> BENCHMARK 2: FastFlow (Buf=4) | Workers=4"
rm -f $OUTDIR/run_*.dat
time -p ./ff/bin/run_gen_ff_buf4 $INPUT $MEM $OUTDIR/run_ 4

echo ">>> BENCHMARK 3: FastFlow (Buf=4) | Workers=8"
rm -f $OUTDIR/run_*.dat
time -p ./ff/bin/run_gen_ff_buf4 $INPUT $MEM $OUTDIR/run_ 8

echo "Done."