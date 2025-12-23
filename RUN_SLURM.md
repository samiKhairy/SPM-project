# Running the benchmark SLURM scripts (step-by-step)

This guide is written so you can follow it line-by-line on the cluster login node. It also includes a fix for the **DOS line ending** error you saw from `sbatch`.

---

## 0) Go to the repo root

```bash
cd ~/projectcopy
pwd
```

You should see the repo folders (`omp`, `ff`, `mpi`, `inputs`, `tools`).

---

## 1) Fix the “DOS line breaks” error (one-time)

If `sbatch` prints:

```
sbatch: error: Batch script contains DOS line breaks (\r\n)
```

Run the following **one-time normalization**. It converts all `.slurm` scripts to UNIX line endings (`\n`) in-place:

```bash
python - <<'PY'
from pathlib import Path
root = Path('.')
for path in root.rglob('*.slurm'):
    data = path.read_bytes()
    if b'\r\n' in data:
        path.write_bytes(data.replace(b'\r\n', b'\n'))
        print(f"fixed {path}")
PY
```

Re-run the `sbatch` command after this.

---

## 2) Make sure inputs exist (recommended check)

These scripts expect input files like:

```
inputs/input_10gb_p256.dat
inputs/input_10gb_p8192.dat
```

Verify the files exist:

```bash
ls -lh inputs/input_10gb_*.dat
```

---

## 3) Submit the OpenMP sweeps (one dimension at a time)

**K sweep (K = 4, 16, 64)**
```bash
sbatch omp/omp_bench_k_sweep.slurm
```

**Memory sweep (MEM_MERGE_GB = 8, 16, 32)**
```bash
sbatch omp/omp_bench_mem_sweep.slurm
```

**Thread sweep (OMP threads = 1, 2, 4, 8, 16)**
```bash
sbatch omp/omp_bench_thread_sweep.slurm
```

---

## 4) Submit the FastFlow sweeps (one dimension at a time)

**K sweep (K = 4, 16, 64)**
```bash
sbatch ff/ff_bench_k_sweep.slurm
```

**Memory sweep (MEM_MERGE_GB = 8, 16, 32)**
```bash
sbatch ff/ff_bench_mem_sweep.slurm
```

**Worker sweep (workers = 1, 2, 4, 8, 16)**
```bash
sbatch ff/ff_bench_thread_sweep.slurm
```

---

## 5) (Optional) Run the full, longer sweeps

These do **all combinations** and may exceed the 30-minute limit depending on input size and the cluster.

```bash
sbatch omp/omp_bench_sweep.slurm
sbatch ff/ff_bench_sweep.slurm
```

---

## 6) Check job status and logs

Check queued/running jobs:
```bash
squeue -u $USER
```

Each job writes a log file named like:
```
omp_k_sweep_<jobid>.out
ff_mem_sweep_<jobid>.out
```

You can view a log live with:
```bash
tail -f omp_k_sweep_<jobid>.out
```

---

## 7) Common issues and fixes

- **Missing input files**: the scripts will skip missing inputs, but you’ll get fewer data points.
- **Still seeing the CRLF error**: re-run step 1 and confirm your terminal shows `fixed <file>` for at least one script.
- **Job timeout**: reduce input size or run only one sweep dimension.
