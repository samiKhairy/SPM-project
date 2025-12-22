#!/usr/bin/env bash
set -euo pipefail

MODE="${MODE:-omp}"               # omp | ff | seq
INPUT="${INPUT:-inputs/input_10gb.dat}"
OUT_DIR="${OUT_DIR:-results_sweep}"
CSV="${CSV:-results_sweep.csv}"

THREADS_LIST=(${THREADS_LIST:-1 2 4 8})
WORKERS_LIST=(${WORKERS_LIST:-1 2 4 8})
K_LIST=(${K_LIST:-16 32})
MEM_GEN_LIST=(${MEM_GEN_LIST:-32 64})     # MB
MEM_MERGE_GB_LIST=(${MEM_MERGE_GB_LIST:-8 16}) # GB

mkdir -p "${OUT_DIR}"
echo "mode,threads,workers,k,mem_gen_mb,mem_merge_gb,gen_s,merge_s,verify_rc" > "${CSV}"

time_cmd() {
  local out_file="$1"
  shift
  /usr/bin/time -f "%e" -o "${out_file}" "$@"
}

run_verify() {
  local file="$1"
  if ./tools/verify_sorted "${file}" >/dev/null 2>&1; then
    echo 0
  else
    echo 1
  fi
}

for k in "${K_LIST[@]}"; do
  for mem_gen in "${MEM_GEN_LIST[@]}"; do
    for mem_merge in "${MEM_MERGE_GB_LIST[@]}"; do
      if [[ "${MODE}" == "seq" ]]; then
        RUN_DIR="${OUT_DIR}/seq_runs"
        FINAL_OUT="${OUT_DIR}/seq_final.dat"
        rm -rf "${RUN_DIR}"
        mkdir -p "${RUN_DIR}"

        gen_time_file="${OUT_DIR}/gen_time.txt"
        merge_time_file="${OUT_DIR}/merge_time.txt"

        time_cmd "${gen_time_file}" ./seq/bin/run_gen_seq "${INPUT}" "${mem_gen}" "${RUN_DIR}/run_"
        num_runs=$(ls -1 "${RUN_DIR}"/run_*.dat | wc -l | tr -d ' ')
        time_cmd "${merge_time_file}" ./seq/bin/merge_seq "${RUN_DIR}/run_" "${num_runs}" "${FINAL_OUT}" "${mem_merge}"

        gen_s=$(cat "${gen_time_file}")
        merge_s=$(cat "${merge_time_file}")
        verify_rc=$(run_verify "${FINAL_OUT}")

        echo "seq,1,1,${k},${mem_gen},${mem_merge},${gen_s},${merge_s},${verify_rc}" >> "${CSV}"
        continue
      fi

      if [[ "${MODE}" == "ff" ]]; then
        for workers in "${WORKERS_LIST[@]}"; do
          RUN_DIR="${OUT_DIR}/ff_runs_${workers}"
          FINAL_OUT="${OUT_DIR}/ff_final_w${workers}.dat"
          rm -rf "${RUN_DIR}"
          mkdir -p "${RUN_DIR}"

          gen_time_file="${OUT_DIR}/gen_time.txt"
          merge_time_file="${OUT_DIR}/merge_time.txt"

          time_cmd "${gen_time_file}" ./ff/bin/run_gen_ff "${INPUT}" "${mem_gen}" "${RUN_DIR}/run_" "${workers}"
          time_cmd "${merge_time_file}" ./ff/bin/merge_ff "${RUN_DIR}/run_" "${FINAL_OUT}" "${k}" "${mem_merge}" "${workers}"

          gen_s=$(cat "${gen_time_file}")
          merge_s=$(cat "${merge_time_file}")
          verify_rc=$(run_verify "${FINAL_OUT}")

          echo "ff,1,${workers},${k},${mem_gen},${mem_merge},${gen_s},${merge_s},${verify_rc}" >> "${CSV}"
        done
        continue
      fi

      for threads in "${THREADS_LIST[@]}"; do
        RUN_DIR="${OUT_DIR}/omp_runs_${threads}"
        FINAL_OUT="${OUT_DIR}/omp_final_t${threads}.dat"
        rm -rf "${RUN_DIR}"
        mkdir -p "${RUN_DIR}"

        gen_time_file="${OUT_DIR}/gen_time.txt"
        merge_time_file="${OUT_DIR}/merge_time.txt"

        export OMP_NUM_THREADS="${threads}"
        export OMP_PLACES=cores
        export OMP_PROC_BIND=close

        time_cmd "${gen_time_file}" ./omp/run_gen_omp "${INPUT}" "${mem_gen}" "${RUN_DIR}/run_"
        time_cmd "${merge_time_file}" ./omp/merge_omp "${RUN_DIR}/run_" "${FINAL_OUT}" "${k}" "${mem_merge}"

        gen_s=$(cat "${gen_time_file}")
        merge_s=$(cat "${merge_time_file}")
        verify_rc=$(run_verify "${FINAL_OUT}")

        echo "omp,${threads},1,${k},${mem_gen},${mem_merge},${gen_s},${merge_s},${verify_rc}" >> "${CSV}"
      done
    done
  done
done

echo "Results saved to ${CSV}"
