#!/usr/bin/env bash
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

workload_params() {
  local w="${1:-cpu}"
  case "$w" in
    # Thread Scaling	
    #io)  RECORDS=20000000;  PAYLOAD=128; MEM=256 ;;
    #cpu) RECORDS=20000000; PAYLOAD=16;  MEM=64 ;;
	
    # N scaling		
    # io)  RECORDS=30000000;  PAYLOAD=128; MEM=256 ;;
    # cpu) RECORDS=30000000; PAYLOAD=16;  MEM=64 ;;
	
    #  payload vary scaling		
     io)  RECORDS=20000000;  PAYLOAD=128; MEM=256 ;;
     cpu) RECORDS=20000000; PAYLOAD=16;  MEM=256 ;;
	


    *) die "Usage: sbatch new.sh [cpu|io]" ;;
  esac
}

setup_logging() {
  local results_dir="$1" tag="$2"
  mkdir -p "$results_dir"
  JOBID="${SLURM_JOB_ID:-manual}"
  LOG_FILE="${results_dir}/${tag}_job${JOBID}.log"
  exec > >(tee -a "$LOG_FILE") 2>&1
}

build_datagen() {
  local submit_dir="$1"
  mkdir -p "$submit_dir/../tools/bin"
  g++ -O3 -DNDEBUG -std=c++20 "$submit_dir/../tools/datagen.cpp" -o "$submit_dir/../tools/bin/datagen"
}

ensure_dataset() {
  local submit_dir="$1" data_file="$2"
  mkdir -p "$(dirname "$data_file")"
  if [[ ! -f "$data_file" ]]; then
    echo "[Setup] Generating Data..."
    "$submit_dir/../tools/bin/datagen" "$data_file" "$RECORDS" "$PAYLOAD" 1
  else
    echo "[Setup] Reusing existing dataset."
  fi
  ls -lh "$data_file" || true
}

mk_scratch_and_trap() {
 scratch_dir="$1"
  mkdir -p "$scratch_dir"
  cd "$scratch_dir"
  cleanup() {
    cd /
    rm -rf "$scratch_dir"
    echo "[Cleanup] Removed $scratch_dir"
  }
  trap cleanup EXIT
}
