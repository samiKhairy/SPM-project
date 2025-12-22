#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=tools/bench_common.sh
source "$SCRIPT_DIR/bench_common.sh"

usage() {
  cat <<'EOF'
Usage:
  run_ff_bench.sh --inputs <comma-separated paths> [options]

Options:
  --inputs <paths>           required (e.g., inputs/input_5gb.dat,inputs/input_10gb.dat)
  --workers <list>           default: 1,2,4,8,16
  --mem-gen-mb <int>         default: 32
  --mem-merge-gb <int>       default: 16
  --k <int>                  default: 32
  --run-gen <path>           default: ff/bin/run_gen_ff
  --run-merge <path>         default: ff/bin/merge_ff
  --work-dir <path>          default: results/ff_runs
  --output-csv <path>        default: results/ff_results.csv
EOF
}

main() {
    local inputs=""
    local workers="1,2,4,8,16"
    local mem_gen_mb=32
    local mem_merge_gb=16
    local k=32
    local run_gen="ff/bin/run_gen_ff"
    local run_merge="ff/bin/merge_ff"
    local work_dir="results/ff_runs"
    local output_csv="results/ff_results.csv"
    
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --inputs)
                inputs="$2"
                shift 2
            ;;
            --workers)
                workers="$2"
                shift 2
            ;;
            --mem-gen-mb)
                mem_gen_mb="$2"
                shift 2
            ;;
            --mem-merge-gb)
                mem_merge_gb="$2"
                shift 2
            ;;
            --k)
                k="$2"
                shift 2
            ;;
            --run-gen)
                run_gen="$2"
                shift 2
            ;;
            --run-merge)
                run_merge="$2"
                shift 2
            ;;
            --work-dir)
                work_dir="$2"
                shift 2
            ;;
            --output-csv)
                output_csv="$2"
                shift 2
            ;;
            *)
                log "Unknown option: $1"
                usage
                exit 1
            ;;
        esac
    done
    
    if [[ -z "$inputs" ]]; then
        log "Missing required flag: --inputs"
        usage
        exit 1
    fi
    
    write_csv_header "$output_csv"
    
    declare -A baseline
    
    while IFS= read -r input_path; do
        local input_bytes
        input_bytes=$(file_size_bytes "$input_path")
        
        while IFS= read -r workers_value; do
            local run_dir="$work_dir/ff_runs_w${workers_value}"
            mkdir -p "$run_dir"
            rm -f "$run_dir"/run_*.dat
            
            log "[FF] input=${input_path} workers=${workers_value}"
            
            local gen_time_file="$run_dir/gen_time.txt"
            local merge_time_file="$run_dir/merge_time.txt"
            
            /usr/bin/time -f "%e" -o "$gen_time_file" "$run_gen" "$input_path" "$mem_gen_mb" "$run_dir/run_" "$workers_value"
            /usr/bin/time -f "%e" -o "$merge_time_file" "$run_merge" "$run_dir/run_" "$run_dir/final.dat" "$k" "$mem_merge_gb" "$workers_value"
            
            local gen_s merge_s total_s
            gen_s=$(cat "$gen_time_file")
            merge_s=$(cat "$merge_time_file")
            total_s=$(float_add "$gen_s" "$merge_s")
            
            if [[ "$workers_value" == "1" ]]; then
                baseline["$input_path"]="$total_s"
            fi
            
            local speedup efficiency throughput
            speedup=$(float_div "${baseline[$input_path]:-$total_s}" "$total_s")
            efficiency=$(float_div "$speedup" "$workers_value")
            throughput=$(throughput_mb_s "$input_bytes" "$total_s")
            
            append_csv "$output_csv" "ff,$input_path,$input_bytes,$(float_div "$input_bytes" "$BYTES_PER_GB"),1,1,$workers_value,$total_s,$throughput,$speedup,$efficiency,$gen_s,$merge_s"
        done < <(split_list "$workers")
    done < <(split_list "$inputs")
    
    log "Results saved to $output_csv"
}

main "$@"