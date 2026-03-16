#!/bin/bash
set -eu

PY_ANALYZER="data_analysis/analyze_results.py"
R_ANALYZER="data_analysis/output_analysis.Rscript"
RESULT_DIRS=("results")     # "results/centralized"
PLOTS_DIR="plots/individual"
PLOTS_DIR_CENTRALIZED="plots/centralized"

mkdir -p "$PLOTS_DIR"
mkdir -p "$PLOTS_DIR_CENTRALIZED"

# Workload files
DAYS=("busy")       # "idle" 
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")
NUM_JOBS=(1000)        #  4000 8000 16000 32000
R_VALUES=(32)        # 8 4 2 1
METHODS=(
    "HeuristicBidding"
    "EmbeddingBidding"
    # "llm_claude_bidder"
    "RandomBidding"
    "PureLocal"
)

for day in "${DAYS[@]}"; do
    for i in "${!NUM_JOBS[@]}"; do
        num="${NUM_JOBS[$i]}"
        r="${R_VALUES[$i]}"
        for type in "${TYPES[@]}"; do

            workload_name="${day}_${type}_${num}_r${r}"

            for dir in "${RESULT_DIRS[@]}"; do
                for method in "${METHODS[@]}"; do

                    if [ "$dir" == "results" ]; then
                        csv_file="$dir/${workload_name}_${method}.csv"
                    else
                        csv_file="$dir/${workload_name}_${method}.csv"
                    fi

                    if [ -f "$csv_file" ]; then
                        base_name=$(basename "$csv_file" .csv)
                        echo "Analyzing $csv_file"

                        if [ "$dir" == "results" ]; then
                            python "$PY_ANALYZER" --csv_file "$csv_file" --output-dir "$PLOTS_DIR" --metrics-dir "$dir"
                            Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR/${base_name}_summary.pdf"
                        else
                            python "$PY_ANALYZER" --csv_file "$csv_file" --output-dir "$PLOTS_DIR_CENTRALIZED" --metrics-dir dir
                            Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR_CENTRALIZED/${base_name}_summary.pdf"
                        fi
                    else
                        echo "Skipping missing file: $csv_file"
                    fi
                done
            done
        done
    done
done
