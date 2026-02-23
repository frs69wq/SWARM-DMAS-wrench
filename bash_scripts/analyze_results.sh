#!/bin/bash
set -eu

PY_ANALYZER="data_analysis/analyze_results_old.py"
R_ANALYZER="data_analysis/output_analysis.Rscript"
RESULT_DIRS=("results" "results/centralized")
PLOTS_DIR="plots/individual"
PLOTS_DIR_CENTRALIZED="plots/centralized"

mkdir -p "$PLOTS_DIR"
mkdir -p "$PLOTS_DIR_CENTRALIZED"

# Workload files
DAYS=("idle" "busy")
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")

METHODS=(
    "HeuristicBidding"
    "EmbeddingBidding"
    "llm_claude_bidder"
    "RandomBidding"
    "PureLocal"
)

for day in "${DAYS[@]}"; do
    if [ "$day" == "idle" ]; then
        num=100
    else
        num=700
    fi

    for type in "${TYPES[@]}"; do
        workload_name="${day}_${type}_${num}"

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
                        python "$PY_ANALYZER" "$csv_file" --output-dir "$PLOTS_DIR"
                        Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR/${base_name}_summary.pdf"
                    else
                        python "$PY_ANALYZER" "$csv_file" --output-dir "$PLOTS_DIR_CENTRALIZED"
                        Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR_CENTRALIZED/${base_name}_summary.pdf"
                    fi
                else
                    echo "Skipping missing file: $csv_file"
                fi

            done
        done
    done
done
