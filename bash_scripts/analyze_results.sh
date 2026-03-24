#!/bin/bash
set -eu

NUM_JOBS=(2000)        #  4000 8000 16000 32000
R_VALUES=(16)        # 8 4 2 1

PY_ANALYZER="data_analysis/analyze_results.py"
R_ANALYZER="data_analysis/output_analysis.Rscript"
RESULT_DIRS=("results/sfactor_${R_VALUES[0]}" "results/sfactor_${R_VALUES[0]}/centralized")     #


# Workload files
DAYS=("busy" "bursty_low_stress" "bursty_high_stress")       # "busy" bursty_low_stress
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")
METHODS=(
    "HeuristicBidding"
    "EmbeddingBidding"
    # "llm_claude_bidder"
    "RandomBidding"
    "PureLocal"
)

PLOTS_DIR="plots/sfactor_${R_VALUES[0]}/individual"
PLOTS_DIR_CENTRALIZED="plots/sfactor_${R_VALUES[0]}/centralized"

mkdir -p "$PLOTS_DIR"
mkdir -p "$PLOTS_DIR_CENTRALIZED"


for day in "${DAYS[@]}"; do
    for i in "${!NUM_JOBS[@]}"; do
        num="${NUM_JOBS[$i]}"
        r="${R_VALUES[$i]}"
        for type in "${TYPES[@]}"; do

            workload_name="${day}_${type}_${num}_r${r}"

            for dir in "${RESULT_DIRS[@]}"; do
                for method in "${METHODS[@]}"; do

                    if [ "$dir" == "${RESULT_DIRS[0]}" ]; then
                        csv_file="$dir/${workload_name}_${method}.csv"
                    else
                        csv_file="$dir/${workload_name}_${method}.csv"
                    fi

                    if [ -f "$csv_file" ]; then
                        base_name=$(basename "$csv_file" .csv)
                        echo "Analyzing $csv_file"

                        if [ "$dir" == "${RESULT_DIRS[0]}" ]; then
                            python "$PY_ANALYZER" --csv_file "$csv_file" --output-dir "$PLOTS_DIR" --metrics-dir "$dir"
                            Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR/${base_name}_summary.pdf"
                        else
                            python "$PY_ANALYZER" --csv_file "$csv_file" --output-dir "$PLOTS_DIR_CENTRALIZED" --metrics-dir "$dir"
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
