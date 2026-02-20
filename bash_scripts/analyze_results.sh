#!/bin/bash
set -eu

PY_ANALYZER="data_analysis/analyze_results.py"
R_ANALYZER="data_analysis/output_analysis.Rscript"
RESULTS_DIR="results"
PLOTS_DIR="plots/individual"

mkdir -p "$PLOTS_DIR"

# Workload files
DAYS=("idle" "busy")
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")
PYTHON_BIDDERS=(
    "HeuristicBidding"
    "EmbeddingBidding"
    "llm_claude_bidder"
)
BASELINE_POLICIES=("RandomBidding" "PureLocal")

for day in "${DAYS[@]}"; do
    if [ "$day" == "idle" ]; then
        num=100
    else
        num=700
    fi

    for type in "${TYPES[@]}"; do
        workload_name="${day}_${type}_${num}"
        
        # PythonBidding policies
        for bidder in "${PYTHON_BIDDERS[@]}"; do
            csv_file="$RESULTS_DIR/${workload_name}_${bidder}.csv"
            if [ -f "$csv_file" ]; then
                base_name=$(basename "$csv_file" .csv)

                echo "Analyzing $csv_file"

                python "$PY_ANALYZER" "$csv_file" --output-dir "$PLOTS_DIR"
                Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR/${base_name}_summary.pdf"
            else
                echo "Skipping missing file: $csv_file"
            fi
        done

        # Baseline policies
        for policy in "${BASELINE_POLICIES[@]}"; do
            csv_file="$RESULTS_DIR/${workload_name}_${policy}.csv"

            if [ -f "$csv_file" ]; then
                base_name=$(basename "$csv_file" .csv)

                echo "Analyzing $csv_file"

                python "$PY_ANALYZER" "$csv_file" --output-dir "$PLOTS_DIR"
                Rscript "$R_ANALYZER" "$csv_file" "$PLOTS_DIR/${base_name}_summary.pdf"
            else
                echo "Skipping missing file: $csv_file"
            fi
        done
    done
done

echo "All analyses complete."