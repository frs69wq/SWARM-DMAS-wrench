#!/bin/bash
set -eu

WORKLOAD_ANALYZER="data_analysis/workload_analysis.Rscript"
WORKLOAD_DIR="data_generation/data"
PLOTS_DIR="plots/workload"

mkdir -p "$PLOTS_DIR"

# Workload config
DAYS=("idle" "busy")
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")

for day in "${DAYS[@]}"; do
    if [ "$day" == "idle" ]; then
        num=100
    else
        num=700
    fi

    for type in "${TYPES[@]}"; do
        workload_file="$WORKLOAD_DIR/${day}_${type}_${num}.json"

        if [ -f "$workload_file" ]; then
            base_name=$(basename "$workload_file" .json)
            output_file="$PLOTS_DIR/${base_name}_workload_profile.pdf"

            echo "Analyzing workload: $workload_file"

            Rscript "$WORKLOAD_ANALYZER" \
                "$workload_file" \
                "$output_file"
        else
            echo "Skipping missing workload: $workload_file"
        fi
    done
done

echo "All workload analyses complete."
