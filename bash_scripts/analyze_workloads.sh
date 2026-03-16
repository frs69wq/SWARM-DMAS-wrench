#!/bin/bash
set -eu

WORKLOAD_ANALYZER="data_analysis/workload_analysis.Rscript"
WORKLOAD_DIR="data_generation/data"
PLOTS_DIR="plots/workload"

mkdir -p "$PLOTS_DIR"

# Workload config
BUSY_JOBS=(1000 2000 4000 8000 16000 32000)
R_VALUES=(32 16 8 4 2 1)
# DAYS=("idle" "busy")
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")

for i in "${!BUSY_JOBS[@]}"; do
    busy_num=${BUSY_JOBS[$i]}
    r_val=${R_VALUES[$i]}
    idle_num=$((busy_num / 6))

    for type in "${TYPES[@]}"; do

        # ----- BUSY DAY -----
        busy_workload_file="$WORKLOAD_DIR/busy_${type}_${busy_num}_r${r_val}.json"

        if [ -f "$busy_workload_file" ]; then
            base_name=$(basename "$busy_workload_file" .json)
            output_file="$PLOTS_DIR/${base_name}_workload_profile.pdf"

            echo "Analyzing workload: $busy_workload_file"

            Rscript "$WORKLOAD_ANALYZER" \
                "$busy_workload_file" \
                "$output_file"
        else
            echo "Skipping missing workload: $busy_workload_file"
        fi


        # # ----- IDLE DAY (1/6 of busy) -----
        # idle_workload_file="$WORKLOAD_DIR/idle_${type}_${idle_num}_r${r_val}.json"

        # if [ -f "$idle_workload_file" ]; then
        #     base_name=$(basename "$idle_workload_file" .json)
        #     output_file="$PLOTS_DIR/${base_name}_workload_profile.pdf"

        #     echo "Analyzing workload: $idle_workload_file"

        #     Rscript "$WORKLOAD_ANALYZER" \
        #         "$idle_workload_file" \
        #         "$output_file"
        # else
        #     echo "Skipping missing workload: $idle_workload_file"
        # fi

    done
done

echo "All workload analyses complete."
