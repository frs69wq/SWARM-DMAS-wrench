#!/bin/bash
set -eu

WORKLOAD_ANALYZER="data_analysis/workload_analysis.Rscript"
WORKLOAD_DIR="data_generation/data"
PLOTS_DIR="plots/workload"

mkdir -p "$PLOTS_DIR"

# Workload config
JOBS=(1000 2000 4000 8000 16000 32000)
R_VALUES=(32 16 8 4 2 1)
DAYS=("busy" "bursty_low_stress" "bursty_high_stress")
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")

for i in "${!JOBS[@]}"; do
    num_jobs=${JOBS[$i]}
    r_val=${R_VALUES[$i]}

    for day in "${DAYS[@]}"; do

        for type in "${TYPES[@]}"; do

        # ----- BUSY DAY -----
           workload_file="$WORKLOAD_DIR/${day}_${type}_${num_jobs}_r${r_val}.json"

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
done

echo "All workload analyses complete."
