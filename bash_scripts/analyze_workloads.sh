#!/bin/bash
set -eu

WORKLOAD_ANALYZER="data_analysis/workload_analysis.Rscript"
WORKLOAD_DIR="data_generation/data"

RHO_VALUES=("0.9" "1.5")

declare -A SCENARIO_NJOBS_RHO09=(
    ["small_short"]=2880
    ["mixed_80_20"]=251
    ["mixed_20_80"]=67
    ["large_long"]=54
)

declare -A SCENARIO_NJOBS_RHO15=(
    ["small_short"]=4800
    ["mixed_80_20"]=415
    ["mixed_20_80"]=112
    ["large_long"]=91
)

DAYS=("business" "bursty_low_stress" "bursty_high_stress")
TYPES=("small_short" "large_long" "mixed_80_20" "mixed_20_80")
 
PLOTS_DIR="plots/workload"
mkdir -p "$PLOTS_DIR"
 
for rho in "${RHO_VALUES[@]}"; do
    for day in "${DAYS[@]}"; do
        for type in "${TYPES[@]}"; do

            if [ "$rho" = "0.9" ]; then
                num_jobs="${SCENARIO_NJOBS_RHO09[$type]}"
            elif [ "$rho" = "1.5" ]; then
                num_jobs="${SCENARIO_NJOBS_RHO15[$type]}"
            else
                echo "Unknown rho: $rho"
                exit 1
            fi

            workload_file="$WORKLOAD_DIR/${day}_${type}_${num_jobs}_rho${rho}.json"

            if [ -f "$workload_file" ]; then
                base_name=$(basename "$workload_file" .json)
                output_file="$PLOTS_DIR/${base_name}.pdf"

                echo "Analyzing workload: $workload_file"
                echo "Saving to: $output_file"

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
