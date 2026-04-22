#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"
RESULTS_DIR="$PROJECT_ROOT/results"

BIDDING_SCRIPT="$PROJECT_ROOT/data_analysis/biddingComparison.Rscript"
MODE_SCRIPT="$PROJECT_ROOT/data_analysis/compareToCentralized.Rscript"

PLOTS_DIR="$PROJECT_ROOT/plots"
mkdir -p "$PLOTS_DIR"

found_any=false

for sfactor_dir in "$RESULTS_DIR"/sfactor_*; do
    [ -d "$sfactor_dir" ] || continue

    # Skip centralized subdirectory — it is a child of an sfactor dir, not one itself
    [[ "$(basename "$sfactor_dir")" == "centralized" ]] && continue

    found_any=true

    sfactor_name="$(basename "$sfactor_dir")"
    aggregated_file="$sfactor_dir/aggregated_metrics.csv"
    output_dir="$PLOTS_DIR/$sfactor_name/comparison"

    if [ ! -f "$aggregated_file" ]; then
        echo "Warning: $aggregated_file not found, skipping $sfactor_name"
        continue
    fi

    mkdir -p "$output_dir"

    echo "Processing $sfactor_name ..."

    echo "  Generating bidding method comparisons..."
    Rscript "$BIDDING_SCRIPT" "$aggregated_file" "$output_dir"

    # Only run centralized comparison if centralized results exist
    centralized_dir="$sfactor_dir/centralized"
    if [ -d "$centralized_dir" ] && [ -n "$(ls -A "$centralized_dir" 2>/dev/null)" ]; then
        echo "  Generating centralized vs decentralized comparisons..."
        Rscript "$MODE_SCRIPT" "$aggregated_file" "$output_dir"
    else
        echo "  Skipping centralized comparison (no results found in $centralized_dir)"
    fi
done

if [ "$found_any" = false ]; then
    echo "No sfactor_* directories found under $RESULTS_DIR"
    exit 1
fi

echo "All comparison plots saved under $PLOTS_DIR/<sfactor>/comparison"

# #!/bin/bash
# set -eu

# SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"
# RESULTS_DIR="$PROJECT_ROOT/results"

# BIDDING_SCRIPT="$PROJECT_ROOT/data_analysis/biddingComparison.Rscript"
# MODE_SCRIPT="$PROJECT_ROOT/data_analysis/compareToCentralized.Rscript"

# PLOTS_DIR="$PROJECT_ROOT/plots"
# mkdir -p "$PLOTS_DIR"

# found_any=false

# for sfactor_dir in "$RESULTS_DIR"/sfactor_*; do
#     [ -d "$sfactor_dir" ] || continue
#     found_any=true

#     sfactor_name="$(basename "$sfactor_dir")"
#     aggregated_file="$sfactor_dir/aggregated_metrics.csv"

#     output_dir="$PLOTS_DIR/$sfactor_name/comparison"

#     if [ ! -f "$aggregated_file" ]; then
#         echo "Warning: $aggregated_file not found, skipping $sfactor_name"
#         continue
#     fi

#     mkdir -p "$output_dir"

#     echo "Processing $sfactor_name ..."
#     echo "  Generating bidding method comparisons..."
#     Rscript "$BIDDING_SCRIPT" "$aggregated_file" "$output_dir"

#     echo "  Generating centralized vs decentralized comparisons..."
#     Rscript "$MODE_SCRIPT" "$aggregated_file" "$output_dir"
# done

# if [ "$found_any" = false ]; then
#     echo "No sfactor_* directories found under $RESULTS_DIR"
#     exit 1
# fi

# echo "All comparison plots saved under $PLOTS_DIR/<sfactor>/comparison"

# OUTPUT_DIR="$PROJECT_ROOT/plots/comparison"

# # AGGREGATED_FILE="$PROJECT_ROOT/results/aggregated_metrics.csv"

# mkdir -p "$OUTPUT_DIR"

# echo "Generating bidding method comparisons..."
# Rscript "$BIDDING_SCRIPT" "$AGGREGATED_FILE" "$OUTPUT_DIR"

# echo "Generating centralized vs decentralized comparisons..."
# Rscript "$MODE_SCRIPT" "$AGGREGATED_FILE" "$OUTPUT_DIR"

# echo "All comparison plots saved in $OUTPUT_DIR"