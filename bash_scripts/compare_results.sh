#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

RESULTS_DIR="$PROJECT_ROOT/results"
AGGREGATED_FILE="$RESULTS_DIR/aggregated_metrics.csv"

BIDDING_SCRIPT="$PROJECT_ROOT/data_analysis/biddingComparison.Rscript"
MODE_SCRIPT="$PROJECT_ROOT/data_analysis/compareToCentralized.Rscript"

PLOTS_DIR="$PROJECT_ROOT/plots/comparison"

mkdir -p "$PLOTS_DIR"

if [ ! -f "$AGGREGATED_FILE" ]; then
    echo "Error: $AGGREGATED_FILE not found"
    exit 1
fi

echo "Processing aggregated metrics: $AGGREGATED_FILE"

echo "Generating bidding method comparisons..."
Rscript "$BIDDING_SCRIPT" "$AGGREGATED_FILE" "$PLOTS_DIR"

CENTRALIZED_DIR="$RESULTS_DIR/centralized"

if [ -d "$CENTRALIZED_DIR" ] && [ -n "$(ls -A "$CENTRALIZED_DIR" 2>/dev/null)" ]; then
    echo "Generating centralized vs decentralized comparisons..."
    Rscript "$MODE_SCRIPT" "$AGGREGATED_FILE" "$PLOTS_DIR"
else
    echo "Skipping centralized comparison (no results found in $CENTRALIZED_DIR)"
fi

echo "All comparison plots saved under $OUTPUT_DIR"

# for sfactor_dir in "$RESULTS_DIR"/sfactor_*; do
#     [ -d "$sfactor_dir" ] || continue

#     # Skip centralized subdirectory — it is a child of an sfactor dir, not one itself
#     [[ "$(basename "$sfactor_dir")" == "centralized" ]] && continue

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

#     # Only run centralized comparison if centralized results exist
#     centralized_dir="$sfactor_dir/centralized"
#     if [ -d "$centralized_dir" ] && [ -n "$(ls -A "$centralized_dir" 2>/dev/null)" ]; then
#         echo "  Generating centralized vs decentralized comparisons..."
#         Rscript "$MODE_SCRIPT" "$aggregated_file" "$output_dir"
#     else
#         echo "  Skipping centralized comparison (no results found in $centralized_dir)"
#     fi
# done

# if [ "$found_any" = false ]; then
#     echo "No sfactor_* directories found under $RESULTS_DIR"
#     exit 1
# fi

# echo "All comparison plots saved under $PLOTS_DIR/<sfactor>/comparison"
