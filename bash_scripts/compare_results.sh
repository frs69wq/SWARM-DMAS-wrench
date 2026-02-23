#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

AGGREGATED_FILE="$PROJECT_ROOT/results/aggregated_metrics.csv"

BIDDING_SCRIPT="$PROJECT_ROOT/data_analysis/biddingComparison.Rscript"
MODE_SCRIPT="$PROJECT_ROOT/data_analysis/compareToCentralized.Rscript"

OUTPUT_DIR="$PROJECT_ROOT/plots/comparison"

mkdir -p "$OUTPUT_DIR"

echo "Generating bidding method comparisons..."
Rscript "$BIDDING_SCRIPT" "$AGGREGATED_FILE" "$OUTPUT_DIR"

echo "Generating centralized vs decentralized comparisons..."
Rscript "$MODE_SCRIPT" "$AGGREGATED_FILE" "$OUTPUT_DIR"

echo "All comparison plots saved in $OUTPUT_DIR"