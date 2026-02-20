#!/bin/bash
set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

R_SCRIPT="$PROJECT_ROOT/data_analysis/comparison_analysis.Rscript"
INPUT_FILE="$PROJECT_ROOT/results/aggregated_metrics.csv"
OUTPUT_DIR="$PROJECT_ROOT/plots/comparisons"

Rscript "$R_SCRIPT" "$INPUT_FILE" "$OUTPUT_DIR"

echo "All comparison plots generated." 