#!/bin/bash
set -e

EXEC_FILE=./build/swarm_dmas

# workload files
WORKLOADS=()
DAYS=("idle" "busy")
TYPES=("homogeneous_short" "only_large_long" "mixed_80_20" "mixed_20_80")
for day in "${DAYS[@]}"; do
    if [ "$day" == "idle" ]; then
        num=100
    else
        num=700
    fi

    for type in "${TYPES[@]}"; do
        WORKLOADS+=("data_generation/data/${day}_${type}_${num}.json")
    done
done


# --------------------------------------------------
# Run experiments for DECENTRALIZED scheduling
# --------------------------------------------------
TEMPLATE=experiments/test_decentralized.json
RESULT_DIR=results

mkdir -p $RESULT_DIR

# # Run experiments for PythonBidding policies
# PYTHON_BIDDERS=(
#     "python_scripts/HeuristicBidding.py"
#     "python_scripts/EmbeddingBidding.py"
#     # "python_scripts/llm_claude_bidder.py"
# )

# for workload in "${WORKLOADS[@]}"; do
#     workload_name=$(basename "$workload" .json)

#     for bidder in "${PYTHON_BIDDERS[@]}"; do
#         bidder_name=$(basename "$bidder" .py)

#         output_file="$RESULT_DIR/${workload_name}_${bidder_name}.csv"
#         temp_json="temp_config.json"

#         jq \
#         --arg workload "$workload" \
#         --arg bidder "$bidder" \
#         '
#         .workload = $workload |
#         .decentralized_policy = "PythonBidding" |
#         .decentralized_bidder = $bidder
#         ' $TEMPLATE > $temp_json

#         echo "Running $workload_name - PythonBidding - $bidder_name"

#         $EXEC_FILE $temp_json > "$output_file"

#         rm $temp_json
#     done
# done


# # Run experiments for baseline bidding policies
# BASELINE_POLICIES=(
#         "RandomBidding" 
#         "PureLocal"
#     )

# for workload in "${WORKLOADS[@]}"; do
#     workload_name=$(basename "$workload" .json)

#     for bidder in "${BASELINE_POLICIES[@]}"; do
#         output_file="$RESULT_DIR/${workload_name}_${bidder}.csv"
#         temp_json="temp_config.json"

#         jq \
#         --arg workload "$workload" \
#         --arg policy "$bidder" \
#         '
#         .workload = $workload |
#         .decentralized_policy = $policy |
#         del(.decentralized_bidder)
#         ' $TEMPLATE > $temp_json

#         echo "Running $workload_name - $bidder"

#         $EXEC_FILE $temp_json > "$output_file"

#         rm $temp_json
#     done
# done


CENTRALIZED_TEMPLATE=experiments/test_centralized.json
RESULT_DIR_CENTRALIZED=results/centralized

mkdir -p $RESULT_DIR_CENTRALIZED
# --------------------------------------------------
# Run experiments for CENTRALIZED scheduling
# This is now hardcoded for a single bidding method, this will be automated later when CentralizedScheduling.py is updated.
# --------------------------------------------------

for workload in "${WORKLOADS[@]}"; do
    workload_name=$(basename "$workload" .json)
    bidder='EmbeddingBidding' 

    output_file="$RESULT_DIR_CENTRALIZED/${workload_name}_${bidder}.csv"
    temp_json="temp_config.json"

    jq \
    --arg workload "$workload" \
    '
    .workload = $workload
    ' $CENTRALIZED_TEMPLATE > $temp_json

    echo "Running $workload_name - CentralizedScheduling - $bidder"

    $EXEC_FILE $temp_json > "$output_file"

    rm $temp_json
done


# echo "All experiments completed."