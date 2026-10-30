#!/bin/bash
set -euo pipefail

EXEC_FILE=./build/swarm_dmas
WRENCH_ARGS="--wrench-commport-pool-size=80000"     # --cfg=precision/work-amount:1e-12

# Number of concurrent runs
MAX_JOBS=8
SLEEP_BETWEEN_RUNS=120   # seconds

declare -A SCENARIO_NJOBS_RHO_15=(
    ["mixed_80_20"]=415
    ["mixed_20_80"]=112
    ["large_long"]=91
    ["small_short"]=4800
)
declare -A SCENARIO_NJOBS_RHO_09=(
    ["mixed_80_20"]=251
    ["mixed_20_80"]=67
    ["large_long"]=54
    ["small_short"]=2880
)

DAYS=("bursty_low_stress" "bursty_high_stress" "business")           #  "bursty_low_stress" "bursty_high_stress" business" 
TYPES=("mixed_80_20" "mixed_20_80" "large_long" "small_short")       #    "mixed_80_20"  "mixed_20_80" "large_long" "small_short"
RHO_VALUES=(1.5 0.9)            #   1.5 0.9


# Build workload list with per-scenario job counts
WORKLOADS=()

for rho in "${RHO_VALUES[@]}"; do
    if [[ "$rho" == "1.5" ]]; then
            declare -n SCENARIO_NJOBS=SCENARIO_NJOBS_RHO_15
        else
            declare -n SCENARIO_NJOBS=SCENARIO_NJOBS_RHO_09
        fi

    for day in "${DAYS[@]}"; do
        for type in "${TYPES[@]}"; do
            num="${SCENARIO_NJOBS[$type]}"
            WORKLOADS+=("${rho}|data_generation/data/${day}_${type}_${num}_rho${rho}.json")
        done
    done
done

# Directories & templates
DEC_TEMPLATE=experiments/test_decentralized.json
CENT_TEMPLATE=experiments/test_centralized.json
RESULT_DIR=results
RESULT_DIR_CENTRALIZED=results/centralized
SIM_LOG_DIR=logs/simulator

mkdir -p "$RESULT_DIR"
mkdir -p "$RESULT_DIR_CENTRALIZED"
mkdir -p "$SIM_LOG_DIR"

FAILURE_COUNT=0

# Bidding strategies to evaluate
PYTHON_BIDDERS=(
    # "python_scripts/HeuristicBidding.py"
    "python_scripts/EmbeddingBidding.py"
    # "python_scripts/llm_claude_bidder.py"
)

EMBEDDING_STATIC_BID_WEIGHTS=(0.0 0.2 0.5)

BASELINE_POLICIES=(
    # "RandomBidding"
    # "PureLocal"
)

python_bidder_label() {
    local bidder="$1"
    local bidder_name

    bidder_name=$(basename "$bidder" .py)
    case "$bidder_name" in
        llm_claude_bidder)
            echo "LLMBidding"
            ;;
        *)
            echo "$bidder_name"
            ;;
    esac
}

run_simulation_capture() {
    local temp_json="$1"
    local output_file="$2"
    local stderr_file="$3"
    local temp_output status first_line

    mkdir -p "$(dirname "$output_file")"
    mkdir -p "$(dirname "$stderr_file")"
    temp_output=$(mktemp "${output_file}.tmp.XXXXXX")

    if "$EXEC_FILE" "$temp_json" $WRENCH_ARGS > "$temp_output" 2> "$stderr_file"; then
        if [[ ! -s "$temp_output" ]]; then
            echo "ERROR: Simulation produced an empty CSV: $output_file" >&2
            echo "       Stderr log: $stderr_file" >&2
            rm -f "$temp_output"
            return 1
        fi
        IFS= read -r first_line < "$temp_output"
        if [[ "$first_line" != JobId,* ]]; then
            echo "ERROR: Simulation output did not start with the expected CSV header: $output_file" >&2
            echo "       First line: $first_line" >&2
            echo "       Stderr log: $stderr_file" >&2
            rm -f "$temp_output"
            return 1
        fi
        mv "$temp_output" "$output_file"
        return 0
    else
        status=$?
        echo "ERROR: Simulation failed with exit code $status: $output_file" >&2
        echo "       Stderr log: $stderr_file" >&2
        rm -f "$temp_output"
        return "$status"
    fi
}


run_decentralized_python() {
    local workload="$1"
    local platform_file="$2"
    local bidder="$3"
    local workload_name="$4"
    local bidder_name output_file stderr_file llm_log_file temp_json status

    bidder_name=$(python_bidder_label "$bidder")
    output_file="$RESULT_DIR/${workload_name}_${bidder_name}.csv"
    stderr_file="$SIM_LOG_DIR/${workload_name}_${bidder_name}.stderr.log"
    llm_log_file="logs/llm_bidding/${workload_name}_${bidder_name}.log"
    temp_json=$(mktemp /tmp/swarm_dec_py_XXXXXX.json)

    jq \
        --arg workload "$workload" \
        --arg bidder "$bidder" \
        --arg platform "$platform_file" \
        '
        .workload = $workload |
        .platform = $platform |
        .decentralized_policy = "PythonBidding" |
        .decentralized_bidder = $bidder
        ' "$DEC_TEMPLATE" > "$temp_json"

    echo "Running DECENTRALIZED - $bidder_name on $workload_name"
    mkdir -p "$(dirname "$llm_log_file")"
    : > "$llm_log_file"
    export LLM_WORKLOAD_NAME="$workload_name"
    export LLM_LOG_DIR="logs/llm_bidding"
    export LLM_LOG_FILE="$llm_log_file"
    if run_simulation_capture "$temp_json" "$output_file" "$stderr_file"; then
        status=0
    else
        status=$?
    fi
    rm -f "$temp_json"
    return "$status"
}


run_decentralized_baseline() {
    local workload="$1"
    local platform_file="$2"
    local policy="$3"
    local workload_name="$4"
    local output_file stderr_file temp_json status

    output_file="$RESULT_DIR/${workload_name}_${policy}.csv"
    stderr_file="$SIM_LOG_DIR/${workload_name}_${policy}.stderr.log"
    temp_json=$(mktemp /tmp/swarm_dec_base_XXXXXX.json)

    jq \
        --arg workload "$workload" \
        --arg policy "$policy" \
        --arg platform "$platform_file" \
        '
        .workload = $workload |
        .platform = $platform |
        .decentralized_policy = $policy |
        del(.decentralized_bidder)
        ' "$DEC_TEMPLATE" > "$temp_json"

    echo "Running DECENTRALIZED - $policy on $workload_name"
    if run_simulation_capture "$temp_json" "$output_file" "$stderr_file"; then
        status=0
    else
        status=$?
    fi
    rm -f "$temp_json"
    return "$status"
}


run_centralized() {
    local workload="$1"
    local platform_file="$2"
    local bidder="$3"
    local workload_name="$4"
    local static_weight="${5:-}"
    local bidder_name output_file stderr_file llm_log_file temp_json status result_suffix

    bidder_name=$(python_bidder_label "$bidder")
    result_suffix=""
    if [[ "$bidder_name" == "EmbeddingBidding" && -n "$static_weight" ]]; then
        result_suffix="_static${static_weight}"
        export EMBEDDING_STATIC_BID_WEIGHT="$static_weight"
    else
        unset EMBEDDING_STATIC_BID_WEIGHT
    fi

    output_file="$RESULT_DIR_CENTRALIZED/${workload_name}${result_suffix}_${bidder_name}.csv"
    stderr_file="$SIM_LOG_DIR/centralized_${workload_name}${result_suffix}_${bidder_name}.stderr.log"
    llm_log_file="logs/llm_bidding/centralized_${workload_name}${result_suffix}_${bidder_name}.log"
    temp_json=$(mktemp /tmp/swarm_cent_XXXXXX.json)

    jq \
        --arg workload "$workload" \
        --arg platform "$platform_file" \
        --arg bidder "$bidder" \
        '
        .workload = $workload |
        .platform = $platform |
        .centralized_submission = true |
        .centralized_policy = $bidder |
        del(.centralized_bidder)
        ' "$CENT_TEMPLATE" > "$temp_json"

    echo "Running CENTRALIZED - $bidder_name on $workload_name"
    mkdir -p "$(dirname "$llm_log_file")"
    : > "$llm_log_file"
    export LLM_WORKLOAD_NAME="$workload_name"
    export LLM_LOG_DIR="logs/llm_bidding"
    export LLM_LOG_FILE="$llm_log_file"
    if run_simulation_capture "$temp_json" "$output_file" "$stderr_file"; then
        status=0
    else
        status=$?
    fi
    rm -f "$temp_json"
    return "$status"
}


throttle_jobs() {
    while [ "$(jobs -rp | wc -l)" -ge "$MAX_JOBS" ]; do
        if ! wait -n; then
            FAILURE_COUNT=$((FAILURE_COUNT + 1))
        fi
    done
}


# ==============
# Main loop
# ==============
platform_file="platforms/AmSC.xml"

for entry in "${WORKLOADS[@]}"; do
    rho="${entry%%|*}"
    workload="${entry#*|}"
    workload_name=$(basename "${workload}" .json)

    # Verify workload file exists before launching
    if [[ ! -f "$workload" ]]; then
        echo "WARNING: Workload file not found, skipping: $workload"
        continue
    fi

    echo "==============================================="
    echo "Workload: $workload_name"
    echo "Using platform: $platform_file"
    echo "==============================================="

    # for bidder in "${PYTHON_BIDDERS[@]}"; do
    #     bidder_name=$(python_bidder_label "$bidder")

    #     if [[ "$bidder_name" == *"llm"* || "$bidder" == *"llm_"* ]]; then
    #         throttle_jobs
    #         run_decentralized_python \
    #             "$workload" "$platform_file" "$bidder" "$workload_name" &

    #         echo "Sleeping ${SLEEP_BETWEEN_RUNS}s before the next LLM run..."
    #         sleep "$SLEEP_BETWEEN_RUNS"
    #     elif [[ "$bidder_name" == "EmbeddingBidding" ]]; then
    #         for static_weight in "${EMBEDDING_STATIC_BID_WEIGHTS[@]}"; do
    #             throttle_jobs
    #             EMBEDDING_STATIC_BID_WEIGHT="$static_weight" \
    #                 run_decentralized_python \
    #                 "$workload" "$platform_file" "$bidder" \
    #                 "${workload_name}_static${static_weight}" &
    #         done
    #     else
    #         throttle_jobs
    #         run_decentralized_python \
    #             "$workload" "$platform_file" "$bidder" "$workload_name" &
    #     fi
    # done

    # for policy in "${BASELINE_POLICIES[@]}"; do
    #     throttle_jobs
    #     run_decentralized_baseline "$workload" "$platform_file" "$policy" "$workload_name" &
    # done

    for bidder in "${PYTHON_BIDDERS[@]}"; do
        if [[ "$(python_bidder_label "$bidder")" == "EmbeddingBidding" ]]; then
            for static_weight in "${EMBEDDING_STATIC_BID_WEIGHTS[@]}"; do
                throttle_jobs
                run_centralized "$workload" "$platform_file" "$bidder" "$workload_name" "$static_weight" &
                wait -n || true
                echo "Sleeping ${SLEEP_BETWEEN_RUNS}s before the next centralized run..."
                sleep "$SLEEP_BETWEEN_RUNS"
            done
        else
            throttle_jobs
            run_centralized "$workload" "$platform_file" "$bidder" "$workload_name" &
            wait -n || true
            echo "Sleeping ${SLEEP_BETWEEN_RUNS}s before the next centralized run..."
            sleep "$SLEEP_BETWEEN_RUNS"
        fi
    done

    # throttle_jobs
    # # run_centralized "$workload" "$platform_file" "HeuristicBidding" "$workload_name" &
    # run_centralized "$workload" "$platform_file" "EmbeddingBidding" "$workload_name" &
done

while [ "$(jobs -rp | wc -l)" -gt 0 ]; do
    if ! wait -n; then
        FAILURE_COUNT=$((FAILURE_COUNT + 1))
    fi
done

if [[ "$FAILURE_COUNT" -gt 0 ]]; then
    echo "Completed with $FAILURE_COUNT failed experiment(s). Check $SIM_LOG_DIR for stderr logs." >&2
    exit 1
fi

echo "All experiments completed."
