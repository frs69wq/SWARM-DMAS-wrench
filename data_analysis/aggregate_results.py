import pandas as pd
from pathlib import Path

RESULTS_DIR = Path("results")
OUTPUT_FILE = RESULTS_DIR / "aggregated_metrics.csv"

# workload files
DAYS = ["idle", "busy"]
TYPES = ["homogeneous_short", "only_large_long", "mixed_80_20", "mixed_20_80"]
PYTHON_BIDDERS = ["HeuristicBidding", "EmbeddingBidding"]  # ,"llm_claude_bidder"
BASELINE_POLICIES = ["RandomBidding", "PureLocal"]
ALL_STRATEGIES = PYTHON_BIDDERS + BASELINE_POLICIES


def _to_numeric(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df



def _mean_std(series: pd.Series, prefix: str):
    s = series.dropna()
    if s.empty:
        return {
            f"{prefix}_mean": None,
            f"{prefix}_std": None,
        }

    return {
        f"{prefix}_mean": float(s.mean()),
        f"{prefix}_std": float(s.std(ddof=1)) if len(s) > 1 else 0.0,
    }


def calculate_metrics(csv_path: Path):
    df = pd.read_csv(csv_path)

    time_cols = [
        "SubmissionTime", "SchedulingTime", "StartTime", "EndTime",
        "DecisionTime", "WaitingTime", "ExecutionTime",
    ]
    df = _to_numeric(df, time_cols)

    total_jobs = len(df)

    # Completion stats
    completed_jobs = None
    failed_jobs = None
    completion_ratio = None

    if "FailureCause" in df.columns:
        fc = df["FailureCause"]
        fc_str = fc.astype("string")  
        fc_norm = fc_str.str.strip().str.lower()
        no_failure = fc_norm.isna() | fc_norm.isin(["none", ""])

        completed_jobs = int(no_failure.sum())
        failed_jobs = total_jobs - completed_jobs
        completion_ratio = completed_jobs / total_jobs if total_jobs > 0 else None


    # Turnaround time
    if "TurnaroundTime" not in df.columns and {"EndTime", "SubmissionTime"}.issubset(df.columns):
        df["TurnaroundTime"] = df["EndTime"] - df["SubmissionTime"]

    # Makespan and throughput
    makespan = None
    throughput_jobs_per_hour = None
    if {"EndTime", "SubmissionTime"}.issubset(df.columns):
        min_submit = df["SubmissionTime"].min()
        max_end = df["EndTime"].max()
        if pd.notna(min_submit) and pd.notna(max_end):
            makespan = float(max_end - min_submit) 
            if makespan > 0:
                throughput_jobs_per_hour = total_jobs / (makespan / 60.0)

    metrics = {
        "total_jobs": total_jobs,
        "completed_jobs": completed_jobs,
        "failed_jobs": failed_jobs,
        "completion_ratio": completion_ratio,
        "makespan_minutes": makespan,
        "throughput_jobs_per_hour": throughput_jobs_per_hour,
    }

    # Aggregate stats (mean/std)
    if "TurnaroundTime" in df.columns:
        metrics.update(_mean_std(df["TurnaroundTime"], "turnaround"))

    if "WaitingTime" in df.columns:
        metrics.update(_mean_std(df["WaitingTime"], "waiting"))

    if "ExecutionTime" in df.columns:
        metrics.update(_mean_std(df["ExecutionTime"], "execution"))

    if "DecisionTime" in df.columns:
        metrics.update(_mean_std(df["DecisionTime"], "decision"))

    return metrics


def main():
    rows = []

    for day in DAYS:
        num_jobs = 100 if day == "idle" else 700

        for workload_type in TYPES:
            workload_name = f"{day}_{workload_type}_{num_jobs}"

            for strategy in ALL_STRATEGIES:
                csv_path = RESULTS_DIR / f"{workload_name}_{strategy}.csv"

                if not csv_path.exists():
                    print(f"Skipping missing file: {csv_path.name}")
                    continue

                print(f"Processing {csv_path.name}")

                metrics = calculate_metrics(csv_path)

                row = {
                    "day": day,
                    "workload_type": workload_type,
                    "num_jobs": num_jobs,
                    "strategy": strategy,
                    **metrics,
                }
                rows.append(row)

    if not rows:
        print("No matching CSV files found.")
        return

    df_summary = pd.DataFrame(rows)

    df_summary.sort_values(
        by=["day", "workload_type", "num_jobs", "strategy"],
        inplace=True,
    )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    df_summary.to_csv(OUTPUT_FILE, index=False)

    print("\nAggregated Metrics Table:\n")
    print(df_summary.to_string(index=False))
    print(f"\nSaved aggregated metrics to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()