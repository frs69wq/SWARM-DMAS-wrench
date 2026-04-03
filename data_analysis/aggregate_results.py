import pandas as pd
from pathlib import Path

NUM_JOBS = [2000]
R_VALUES = [16]

RESULTS_DIR = Path(f"results/sfactor_{R_VALUES[0]}")
CENTRALIZED_DIR = RESULTS_DIR / "centralized"
OUTPUT_FILE = RESULTS_DIR / "aggregated_metrics.csv"

DAYS = ["busy", "bursty_low_stress", "bursty_high_stress"]
TYPES = ["homogeneous_short", "only_large_long", "mixed_80_20", "mixed_20_80"]

PYTHON_BIDDERS = ["HeuristicBidding", "EmbeddingBidding"]
BASELINE_POLICIES = ["RandomBidding", "PureLocal"]
ALL_STRATEGIES = PYTHON_BIDDERS + BASELINE_POLICIES


def _to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def filter_valid_jobs(df):
    """Remove rejected / failed / unscheduled jobs."""
    filtered = df.copy()

    if "FailureCause" in filtered.columns:
        fc = filtered["FailureCause"].astype("string")
        fc_norm = fc.str.strip().str.lower()
        valid = fc_norm.isna() | fc_norm.isin(["", "none"])
        filtered = filtered[valid]

    if "ScheduledOn" in filtered.columns:
        filtered = filtered[filtered["ScheduledOn"].notna()]

    if "ExecutionTime" in filtered.columns:
        filtered = filtered[filtered["ExecutionTime"] > 0]

    if "EndTime" in filtered.columns:
        filtered = filtered[filtered["EndTime"].notna()]

    return filtered


def mean_std(series):
    s = series.dropna()
    if s.empty:
        return None, None
    return float(s.mean()), float(s.std(ddof=1)) if len(s) > 1 else 0.0


def calculate_metrics(csv_path):

    raw_df = pd.read_csv(csv_path)

    time_cols = [
        "SubmissionTime",
        "SchedulingTime",
        "StartTime",
        "EndTime",
        "DecisionTime",
        "WaitingTime",
        "ExecutionTime",
    ]

    raw_df = _to_numeric(raw_df, time_cols)

    total_jobs = len(raw_df)

    df = filter_valid_jobs(raw_df)

    completed_jobs = len(df)
    failed_jobs = total_jobs - completed_jobs
    completion_ratio = completed_jobs / total_jobs if total_jobs > 0 else None

    if "TurnaroundTime" not in df.columns and {"EndTime","SubmissionTime"}.issubset(df.columns):
        df["TurnaroundTime"] = df["EndTime"] - df["SubmissionTime"]

    # Makespan
    makespan_minutes = None
    throughput_jobs_per_hour = None

    if not df.empty and {"EndTime","SubmissionTime"}.issubset(df.columns):

        min_submit = df["SubmissionTime"].min()
        max_end = df["EndTime"].max()

        if pd.notna(min_submit) and pd.notna(max_end):

            makespan_seconds = max_end - min_submit
            makespan_minutes = makespan_seconds / 60

            if makespan_seconds > 0:
                throughput_jobs_per_hour = completed_jobs / (makespan_seconds / 3600)

    turnaround_mean, turnaround_std = mean_std(df["TurnaroundTime"]) if "TurnaroundTime" in df else (None,None)
    waiting_mean, waiting_std = mean_std(df["WaitingTime"]) if "WaitingTime" in df else (None,None)
    execution_mean, execution_std = mean_std(df["ExecutionTime"]) if "ExecutionTime" in df else (None,None)
    decision_mean, decision_std = mean_std(df["DecisionTime"]) if "DecisionTime" in df else (None,None)

    return {
        "total_jobs": total_jobs,
        "completed_jobs": completed_jobs,
        "failed_jobs": failed_jobs,
        "completion_ratio": completion_ratio,
        "makespan_minutes": makespan_minutes,
        "throughput_jobs_per_hour": throughput_jobs_per_hour,
        "turnaround_mean": turnaround_mean,
        "turnaround_std": turnaround_std,
        "waiting_mean": waiting_mean,
        "waiting_std": waiting_std,
        "execution_mean": execution_mean,
        "execution_std": execution_std,
        "decision_mean": decision_mean,
        "decision_std": decision_std,
    }


def main():

    rows = []

    for day in DAYS:

        for num_jobs, r in zip(NUM_JOBS, R_VALUES):

            for workload_type in TYPES:

                workload_name = f"{day}_{workload_type}_{num_jobs}_r{r}"

                for mode, base_dir in [
                    ("decentralized", RESULTS_DIR),
                    ("centralized", CENTRALIZED_DIR),
                ]:

                    for strategy in ALL_STRATEGIES:

                        csv_path = base_dir / f"{workload_name}_{strategy}.csv"

                        if not csv_path.exists():
                            print(f"Skipping missing file: {csv_path}")
                            continue

                        print(f"Processing {mode} - {csv_path.name}")

                        metrics = calculate_metrics(csv_path)

                        row = {
                            "mode": mode,
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

    column_order = [
        "mode","day","workload_type","num_jobs","strategy",
        "total_jobs","completed_jobs","failed_jobs","completion_ratio",
        "makespan_minutes","throughput_jobs_per_hour",
        "turnaround_mean","turnaround_std",
        "waiting_mean","waiting_std",
        "execution_mean","execution_std",
        "decision_mean","decision_std"
    ]

    df_summary = df_summary[column_order]

    df_summary.sort_values(
        by=["mode","day","workload_type","num_jobs","strategy"],
        inplace=True
    )

    # Compute percent change vs PureLocal for key metrics
    for metric in ["makespan_minutes", "waiting_mean", "execution_mean", "turnaround_mean"]:
        pct_col = f"{metric}_pct_vs_purelocal"
        # Find PureLocal baseline for each group
        purelocal = df_summary[df_summary["strategy"] == "PureLocal"]
        # Merge to align each row with its PureLocal baseline
        merged = df_summary.merge(
            purelocal[["mode", "day", "workload_type", "num_jobs", metric]],
            on=["mode", "day", "workload_type", "num_jobs"],
            suffixes=("", "_purelocal"),
            how="left"
        )

        # add arrow if increase/decrease
        def format_pct_change(row):
            if pd.isna(row[f"{metric}_purelocal"]):
                return None
            change = row[metric] - row[f"{metric}_purelocal"]
            arrow = "↑" if change > 0 else "↓" if change < 0 else ""
            return f"{row[pct_col]}% {arrow}"

        # Compute percent change
        merged[pct_col] = ((merged[metric] - merged[f"{metric}_purelocal"]) / merged[f"{metric}_purelocal"] * 100).round(2)
        merged[pct_col] = merged.apply(format_pct_change, axis=1)
        df_summary[pct_col] = merged[pct_col]

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_summary.to_csv(OUTPUT_FILE, index=False)

    print("\nAggregated Metrics Table:\n")
    print(df_summary.to_string(index=False))

    print(f"\nSaved aggregated metrics to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
