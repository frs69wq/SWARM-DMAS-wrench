#!/usr/bin/env python3
"""Compare job placement across independent decentralized LLM runs."""

# how to run


import re
import sys
from pathlib import Path

import pandas as pd

RESULTS_DIR = Path(__file__).resolve().parents[1] / "results_consistency"
PATTERN = re.compile(
    r"(?P<scenario>"
    r"(?P<arrival>bursty_high_stress|bursty_low_stress|business)_"
    r"(?P<workload>small_short|large_long|mixed_80_20|mixed_20_80)_"
    r"(?P<jobs>\d+)_rho(?P<rho>[\d.]+)"
    r")_LLMBidding(?:_run(?P<run>[12]))?\.csv",
    re.IGNORECASE,
)
ARRIVALS = {
    "bursty_high_stress": "BHS",
    "bursty_low_stress": "BLS",
    "business": "BUSI",
}
WORKLOADS = {
    "small_short": "SS",
    "large_long": "LL",
    "mixed_80_20": "Mixed 80/20",
    "mixed_20_80": "Mixed 20/80",
}
REQUIRED = {"JobId", "ScheduledOn", "FinalStatus", "SubmissionTime", "EndTime"}


def load_run(path):
    frame = pd.read_csv(path, dtype={"JobId": "string", "ScheduledOn": "string"})
    missing = REQUIRED - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")

    frame["JobId"] = frame["JobId"].str.strip()
    frame["ScheduledOn"] = frame["ScheduledOn"].str.strip().replace("", pd.NA)

    if frame["JobId"].isna().any() or frame["JobId"].eq("").any():
        raise ValueError(f"{path.name}: missing JobId")
    if frame["JobId"].duplicated().any():
        raise ValueError(f"{path.name}: duplicate JobId values")
    return frame


def mean_turnaround(frame):
    completed = frame[frame["FinalStatus"].eq("COMPLETED")]
    end = pd.to_numeric(completed["EndTime"], errors="coerce")
    submission = pd.to_numeric(completed["SubmissionTime"], errors="coerce")
    return ((end - submission) / 3600).mean()



def collect_files(directory, expected_run):
    if not directory.is_dir():
        raise FileNotFoundError(f"Results directory not found: {directory}")

    files = {}
    for path in sorted(directory.iterdir()):
        match = PATTERN.fullmatch(path.name)
        if not path.is_file() or not match:
            continue
        if match["run"] != expected_run or float(match["rho"]) != 0.9:
            continue

        scenario = match["scenario"].lower()
        if scenario in files:
            raise ValueError(f"Duplicate scenario in {directory}: {scenario}")
        files[scenario] = (path, match)
    return files


def compare_pairs(first_files, second_files, first_label, second_label):
    rows = []
    for scenario in sorted(first_files.keys() | second_files.keys()):
        if scenario not in first_files or scenario not in second_files:
            missing = first_label if scenario not in first_files else second_label
            print(f"Skipping {scenario}: missing {missing}", file=sys.stderr)
            continue

        first_path, match = first_files[scenario]
        second_path, _ = second_files[scenario]
        try:
            first = load_run(first_path)
            second = load_run(second_path)
        except (ValueError, OSError, pd.errors.ParserError) as error:
            print(f"Skipping {scenario}: {error}", file=sys.stderr)
            continue

        paired = first[["JobId", "ScheduledOn"]].merge(
            second[["JobId", "ScheduledOn"]],
            on="JobId",
            how="inner",
            suffixes=("_1", "_2"),
            validate="one_to_one",
        )
        valid = paired.dropna(subset=["ScheduledOn_1", "ScheduledOn_2"])
        unmatched_first = len(first) - len(paired)
        unmatched_second = len(second) - len(paired)
        excluded = len(paired) - len(valid)
        if unmatched_first or unmatched_second or excluded:
            print(
                f"{scenario}: unmatched {first_label}={unmatched_first}, "
                f"{second_label}={unmatched_second}; "
                f"matched jobs without both placements={excluded}",
                file=sys.stderr,
            )

        agreement = valid["ScheduledOn_1"].eq(valid["ScheduledOn_2"])
        job1 = valid[valid["JobId"].eq("1")]
        rows.append({
            "scenario": scenario,
            "arrival": ARRIVALS[match["arrival"].lower()],
            "workload": WORKLOADS[match["workload"].lower()],
            "jobs": int(match["jobs"]),
            "rho": float(match["rho"]),
            "agreement_pct": 100 * agreement.mean() if len(valid) else float("nan"),
            "agreements": int(agreement.sum()),
            "compared_jobs": len(valid),
            f"{first_label}_turnaround_h": mean_turnaround(first),
            f"{second_label}_turnaround_h": mean_turnaround(second),
            "job1_agreement": (
                bool(job1["ScheduledOn_1"].iloc[0] == job1["ScheduledOn_2"].iloc[0])
                if len(job1) else pd.NA
            ),
        })

    columns = [
        "scenario", "arrival", "workload", "jobs", "rho",
        "agreement_pct", "agreements", "compared_jobs",
        f"{first_label}_turnaround_h", f"{second_label}_turnaround_h",
        "job1_agreement",
    ]
    return pd.DataFrame(rows, columns=columns)


def main():
    root = Path(__file__).resolve().parents[1]
    consistency = root / "results_consistency"
    results = root / "results"
    data_analysis = root / "data_analysis"

    dec1_dec2 = compare_pairs(
        collect_files(consistency, "1"),
        collect_files(consistency, "2"),
        "DEC1", "DEC2",
    )
    cen1_dec1 = compare_pairs(
        collect_files(results / "centralized", None),
        collect_files(results, None),
        "CEN1", "DEC1",
    )

    for title, frame, filename, first_label, second_label in [
    ("DEC1 vs DEC2", dec1_dec2, "dec1_dec2_agreement.csv", "DEC1", "DEC2"),
    ("CEN1 vs DEC1", cen1_dec1, "cen1_dec1_agreement.csv", "CEN1", "DEC1"),
]:
        output = frame.drop(columns="scenario")
        output.to_csv(data_analysis / filename, index=False)

        display = output.rename(columns={
            "arrival": "Arrival",
            "workload": "Workload",
            "jobs": "Jobs",
            "rho": "Rho",
            "agreement_pct": "Agreement (%)",
            "agreements": "Agreements",
            "compared_jobs": "Compared",
            f"{first_label}_turnaround_h": f"{first_label} TT (h)",
            f"{second_label}_turnaround_h": f"{second_label} TT (h)",
            "job1_agreement": "Job1_agreement",
        })
        print(f"\n=== {title} (rho=0.9) ===")
        print(display.to_string(index=False, na_rep="N/A",
                                float_format=lambda value: f"{value:.2f}"))
        print(f"Saved: {data_analysis / filename}")


        # print("\n=== Averages Across Scenarios ===")
    for title, frame, first_label, second_label in [
        ("DEC1 vs DEC2", dec1_dec2, "DEC1", "DEC2"),
        ("CEN1 vs DEC1", cen1_dec1, "CEN1", "DEC1"),
    ]:
        print(
            f"{title}: agreement = {frame['agreement_pct'].mean():.2f}%, "
            f"{first_label} TT = {frame[f'{first_label}_turnaround_h'].mean():.2f} h, "
            f"{second_label }TT = {frame[f'{second_label}_turnaround_h'].mean():.2f} h"
        )

if __name__ == "__main__":
    main()