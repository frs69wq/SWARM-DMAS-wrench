"""
Sensitivity of embedding-based bidding to the static weight lambda in
    b(j,s) = lambda * b_static(j,s) + (1 - lambda) * b_dynamic(j,s)

Reads the decentralized results of EmbeddingBidding for every lambda value
found in the file names (..._static<lambda>_EmbeddingBidding.csv) and reports,
for each lambda:
  - mean of the per-scenario mean turnaround (hours), overall and per workload type
  - median of the per-scenario mean turnaround (hours)
  - number of scenarios where this lambda yields the lowest mean turnaround
  - mean waiting and execution times (hours), averaged over scenarios
  - share of jobs placed on the CPU-only systems (Crux, Perlmutter-2, Andes)
    and on Aurora + Frontier, per workload type

Used for the lambda-sensitivity table of Section VI.
"""

import glob
import os
import re
import statistics
from collections import defaultdict

import pandas as pd

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
PATTERN = re.compile(r"(.+)_rho([\d.]+)_static([\d.]+)_EmbeddingBidding\.csv")
CPU_ONLY = {"Crux", "Perlmutter-Phase-2", "Andes"}
BIG_GPU = {"Aurora", "Frontier"}


def workload_type(scenario):
    if "small_short" in scenario:
        return "Small/Short"
    if "large_long" in scenario:
        return "Large/Long"
    return "Mixed"


rows = []
for f in sorted(glob.glob(os.path.join(RESULTS_DIR, "*_EmbeddingBidding.csv"))):
    m = PATTERN.fullmatch(os.path.basename(f))
    if not m:
        continue
    prefix, rho, lam = m.groups()
    df = pd.read_csv(f)
    df = df[df.FinalStatus == "COMPLETED"]
    rows.append(dict(
        scenario=f"{prefix}_rho{rho}",
        wtype=workload_type(prefix),
        lam=float(lam),
        ta=((df.EndTime - df.SubmissionTime) / 3600).mean(),
        wait=((df.StartTime - df.SubmissionTime) / 3600).mean(),
        exe=(df.ExecutionTime / 3600).mean(),
        cpu_jobs=df.ScheduledOn.isin(CPU_ONLY).sum(),
        gpu_jobs=df.ScheduledOn.isin(BIG_GPU).sum(),
        jobs=len(df),
    ))

data = pd.DataFrame(rows)
lams = sorted(data.lam.unique())

best = data.loc[data.groupby("scenario").ta.idxmin()].lam.value_counts()

print("=== Overall (per-scenario means, hours) ===")
print(f"{'lambda':>6} {'n':>3} {'meanTA':>7} {'medTA':>7} {'wait':>6} {'exec':>6} {'best in':>8}")
for lam in lams:
    d = data[data.lam == lam]
    print(f"{lam:>6.1f} {len(d):>3} {d.ta.mean():>7.2f} {d.ta.median():>7.2f} "
          f"{d.wait.mean():>6.2f} {d.exe.mean():>6.2f} {int(best.get(lam, 0)):>8}")

print("\n=== Mean turnaround by workload type (hours) ===")
wtypes = ["Small/Short", "Mixed", "Large/Long"]
print(f"{'lambda':>6} " + " ".join(f"{w:>12}" for w in wtypes))
for lam in lams:
    d = data[data.lam == lam]
    print(f"{lam:>6.1f} " + " ".join(f"{d[d.wtype == w].ta.mean():>12.2f}" for w in wtypes))

print("\n=== Job placement shares by workload type (% of jobs) ===")
print(f"{'lambda':>6} {'workload':>12} {'CPU-only':>9} {'Aur+Fro':>8}")
for lam in lams:
    for w in wtypes:
        d = data[(data.lam == lam) & (data.wtype == w)]
        print(f"{lam:>6.1f} {w:>12} {100 * d.cpu_jobs.sum() / d.jobs.sum():>8.1f}% "
              f"{100 * d.gpu_jobs.sum() / d.jobs.sum():>7.1f}%")
