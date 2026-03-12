#!/usr/bin/env python3
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
import seaborn as sns


REQUIRED_COLUMNS = ["SubmissionTime", "HPCSite", "HPCSystem", "Nodes", "Walltime", "JobType", "RequestedGPU"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a single figure with three workload plots: submission time by HPC site "
            "(with min/peak/max in seconds), nodes distribution, and walltime distribution."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to workload JSON file (array of job records).",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="plots_workloads/workload_single_figure.png",
        help="Output image path (default: plots_workloads/workload_single_figure.png).",
    )
    parser.add_argument(
        "--submission-bins",
        type=int,
        default=40,
        help="Number of bins for submission time histogram (default: 40).",
    )
    parser.add_argument(
        "--nodes-bins",
        type=int,
        default=30,
        help="Number of bins for nodes histogram (default: 30).",
    )
    parser.add_argument(
        "--walltime-bins",
        type=int,
        default=30,
        help="Number of bins for walltime histogram (default: 30).",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display the plot window in addition to saving the figure.",
    )
    return parser.parse_args()


def load_workload(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input workload file not found: {path}")

    df = pd.read_json(path)
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return df


def _site_hist_stats(series: pd.Series, bins: int) -> tuple[float, float, float]:
    values = series.dropna().to_numpy(dtype=float)
    if values.size == 0:
        return float("nan"), float("nan"), float("nan")

    hist, edges = np.histogram(values, bins=bins)
    peak_idx = int(np.argmax(hist))
    peak_center = (edges[peak_idx] + edges[peak_idx + 1]) / 2.0
    return float(np.min(values)), float(peak_center), float(np.max(values))


def create_figure(
    df: pd.DataFrame,
    output_path: Path,
    submission_bins: int,
    nodes_bins: int,
    walltime_bins: int,
    show: bool,
    workload: str,
) -> None:
    sns.set_theme(style="whitegrid")
    output_path = Path(f"plots_workloads/{Path(workload).stem}.png")

    fig, axes = plt.subplots(1, 3, figsize=(22, 6))

    # Panel 1: Submission time histogram by site with min/peak/max summary.
    ax0 = axes[0]
    site_order = sorted(df["HPCSite"].dropna().astype(str).unique())
    palette = sns.color_palette("tab10", n_colors=max(1, len(site_order)))
    site_to_color = {site: palette[i] for i, site in enumerate(site_order)}

    site_stats_lines = []
    for i, site in enumerate(site_order):
        site_df = df[df["HPCSite"] == site]
        sns.histplot(
            site_df["SubmissionTime"],
            bins=submission_bins,
            stat="count",
            alpha=0.35,
            ax=ax0,
            color=site_to_color[site],
            label=site,
            element="bars",
            # kde=True,
        )

        s_min, s_peak, s_max = _site_hist_stats(site_df["SubmissionTime"], submission_bins)
        site_stats_lines.append(
            f"{site}: min={s_min:.1f}s, peak={s_peak:.1f}s, max={s_max:.1f}s"
        )

    ax0.set_title("Submission Time by HPC Site")
    ax0.set_xlabel("Submission Time (seconds)")
    ax0.set_ylabel("Job Count")
    ax0.text(
        0.01,
        0.99,
        "\n".join(site_stats_lines),
        transform=ax0.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.85),
    )

    # Panel 2: Nodes distribution.
    # ax1 = axes[1]
    # sns.histplot(df["Nodes"], bins=nodes_bins, kde=True, ax=ax1, color="seagreen")
    # ax1.set_title("Nodes Distribution")
    # ax1.set_xlabel("Requested Nodes")
    # ax1.set_ylabel("Job Count")

    # # Panel 3: Walltime distribution.
    # ax2 = axes[2]
    # sns.histplot(df["Walltime"], bins=walltime_bins, kde=True, ax=ax2, color="tomato")
    # ax2.set_title("Walltime Distribution")
    # ax2.set_xlabel("Walltime (seconds)")
    # ax2.set_ylabel("Job Count")

    # scatter plot of Nodes vs Walltime
    ax1 = axes[1]
    sns.scatterplot(
        data=df,
        y="Nodes",
        x="Walltime",
        hue="HPCSite",
        hue_order=site_order,
        palette=site_to_color,
        ax=ax1,
        alpha=0.7,
        edgecolor=None,
    )
    ax1.set_title("Nodes vs Walltime")
    ax1.set_ylabel("Requested Nodes")
    ax1.set_xlabel("Walltime (seconds)")
    if ax1.get_legend() is not None:
        ax1.get_legend().remove()

    # Panel 3: job type, requested gpu, distribution.
    ax2 = axes[2]
    # df['RequestedGPU'] = df['RequestedGPU'].astype(int)
    # sns.histplot(
    #     data=df,
    #     x="JobType",
    #     weights="RequestedGPU",
    #     stat="frequency",
    #     hue="HPCSite",
    #     palette=palette,
    #     ax=ax2,
    #     alpha=0.75,        shrink=.8, multiple="dodge", discrete=True, legend=True,
    # )
    # ax2.set_title("Job Type Distribution")
    # ax2.set_xlabel("Job Type")
    # ax2.set_ylabel("Total Requested GPU")
    # # ax2.legend(title="HPC Site", loc="upper right")
    # # Move seaborn-created legend instead of recreating it
    # sns.move_legend(
    #     ax2,
    #     "upper left",
    #     bbox_to_anchor=(1.02, 1),
    #     title="HPC Site",
    #     frameon=True,
    #     borderaxespad=0.0,
    # )
    plot_df = df.copy()
    plot_df["JobType"] = plot_df["JobType"].astype(str)
    plot_df["HPCSite"] = plot_df["HPCSite"].astype(str)
    plot_df["HPCSystem"] = plot_df["HPCSystem"].astype(str)
    plot_df["RequestedGPU"] = plot_df["RequestedGPU"].astype(bool)

    # counts per JobType + Site + GPU flag
    agg = (
        plot_df.groupby(["JobType", "HPCSite", "HPCSystem", "RequestedGPU"])
        .size()
        .unstack(fill_value=0)
        .rename(columns={False: "CPU", True: "GPU"})
        .reset_index()
    )
    job_order = sorted(plot_df["JobType"].unique())
    site_order = sorted(plot_df["HPCSite"].unique())
    pair_order = sorted(plot_df[["HPCSite", "HPCSystem"]].drop_duplicates().itertuples(index=False, name=None))

    # Keep HPCSystem legend/hatch order aligned with actual bar drawing order.
    system_order = list(dict.fromkeys(system for _, system in pair_order))

    hatches = ["", "//", "\\\\", "xx", "..", "++", "oo", "**"]
    system_to_hatch = {sys: hatches[i % len(hatches)] for i, sys in enumerate(system_order)}
    system_display_name = {
        "Perlmutter-Phase-1": "P-1",
        "Perlmutter-Phase-2": "P-2",
    }


    x = np.arange(len(job_order))
    group_width = 0.8
    bar_w = group_width / max(1, len(pair_order))
    
    for i, (site, system) in enumerate(pair_order):
        sub = (
            agg[(agg["HPCSite"] == site) & (agg["HPCSystem"] == system)]
            [["JobType", "CPU", "GPU"]]          # keep numeric cols only (+ index col)
            .set_index("JobType")
            .reindex(job_order, fill_value=0)    # now safe
        )
        cpu = sub["CPU"].to_numpy()
        gpu = sub["GPU"].to_numpy()

        xpos = x - group_width / 2 + i * bar_w + bar_w / 2
        color = site_to_color[site]
        hatch = system_to_hatch[system]

        ax2.bar(xpos, cpu, width=bar_w, color=color, alpha=0.35, hatch=hatch)
        ax2.bar(xpos, gpu, width=bar_w, bottom=cpu, color=color, alpha=0.9, hatch=hatch)

    ax2.set_title("Job Type Distribution (Site/System + CPU/GPU)")
    ax2.set_xlabel("Job Type")
    ax2.set_ylabel("Job Count")
    ax2.set_xticks(x)
    ax2.set_xticklabels(job_order)

    # legends for ax2-specific encodings
    system_handles = [
        Patch(
            facecolor="white",
            edgecolor="black",
            hatch=system_to_hatch[sys],
            label=system_display_name.get(sys, sys),
        )
        for sys in system_order
    ]
    leg2 = ax2.legend(handles=system_handles, title="HPC System", loc="upper left", bbox_to_anchor=(1.02, 0.72), frameon=True, fontsize=7)
    ax2.add_artist(leg2)

    split_handles = [
        Patch(facecolor="gray", alpha=0.35, label="CPU"),
        Patch(facecolor="gray", alpha=0.9, label="GPU"),
    ]
    ax2.legend(handles=split_handles, title="Split", loc="upper left", bbox_to_anchor=(1.02, 0.30), frameon=True)

    # common legend for HPCSite across all 3 plots
    site_handles = [Patch(facecolor=site_to_color[s], edgecolor="none", label=s) for s in site_order]
    fig.legend(
        handles=site_handles,
        title="HPC Site",
        loc="lower center",
        bbox_to_anchor=(0.5, -0.02),
        ncol=max(1, min(6, len(site_order))),
        frameon=True,
    )

    fig.suptitle(f"Workload Overview {workload}", fontsize=14, y=1.02)
    fig.tight_layout(rect=(0, 0.07, 0.86, 1))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Saved figure to: {output_path}")

    if show:
        plt.show()

    plt.close(fig)


    # create new plot with just submission time histogram by site
    fig2, ax = plt.subplots(figsize=(8, 6))
    # convert seconds to hours for better readability
    df["SubmissionTimeHours"] = df["SubmissionTime"] / 3600.0
    for site in site_order:
        site_df = df[df["HPCSite"] == site]
        sns.histplot(
            site_df["SubmissionTimeHours"],
            bins=submission_bins,
            stat="count",
            alpha=0.35,
            ax=ax,
            color=site_to_color[site],
            label=site,
            element="bars",)

    # ax.set_title("Submission Time by HPC Site (Hours)")
    ax.set_xlabel("Submission Time (hours)")
    ax.set_ylabel("Job Count")

    if "busy" in workload.lower():
        ax.set_xticks(np.arange(0, 28, 3))
        ax.set_xlim(0, 27)
    else:
        ax.set_xticks(np.arange(0, 25, 3))
        ax.set_xlim(0, 24)
    
    ax.legend(title="HPC Site")
    fig2.tight_layout()
    output_path2 = output_path.parent / f"{output_path.stem}_submission_time_histogram.png"
    fig2.savefig(output_path2, dpi=300, bbox_inches="tight")
    print(f"Saved submission time histogram to: {output_path2}")


def main() -> None:
    args = parse_args()
    df = load_workload(Path(args.input))
    create_figure(
        df=df,
        output_path=Path(args.output),
        submission_bins=args.submission_bins,
        nodes_bins=args.nodes_bins,
        walltime_bins=args.walltime_bins,
        show=args.show,
        workload=args.input,

    )


if __name__ == "__main__":
    main()
