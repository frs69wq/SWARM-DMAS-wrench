#!/usr/bin/env python3
"""
Analyze SWARM-DMAS simulation results from test.csv
- Calculate makespan metric
- Plot resource utilization for all systems

How to Run:
python ../data_analysis/analyze_results.py --workload workloads/test_workload.json
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
from pathlib import Path

def load_job_definitions(workload_path):
    """Load job definitions from workload JSON file."""
    with open(workload_path, 'r') as f:
        jobs = json.load(f)
    jobs_df = pd.DataFrame(jobs)
    # Ensure JobID column exists and select relevant columns
    if 'JobID' in jobs_df.columns:
        return jobs_df[['JobID', 'Nodes', 'MemoryGB', 'RequestedGPU', 'RequestedStorageGB', 
                        'JobType', 'Walltime']].copy()
    return jobs_df

def load_results(csv_path, jobs_df=None):
    """Load simulation results from CSV file and merge with job definitions."""
    df = pd.read_csv(csv_path)
        # rename jobid column 
    if jobs_df is not None and 'JobID' in jobs_df.columns:
        jobs_df.rename(columns={ 'JobID': 'JobId'}, inplace=True)
    
    # Check if 'JobID' exists in both DataFrames before merging
    if jobs_df is not None and 'JobId' in df.columns:
        df = pd.merge(df, jobs_df, on='JobId', how='left')
    else:
        print("Warning: 'JobID' column not found in one of the DataFrames. Skipping merge.")

    # add column in df for turnaround time if not already present
    if 'TurnaroundTime' not in df.columns and 'SubmissionTime' in df.columns and 'EndTime' in df.columns:
        df['TurnaroundTime'] = df['EndTime'] - df['SubmissionTime']

    return df

def calculate_makespan(df):
    """
    Calculate makespan: time from first job submission to last job completion.
    
    Makespan = max(EndTime) - min(SubmissionTime)
    """
    # print(df['EndTime'].max(), df['SubmissionTime'].min())
    makespan = df['EndTime'].max() - df['SubmissionTime'].min()
    
    return makespan

def calculate_throughput(df):
    """
    Calculate throughput: number of jobs completed per unit time.
    Throughput = Total Jobs / Makespan
    """
    total_jobs = len(df)
    makespan = calculate_makespan(df)  # in minutes
    throughput = total_jobs / makespan   
    return throughput

def calculate_resource_utilization(df):
    """
    Calculate resource utilization over time for total system.
    Utilization can be defined as (Nodes Used) / (Total Nodes Available) at any given time for total system.
    """
    # Calculate total node-minutes used across all jobs
    total_node_minutes = (df['Nodes'] * df['ExecutionTime']).sum()
    
    # Calculate makespan (total time window)
    total_time = calculate_makespan(df)
    
    # Total nodes across all systems
    total_nodes = 9472 + 704 + 10624 + 256 + 1536 + 3072  # Total nodes across all systems
    
    # Total available node-minutes in the system
    total_available_node_minutes = total_nodes * total_time
    
    # Node utilization as a percentage
    node_utilization = (total_node_minutes / total_available_node_minutes) * 100

    # Calculate total storage-GB-minutes used across all jobs
    total_storage_gb_minutes = (df['RequestedStorageGB'] * df['ExecutionTime']).sum()
    
    # Total storage capacity in GB (2 storage systems per site)
    total_storage_capacity = (2 * 220_000_000) + (2 * 35_000_000) + (2 * 700_000_000)
    
    # Total available storage-GB-minutes in the system
    total_available_storage_gb_minutes = total_storage_capacity * total_time
    
    # Storage utilization as a percentage
    storage_utilization = (total_storage_gb_minutes / total_available_storage_gb_minutes) * 100

    return node_utilization, storage_utilization

def plot_resource_utilization(df, output_path='resource_utilization.png'):
    """
    Plot resource utilization over time for all systems.
    """
    # Check if system information is available
    if 'ScheduledOn' not in df.columns:
        print("Error: 'ScheduledOn' column not found in CSV")
        return
    
    systems = df['ScheduledOn'].unique()
    
    fig, axes = plt.subplots(len(systems), 1, figsize=(14, 4 * len(systems)), sharex=True)
    if len(systems) == 1:
        axes = [axes]
    
    fig.suptitle('Resource Utilization Across Systems', fontsize=16, fontweight='bold')
    
    for idx, system in enumerate(systems):
        ax = axes[idx]
        system_df = df[df['ScheduledOn'] == system].copy()

        # Create timeline visualization
        for _, job in system_df.iterrows():
            start = job['StartTime']
            duration = job['ExecutionTime']
            
            ax.barh(y=0, width=duration, left=start,
                   alpha=0.6, edgecolor='black', linewidth=0.5)
        # x tick origin
        ax.set_xlim(0, df['EndTime'].max() * 1.1)
        # hide y ticks
        ax.set_yticks([])
        ax.set_ylabel(f'{system}', fontweight='bold')
        ax.set_title(f'{system} - {len(system_df)} jobs', fontweight='bold', fontsize=12)
        ax.grid(True, alpha=0.3, axis='x')
    
    axes[-1].set_xlabel('Execution Time (mins)', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Resource utilization plot saved to {output_path}")
    plt.close()

def plot_utilization_percentage(df, output_path='utilization_percentage.png'):
    """
    Plot resource utilization percentage and absolute nodes for each system over time.
    """
    if 'ScheduledOn' not in df.columns:
        print("Error: 'ScheduledOn' column not found")
        return
    
    # System capacities (from your site_configs)
    system_capacities = {
        'Frontier': 9472,
        'Andes': 704,
        'Aurora': 10624,
        'Crux': 256,
        'Perlmutter-Phase-1': 1536,
        'Perlmutter-Phase-2': 3072
    }
    
    systems = df['ScheduledOn'].unique()
    
    # Create 2 subplots: absolute nodes and normalized percentage
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    
    for system in systems:
        system_df = df[df['ScheduledOn'] == system].copy()
        
        if 'EndTime' not in system_df.columns and 'TurnaroundTime' in system_df.columns:
            system_df['EndTime'] = system_df['SubmissionTime'] + system_df['TurnaroundTime']
        
        if 'ExecutionTime' in system_df.columns:
            system_df['StartTime'] = system_df['EndTime'] - system_df['ExecutionTime']
        else:
            system_df['StartTime'] = system_df['SubmissionTime']
        
        # Create time bins
        max_time = df['EndTime'].max() if 'EndTime' in df.columns else df['SubmissionTime'].max()
        time_bins = np.linspace(0, max_time, 100)
        utilization_pct = []
        nodes_used_abs = []
        
        capacity = system_capacities.get(system, 1000)
        
        for t in time_bins:
            # Count nodes in use at time t
            active_jobs = system_df[(system_df['StartTime'] <= t) & 
                                   (system_df['EndTime'] >= t)]
            if 'Nodes' in active_jobs.columns:
                nodes_used = active_jobs['Nodes'].sum()
            else:
                # Fallback: count number of jobs if Nodes not available
                nodes_used = len(active_jobs) * 10  # Assume 10 nodes per job
            
            nodes_used_abs.append(nodes_used)
            util_pct = (nodes_used / capacity) * 100
            utilization_pct.append(util_pct)
        
        # Plot 1: Absolute nodes used
        ax1.plot(time_bins, nodes_used_abs, label=f'{system} (cap: {capacity})', 
                linewidth=2, marker='o', markersize=3, alpha=0.7)
        
        # Plot 2: Percentage utilization
        ax2.plot(time_bins, utilization_pct, label=system, linewidth=2, marker='o', 
               markersize=3, alpha=0.7)
    
    # Configure absolute nodes plot
    ax1.set_ylabel('Nodes Used (Absolute)', fontweight='bold', fontsize=12)
    ax1.set_title('Absolute Node Utilization Over Time', fontweight='bold', fontsize=14)
    ax1.legend(loc='best', fontsize=9)
    ax1.grid(True, alpha=0.3)
    
    # Configure percentage plot
    ax2.set_xlabel('Time (hours)', fontweight='bold', fontsize=12)
    ax2.set_ylabel('Utilization (%)', fontweight='bold', fontsize=12)
    ax2.set_title('Percentage Utilization (Relative to System Capacity)', fontweight='bold', fontsize=14)
    ax2.legend(loc='best', fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 100)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Utilization percentage plot saved to {output_path}")
    plt.close()

def calculate_per_system_metrics(df):
    """
    Calculate performance metrics for each system/machine.
    
    Returns:
        pd.DataFrame: DataFrame with metrics per system including:
            - Total Jobs Submitted
            - Total Jobs Scheduled
            - Makespan
            - Throughput
            - Node Utilization (%)
            - Storage Utilization (%)
            - Mean Turnaround Time
            - Mean Execution Time
            - Mean Decision Time
    """
    # System capacities
    system_capacities = {
        'Frontier': {'nodes': 9472, 'storage_gb': 220_000_000},
        'Andes': {'nodes': 704, 'storage_gb': 35_000_000},
        'Aurora': {'nodes': 10624, 'storage_gb': 700_000_000},
        'Crux': {'nodes': 256, 'storage_gb': 35_000_000},
        'Perlmutter-Phase-1': {'nodes': 1536, 'storage_gb': 220_000_000},
        'Perlmutter-Phase-2': {'nodes': 3072, 'storage_gb': 700_000_000}
    }
    
    if 'ScheduledOn' not in df.columns:
        print("Error: 'ScheduledOn' column not found")
        return None
    
    # Get all unique systems from both SubmittedOn and ScheduledOn
    systems = set()
    if 'SubmittedOn' in df.columns:
        systems.update(df['SubmittedOn'].unique())
    systems.update(df['ScheduledOn'].unique())
    
    metrics = []
    
    for system in sorted(systems):
        # Jobs submitted to this system

        jobs_submitted = 0
        if 'SubmittedTo' in df.columns:
            jobs_submitted = len(df[df['SubmittedTo'] == system])
        
        # Jobs scheduled/executed on this system
        system_df = df[df['ScheduledOn'] == system].copy()
        total_jobs_scheduled = len(system_df)
        
        # Makespan for this system
        if 'EndTime' in system_df.columns and 'SubmissionTime' in system_df.columns:
            system_makespan = system_df['EndTime'].max() - system_df['SubmissionTime'].min()
        else:
            system_makespan = 0
        
        # Throughput
        throughput = total_jobs_scheduled / system_makespan if system_makespan > 0 else 0
        
        # Node utilization
        if 'Nodes' in system_df.columns and 'ExecutionTime' in system_df.columns:
            total_node_minutes = (system_df['Nodes'] * system_df['ExecutionTime']).sum()
            system_capacity = system_capacities.get(system, {}).get('nodes', 1000)
            available_node_minutes = system_capacity * system_makespan
            node_utilization = (total_node_minutes / available_node_minutes * 100) if available_node_minutes > 0 else 0
        else:
            node_utilization = 0
        
        # Storage utilization
        if 'RequestedStorageGB' in system_df.columns and 'ExecutionTime' in system_df.columns:
            total_storage_gb_minutes = (system_df['RequestedStorageGB'] * system_df['ExecutionTime']).sum()
            storage_capacity = system_capacities.get(system, {}).get('storage_gb', 100_000_000)
            # Multiply by 2 for 2 storage systems per site
            available_storage_gb_minutes = (2 * storage_capacity) * system_makespan
            storage_utilization = (total_storage_gb_minutes / available_storage_gb_minutes * 100) if available_storage_gb_minutes > 0 else 0
        else:
            storage_utilization = 0
        
        # Time statistics
        mean_turnaround = system_df['TurnaroundTime'].mean() if 'TurnaroundTime' in system_df.columns else 0
        mean_execution = system_df['ExecutionTime'].mean() if 'ExecutionTime' in system_df.columns else 0
        mean_decision = system_df['DecisionTime'].mean() if 'DecisionTime' in system_df.columns else 0
        
        metrics.append({
            'System': system,
            'JobsSubmitted': jobs_submitted,
            'JobsScheduled': total_jobs_scheduled,
            'Makespan (min)': round(system_makespan, 2),
            'Throughput (jobs/min)': round(throughput, 4),
            'NodeUtilization (%)': round(node_utilization, 2),
            'StorageUtilization (%)': round(storage_utilization, 2),
            'MeanTurnaroundTime (min)': round(mean_turnaround, 2),
            'MeanExecutionTime (min)': round(mean_execution, 2),
            'MeanDecisionTime (sec)': round(mean_decision, 4)
        })
    
    metrics_df = pd.DataFrame(metrics)
    return metrics_df
def print_per_system_metrics(metrics_df):
    """Pretty print per-system metrics."""
    if metrics_df is None or len(metrics_df) == 0:
        print("No per-system metrics available")
        return
    
    print("\n" + "="*80)
    print("PER-SYSTEM METRICS")
    print("="*80)
    print(metrics_df.to_string(index=False))
    print("="*80 + "\n")

def print_summary_stats(df, makespan):
    """Print summary statistics."""
    print("\n" + "="*60)
    print("SIMULATION SUMMARY")
    print("="*60)
    
    print(f"\nMakespan: {makespan:.2f} minutes")
    print(f"Total Jobs: {len(df)}")
    
    if 'ScheduledOn' in df.columns:
        print(f"\nJobs per System:")
        for system, count in df['ScheduledOn'].value_counts().items():
            print(f"  {system}: {count}")
    
    if 'TurnaroundTime' in df.columns:
        print(f"\nTurnaround Time Statistics:")
        print(f"  Mean: {df['TurnaroundTime'].mean():.2f} minutes")
        print(f"  Median: {df['TurnaroundTime'].median():.2f} minutes")
        print(f"  Min: {df['TurnaroundTime'].min():.2f} minutes")
        print(f"  Max: {df['TurnaroundTime'].max():.2f} minutes")
    
    if 'ExecutionTime' in df.columns:
        print(f"\nExecution Time Statistics:")
        print(f"  Mean: {df['ExecutionTime'].mean():.2f} minutes")
        print(f"  Median: {df['ExecutionTime'].median():.2f} minutes")
    
    if 'DecisionTime' in df.columns:
        print(f"\nDecision Time Statistics:")
        print(f"  Mean: {df['DecisionTime'].mean():.4f} seconds")
        print(f"  Median: {df['DecisionTime'].median():.4f} seconds")
    
    print("="*60 + "\n")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze SWARM-DMAS simulation results')
    parser.add_argument('csv_file', nargs='?', default='../build/test.csv',
                       help='Path to CSV results file (default: ../build/test.csv)')
    parser.add_argument('--workload', '-w', default='../build/workloads/test_workload.json',
                       help='Path to workload JSON file (optional, for job definitions)')
    parser.add_argument('--output-dir', default='./plots',
                       help='Output directory for plots (default: ./plots)')
    
    args = parser.parse_args()
    
    csv_path = Path(args.csv_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        print(f"Please run the simulation first to generate test.csv")
        return
    
    # Load job definitions if workload file provided
    jobs_df = None
    if args.workload:
        workload_path = Path(args.workload)
        if workload_path.exists():
            print(f"Loading job definitions from: {workload_path}")
            jobs_df = load_job_definitions(workload_path)
            print(f"Loaded {len(jobs_df)} job definitions")
        else:
            print(f"Warning: Workload file not found: {workload_path}")
    
    # print(f"Loading results from: {csv_path}")
    df = load_results(csv_path, jobs_df)
    
    # print(f"Loaded {len(df)} jobs")
    # print(f"Columns: {', '.join(df.columns)}")
    
    # Calculate makespan
    makespan = calculate_makespan(df)
    
    # Print summary statistics
    print_summary_stats(df, makespan)

    # Calculate and print per-system metrics
    per_system_metrics = calculate_per_system_metrics(df)
    # print_per_system_metrics(per_system_metrics)
    
    # Save per-system metrics to CSV
    if per_system_metrics is not None:
        metrics_csv_path = output_dir / 'per_system_metrics.csv'
        per_system_metrics.to_csv(metrics_csv_path, index=False)
        print(f"✓ Per-system metrics saved to {metrics_csv_path}") 
    
    # Generate plots
    print("\nGenerating plots...")
    plot_resource_utilization(df, output_dir / 'resource_utilization.png')
    plot_utilization_percentage(df, output_dir / 'utilization_percentage.png')
    
    thrhput = calculate_throughput(df)
    print(f"\nThroughput: {thrhput:.2f} jobs/time unit")
    
    node_utili, storage_utili = calculate_resource_utilization(df)
    print(f"\nOverall Node Utilization: {node_utili}%")
    print(f"Overall Storage Utilization: {storage_utili}%")

    print("\n✓ Analysis complete!")

if __name__ == "__main__":
    main()
