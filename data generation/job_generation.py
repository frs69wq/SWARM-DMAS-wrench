import argparse
import json
import os
import random
import uuid
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

import seaborn as sns

def generate_synthetic_jobs_v2(**kwargs):
    """Scenario-based synthetic job generator."""
    n_jobs = kwargs.get('n_jobs', 100)
    scenario = kwargs.get('scenario', 'heterogeneous_mix')
    
    # Site-specific resource limits
    site_configs = {
        'ALCF': {'node_limit': 560, 'memory_limit': 280, 'storage_limit': 1120}, # polaris, memory in tb, storage in pb
        'OLCF': {'node_limit': 9408, 'memory_limit': 128, 'storage_limit': 695}, # frontier, memory in tb, storage in pb
        'NERSC': {'node_limit': 4864, 'memory_limit': 2312, 'storage_limit': 44} # perlmutter, nodes(gpu+cpu);excluding login nodes, memory in tb, storage (disc space) in pb
    }
    hpc_sites = list(site_configs.keys())

    # Submission times ~ interarrival
    submission_intervals = {
        'homogeneous_short': 1 / 20,
        'heterogeneous_mix': 1 / 10,
        'long_job_dominant': 1 / 8,
        'high_parallelism': 1 / 5,
        'resource_sparse': 1 / 12,
        'bursty_idle': 1 / 6,
        'adversarial': 1 / 25
    }
    arrival_rate = submission_intervals.get(scenario, 1 / 10)
    submission_times = np.cumsum(np.random.exponential(arrival_rate, n_jobs))
    job_ids = np.arange(1, n_jobs + 1)
    
    # Assign sites to jobs first
    sites = [random.choice(hpc_sites) for _ in range(n_jobs)]
    
    # Initialize arrays for job characteristics
    walltimes = np.zeros(n_jobs, dtype=int)
    nodes = np.zeros(n_jobs, dtype=int)
    memory = np.zeros(n_jobs, dtype=int)
    requested_gpu = np.zeros(n_jobs, dtype=bool)
    requested_storage = np.zeros(n_jobs, dtype=int)
    job_type = np.empty(n_jobs, dtype=object)
    
        # Generate job characteristics based on site and scenario
    for i in range(n_jobs):
        site = sites[i]
        site_config = site_configs[site]
        node_limit = site_config['node_limit']
        memory_limit = site_config['memory_limit']
        storage_limit = site_config['storage_limit']
        
        # Generate job characteristics based on scenario and site limits
        if scenario == 'homogeneous_short':
            walltimes[i] = np.random.randint(30, 120)
            nodes[i] = 2
            memory[i] = 4
            requested_gpu[i] = np.random.rand() < 0.1
            requested_storage[i] = min(int(np.random.gamma(2, 5)), storage_limit)
            job_type[i] = np.random.choice(['HPC', 'AI', 'MEMORY'], p=[0.7, 0.2, 0.1])
            
        elif scenario == 'heterogeneous_mix':
            walltimes[i] = int(np.random.gamma(1.5, 300))
            node_power = np.random.randint(1, min(int(np.log2(node_limit)) + 1, 9))
            nodes[i] = min(2 ** node_power, node_limit)
            mem_power = np.random.randint(1, min(int(np.log2(memory_limit)) + 1, 8))
            memory[i] = min(2 ** mem_power, memory_limit)
            requested_gpu[i] = np.random.rand() < 0.3
            requested_storage[i] = min(int(np.random.lognormal(3, 1.5)), storage_limit)
            job_type[i] = np.random.choice(['HPC', 'AI', 'HYBRID', 'GPU', 'MEMORY', 'STORAGE'], 
                                         p=[0.25, 0.2, 0.15, 0.15, 0.15, 0.1])
            
        elif scenario == 'long_job_dominant':
            is_long = np.random.rand() < 0.2
            if is_long:
                walltimes[i] = np.random.randint(10000, 50000)
                nodes[i] = min(128, node_limit)
                memory[i] = min(nodes[i] * np.random.randint(2, 8), memory_limit)
                requested_gpu[i] = np.random.rand() < 0.8
                requested_storage[i] = min(int(np.random.gamma(3, 200)), storage_limit)
                job_type[i] = np.random.choice(['HPC', 'HYBRID', 'MEMORY'], p=[0.6, 0.3, 0.1])
            else:
                walltimes[i] = np.random.randint(100, 500)
                nodes[i] = 2
                memory[i] = min(nodes[i] * np.random.randint(2, 8), memory_limit)
                requested_gpu[i] = np.random.rand() < 0.1
                requested_storage[i] = min(int(np.random.gamma(1.5, 10)), storage_limit)
                job_type[i] = np.random.choice(['HPC', 'AI', 'GPU'], p=[0.5, 0.3, 0.2])
                
        elif scenario == 'high_parallelism':
            walltimes[i] = int(np.random.gamma(1, 800))
            node_power = np.random.randint(6, min(int(np.log2(node_limit)) + 1, 10))
            nodes[i] = min(2 ** node_power, node_limit)
            memory[i] = min(int(nodes[i] * np.random.uniform(2, 6)), memory_limit)
            requested_gpu[i] = np.random.rand() < 0.6
            requested_storage[i] = min(int(nodes[i] * np.random.uniform(1, 5)), storage_limit)
            job_type[i] = np.random.choice(['GPU', 'HYBRID', 'HPC', 'AI'], p=[0.4, 0.3, 0.2, 0.1])
            
        elif scenario == 'resource_sparse':
            walltimes[i] = np.random.randint(30, 300)
            nodes[i] = 1
            memory[i] = np.random.randint(1, min(8, memory_limit))
            requested_gpu[i] = np.random.rand() < 0.05
            requested_storage[i] = min(int(np.random.exponential(2) + 1), storage_limit)
            job_type[i] = np.random.choice(['HPC', 'MEMORY', 'AI'], p=[0.6, 0.3, 0.1])
            
        elif scenario == 'bursty_idle':
            burst_phase = (i // 100) % 2 == 0
            walltimes[i] = int(np.random.gamma(1, 600))
            node_power = np.random.randint(1, min(int(np.log2(node_limit)) + 1, 7))
            nodes[i] = min(2 ** node_power, node_limit)
            memory[i] = min(nodes[i] * np.random.randint(1, 4), memory_limit)
            
            if burst_phase:
                requested_gpu[i] = np.random.rand() < 0.4
                requested_storage[i] = min(int(np.random.gamma(2, 30)), storage_limit)
                job_type[i] = np.random.choice(['AI', 'GPU', 'HYBRID'], p=[0.4, 0.4, 0.2])
            else:
                requested_gpu[i] = np.random.rand() < 0.1
                requested_storage[i] = min(int(np.random.gamma(1, 8)), storage_limit)
                job_type[i] = np.random.choice(['HPC', 'MEMORY', 'STORAGE'], p=[0.5, 0.3, 0.2])
                if i < len(submission_times):
                    submission_times[i] += 50  # Add idle delay
                    
        elif scenario == 'adversarial':
            if i == 0:  # First job is adversarial
                walltimes[i] = 100000
                nodes[i] = min(128, node_limit)
                memory[i] = min(256, memory_limit)
                requested_gpu[i] = True
                requested_storage[i] = min(10000, storage_limit)
                job_type[i] = 'HYBRID'
            else:
                walltimes[i] = 60
                nodes[i] = 1
                memory[i] = 4
                requested_gpu[i] = False
                requested_storage[i] = 1
                job_type[i] = 'HPC'

    # Handle special case for adversarial scenario submission times
    if scenario == 'adversarial':
        submission_times = np.arange(n_jobs)

    # Users & groups
    user_ids = [f"user_{i}" for i in np.random.randint(1, max(2, n_jobs // 2 + 1), size=n_jobs)]
    group_ids = [f"group_{i}" for i in np.random.randint(1, max(2, n_jobs // 4 + 1), size=n_jobs)]

    df = pd.DataFrame({
        'JobID': job_ids,
        'SubmissionTime': np.ceil(submission_times).astype(int),
        'Walltime': walltimes,
        'Nodes': nodes,
        'MemoryGB': memory,
        'RequestedGPU': requested_gpu,
        'RequestedStorageGB': requested_storage,
        'JobType': job_type,
        'UserID': user_ids,
        'GroupID': group_ids,
        'HPCSite': sites
    })

    return df

def main():

    parser = argparse.ArgumentParser(description='Generate synthetic jobs for simulation')
    parser.add_argument('--n_jobs', type=int, default=10_000, help='Total number of jobs to generate')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility, -1 for no seed')
    parser.add_argument('--scenario', type=str, default='heterogeneous_mix',
                    choices=['homogeneous_short', 'heterogeneous_mix', 'long_job_dominant',
                             'high_parallelism', 'resource_sparse', 'bursty_idle', 'adversarial'],
                    help='Workload scenario type')

    args = vars(parser.parse_args())

    if args['seed'] > 0:
        random.seed(args['seed'])
        np.random.seed(args['seed'])

    # Generate Jobs using the new v2 function
    jobs_df = generate_synthetic_jobs_v2(**args)
    scenario_name = args['scenario']

    # Save to JSON
    os.makedirs("./data", exist_ok=True)
    
    # Convert DataFrame to JSON format
    jobs_json = jobs_df.to_dict('records')
    
    # Save as JSON file
    json_filename = f"./data/{args['scenario']}_{args['n_jobs']}.json"
    with open(json_filename, 'w') as f:
        json.dump(jobs_json, f, indent=2)
    
    print(f"Generated {len(jobs_df)} jobs for scenario '{scenario_name}'")
    print(f"Saved to {json_filename}")


if __name__ == "__main__":
    main()