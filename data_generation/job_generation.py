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
    
    # Site and machine-specific resource limits
        # Site and machine-specific resource limits
    site_configs = {
        'OLCF': {
            'machines': {
                'Frontier': {'type': 'GPU', 'node_limit': 9472, 'memory_limit': 12000, 'storage_limit': 700000000},
                'Andes': {'type': 'CPU', 'node_limit': 704, 'memory_limit': 256, 'storage_limit': 700000000}
            }
        },
        'ALCF': {
            'machines': {
                'Aurora': {'type': 'GPU', 'node_limit': 10624, 'memory_limit': 984, 'storage_limit': 220000000},
                'Crux': {'type': 'CPU', 'node_limit': 256, 'memory_limit': 512, 'storage_limit': 220000000}
            }
        },
        'NERSC': {
            'machines': {
                'Perlmutter-Phase-1': {'type': 'GPU', 'node_limit': 1536, 'memory_limit': 672, 'storage_limit': 35000000},
                'Perlmutter-Phase-2': {'type': 'CPU', 'node_limit': 3072, 'memory_limit': 512, 'storage_limit': 36000000}
            }
        }
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
    # Assign machines to jobs, matching job type later
    machines = [None] * n_jobs
    
    # Initialize arrays for job characteristics
    walltimes = np.zeros(n_jobs, dtype=int)
    nodes = np.zeros(n_jobs, dtype=int)
    memory = np.zeros(n_jobs, dtype=int)
    requested_gpu = np.zeros(n_jobs, dtype=bool)
    requested_storage = np.zeros(n_jobs, dtype=int)
    job_type = np.empty(n_jobs, dtype=object)
    machine_names = np.empty(n_jobs, dtype=object)
    
    # Generate job characteristics based on site and scenario
    for i in range(n_jobs):
        site = sites[i]
        site_config = site_configs[site]
        
        # Select job type first
        if scenario == 'homogeneous_short':
            job_type[i] = np.random.choice(['HPC', 'AI', 'MEMORY'], p=[0.7, 0.2, 0.1])
        elif scenario == 'heterogeneous_mix':
            job_type[i] = np.random.choice(['HPC', 'AI', 'HYBRID', 'GPU', 'MEMORY', 'STORAGE'], p=[0.25, 0.2, 0.15, 0.15, 0.15, 0.1])
        elif scenario == 'long_job_dominant':
            is_long = np.random.rand() < 0.2
            job_type[i] = np.random.choice(['HPC', 'HYBRID', 'MEMORY'], p=[0.6, 0.3, 0.1]) if is_long else np.random.choice(['HPC', 'AI', 'GPU'], p=[0.5, 0.3, 0.2])
        elif scenario == 'high_parallelism':
            job_type[i] = np.random.choice(['GPU', 'HYBRID', 'HPC', 'AI'], p=[0.4, 0.3, 0.2, 0.1])
        elif scenario == 'resource_sparse':
            job_type[i] = np.random.choice(['HPC', 'MEMORY', 'AI'], p=[0.6, 0.3, 0.1])
        elif scenario == 'bursty_idle':
            burst_phase = (i // 100) % 2 == 0
            job_type[i] = np.random.choice(['AI', 'GPU', 'HYBRID'], p=[0.4, 0.4, 0.2]) if burst_phase else np.random.choice(['HPC', 'MEMORY', 'STORAGE'], p=[0.5, 0.3, 0.2])
        elif scenario == 'adversarial':
            job_type[i] = 'HYBRID' if i == 0 else 'HPC'

        # Select compatible machine for job type
        machine_options = []
        for mname, mconf in site_config['machines'].items():
            # GPU jobs must go to GPU machines, CPU jobs to CPU machines
            if job_type[i] in ['GPU', 'AI', 'HYBRID'] and mconf['type'] == 'GPU':
                machine_options.append((mname, mconf))
            elif job_type[i] in ['HPC', 'MEMORY', 'STORAGE'] and mconf['type'] == 'CPU':
                machine_options.append((mname, mconf))
        # If no compatible machine, fallback to any
        if not machine_options:
            machine_options = list(site_config['machines'].items())
        chosen_machine, machine_conf = random.choice(machine_options)
        machine_names[i] = chosen_machine
        
        # Get machine-specific limits
        machine_node_limit = machine_conf['node_limit']
        memory_limit = machine_conf['memory_limit']
        storage_limit = machine_conf['storage_limit']

        # Now generate job characteristics for realistic supercomputer sizes
        if scenario == 'homogeneous_short':
            walltimes[i] = np.random.randint(30, 120)
            # Make jobs larger for supercomputers
            nodes[i] = min(np.random.randint(128, 513), machine_node_limit)
            memory[i] = min(nodes[i] * np.random.randint(4, 16), memory_limit)
            requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.1)
            requested_storage[i] = min(int(np.random.gamma(2, 5000)), storage_limit)
            
        elif scenario == 'heterogeneous_mix':
            walltimes[i] = int(np.random.gamma(1.5, 300))
            # Generate realistic supercomputer job sizes (128-2048 nodes)
            nodes[i] = min(np.random.randint(128, 2049), machine_node_limit)
            # Memory scales with nodes: 4-32 GB per node
            memory_per_node = memory_limit / machine_node_limit
            # memory[i] = min(int(nodes[i] * memory_per_node), memory_limit)
            memory[i] = nodes[i] * memory_per_node
            requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.3)
            requested_storage[i] = min(int(np.random.lognormal(10, 1.5)), storage_limit)
            
        elif scenario == 'long_job_dominant':
            is_long = np.random.rand() < 0.2
            if is_long:
                walltimes[i] = np.random.randint(10000, 50000)
                nodes[i] = min(np.random.randint(512, 2049), machine_node_limit)
                memory[i] = min(nodes[i] * np.random.randint(8, 32), memory_limit)
                requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.8)
                requested_storage[i] = min(int(np.random.gamma(3, 50000)), storage_limit)
            else:
                walltimes[i] = np.random.randint(100, 500)
                nodes[i] = min(np.random.randint(32, 257), machine_node_limit)
                memory[i] = min(nodes[i] * np.random.randint(4, 16), memory_limit)
                requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.1)
                requested_storage[i] = min(int(np.random.gamma(1.5, 5000)), storage_limit)
                
        elif scenario == 'high_parallelism':
            walltimes[i] = int(np.random.gamma(1, 800))
            nodes[i] = min(np.random.randint(1024, 4097), machine_node_limit)
            memory[i] = min(int(nodes[i] * np.random.uniform(8, 32)), memory_limit)
            requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.6)
            requested_storage[i] = min(int(nodes[i] * np.random.uniform(1000, 5000)), storage_limit)
            
        elif scenario == 'resource_sparse':
            walltimes[i] = np.random.randint(30, 300)
            nodes[i] = min(np.random.randint(16, 129), machine_node_limit)
            memory[i] = min(nodes[i] * np.random.randint(2, 8), memory_limit)
            requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.05)
            requested_storage[i] = min(int(np.random.exponential(1000) + 1000), storage_limit)
            
        elif scenario == 'bursty_idle':
            burst_phase = (i // 100) % 2 == 0
            walltimes[i] = int(np.random.gamma(1, 600))
            nodes[i] = min(np.random.randint(64, 1025), machine_node_limit)
            memory[i] = min(nodes[i] * np.random.randint(4, 16), memory_limit)
            if burst_phase:
                requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.4)
                requested_storage[i] = min(int(np.random.gamma(2, 30000)), storage_limit)
            else:
                requested_gpu[i] = machine_conf['type'] == 'GPU' and (np.random.rand() < 0.1)
                requested_storage[i] = min(int(np.random.gamma(1, 8000)), storage_limit)
                if i < len(submission_times):
                    submission_times[i] += 50  # Add idle delay
                    
        elif scenario == 'adversarial':
            if i == 0:
                walltimes[i] = 100000
                nodes[i] = min(np.random.randint(1024, 2049), machine_node_limit)
                memory[i] = min(nodes[i] * 16, memory_limit)
                requested_gpu[i] = machine_conf['type'] == 'GPU'
                requested_storage[i] = min(100000000, storage_limit)  # 100GB
            else:
                walltimes[i] = 60
                nodes[i] = min(np.random.randint(32, 129), machine_node_limit)
                memory[i] = min(nodes[i] * 4, memory_limit)
                requested_gpu[i] = False
                requested_storage[i] = 1000000  # 1GB
    # Handle special case for adversarial scenario submission times
    if scenario == 'adversarial':
        submission_times = np.arange(n_jobs)

    # Fix RequestedGPU based on job type (GPU/AI/HYBRID/HPC jobs should always request GPU)
    for i in range(n_jobs):
        if job_type[i] in ['GPU', 'AI', 'HYBRID', 'HPC']:
            requested_gpu[i] = True
        else:  # MEMORY, STORAGE jobs don't need GPU
            requested_gpu[i] = False

    # Users & groups
    user_ids = [users for users in np.random.randint(1, max(2, n_jobs // 2 + 1), size=n_jobs)]
    group_ids = [groups for groups in np.random.randint(1, max(2, n_jobs // 4 + 1), size=n_jobs)]

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
        'HPCSite': sites,
        'HPCSystem': machine_names
    })

    return df

def main():

    parser = argparse.ArgumentParser(description='Generate synthetic jobs for simulation')
    parser.add_argument('--n_jobs', type=int, default=5, help='Total number of jobs to generate')
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
