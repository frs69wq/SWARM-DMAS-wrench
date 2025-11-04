import argparse
import json
import os
import random
import uuid
from typing import List
import numpy as np
import pandas as pd
from scipy import stats
np.random.seed(42)

def generate_synthetic_jobs_v2(**kwargs):
    """Scenario-based synthetic job generator."""
    n_jobs = kwargs.get('n_jobs', 100)
    scenario = kwargs.get('scenario', 'heterogeneous_mix')
    
    # Site and machine-specific resource limits
    site_configs = {
        'OLCF': {
            'machines': {
                'Frontier': {'type': 'HPC', 'has_gpu' : True, 'node_limit': 9472, 'memory_limit': 12000, 'storage_limit': 700000000},
                'Andes': {'type': 'STORAGE', 'has_gpu' : False, 'node_limit': 704, 'memory_limit': 256, 'storage_limit': 700000000}
            }
        },
        'ALCF': {
            'machines': {
                'Aurora': {'type': 'AI', 'has_gpu' : True, 'node_limit': 10624, 'memory_limit': 984, 'storage_limit': 220000000},
                'Crux': {'type': 'STORAGE','has_gpu' : False,  'node_limit': 256, 'memory_limit': 512, 'storage_limit': 220000000}
            }
        },
        'NERSC': {
            'machines': {
                'Perlmutter-Phase-1': {'type': 'HYBRID', 'has_gpu' : True,'node_limit': 1536, 'memory_limit': 672, 'storage_limit': 35000000},
                'Perlmutter-Phase-2': {'type': 'HYBRID', 'has_gpu' : False, 'node_limit': 3072, 'memory_limit': 512, 'storage_limit': 36000000}
            }
        }
    }
    hpc_sites = list(site_configs.keys())

    if scenario == 'heterogeneous_mix':
        # Simulate a 24-hour day (0 = midnight, 24 = next midnight)
        # Peak submission hours: 12-15 (noon to 3 PM) - typical work hours
        peak_start_hour = 10.0  # 10 AM
        peak_end_hour = 14.0    # 2 PM
        peak_center = (peak_start_hour + peak_end_hour) / 2  # 12 PM
        
        # Standard deviation controls how concentrated around peak
        std_hours = 4.0
        
        # Generate normally distributed submission times
        submission_times = np.random.normal(
            loc=peak_center,     # Center at 12 PM
            scale=std_hours,     # 4 hour standard deviation
            size=n_jobs
        )
        
        # Ensure all submission times are within the 24-hour day
        submission_times = np.clip(submission_times, 0.0, 24.0)
        submission_times = np.sort(submission_times)  # Sort chronologically
        
        print(f"Job submissions span: {submission_times[0]:.2f}h to {submission_times[-1]:.2f}h")
        print(f"Peak period ({peak_start_hour}-{peak_end_hour}h) contains {np.sum((submission_times >= peak_start_hour) & (submission_times <= peak_end_hour))} jobs ({100*np.sum((submission_times >= peak_start_hour) & (submission_times <= peak_end_hour))/n_jobs:.1f}%)")


    else:
        # Submission times ~ interarrival
        submission_intervals = {
            'heterogeneous_mix': 1 / 10,
        }
        arrival_rate = submission_intervals.get(scenario, 1 / 10)
        submission_times = np.cumsum(np.random.exponential(arrival_rate, n_jobs))
    
    job_ids = np.arange(1, n_jobs + 1)
    
    # Assign sites to jobs first
    sites = [random.choice(hpc_sites) for _ in range(n_jobs)]
    # print('1.', sites)

    # Assign machines to jobs, matching job type later
    machines = [None] * n_jobs
    
    # Initialize arrays for job characteristics
    gamma_params = (0.21756525789913037, 0.9999999999999999, 77924.92524440849)
    walltimes = stats.gamma(*gamma_params).rvs(size=n_jobs)
    nodes = np.zeros(n_jobs, dtype=int)
    memory = np.zeros(n_jobs, dtype=int)
    requested_gpu = np.zeros(n_jobs, dtype=bool)
    requested_storage = np.zeros(n_jobs, dtype=int)
    job_type = np.empty(n_jobs, dtype=object)
    machine_names = np.empty(n_jobs, dtype=object)
    
    # Generate job characteristics based on site and scenario
    for i in range(n_jobs):
        site = sites[i]
        # print('2.', site)
        site_config = site_configs[site]

        # select type from site_configs
        selected_job_types = [mconf['type'] for mname, mconf in site_config['machines'].items()]
        # print('selected_job_types:', selected_job_types, 'for site:', site)
        job_type[i] = np.random.choice(selected_job_types)
        # print('3.', job_type[i])

        for mname, mconf in site_config['machines'].items():
            # check if job type matches more than one machine in the same site
            if job_type[i] == 'HYBRID':
                # randomly pick one of the hybrid machines
                hybrid_machines = [mn for mn, mc in site_config['machines'].items() if mc['type'] == 'HYBRID']
                chosen_machine = random.choice(hybrid_machines)
                machine_conf = site_config['machines'][chosen_machine]
                break
            elif (mconf['type'] == job_type[i]):
                chosen_machine = mname
                machine_conf = mconf
                break
        machine_names[i] = chosen_machine

        # Get machine-specific limits
        machine_node_limit = machine_conf['node_limit']
        memory_limit = machine_conf['memory_limit']
        storage_limit = machine_conf['storage_limit']

        if scenario == 'heterogeneous_mix':
            # walltimes[i] = int(np.random.gamma(1.5, 300))
            # Generate realistic supercomputer job sizes (128-2048 nodes)
            nodes[i] = min(np.random.randint(32, 2049), machine_node_limit)
            # Memory scales with nodes: 4-32 GB per node
            memory_per_node = memory_limit / machine_node_limit
            # memory[i] = min(int(nodes[i] * memory_per_node), memory_limit)
            memory[i] = nodes[i] * memory_per_node
            requested_gpu[i] = machine_conf['has_gpu']
            requested_storage[i] = min(int(np.random.lognormal(10, 1.5)), storage_limit)

   
    # Users & groups
    user_ids = [users for users in np.random.randint(1, max(2, n_jobs // 2 + 1), size=n_jobs)]
    group_ids = [groups for groups in np.random.randint(1, max(2, n_jobs // 4 + 1), size=n_jobs)]

    df = pd.DataFrame({
        'JobID': job_ids,
        'SubmissionTime': np.round(submission_times, 3),
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
    parser.add_argument('--n_jobs', type=int, default=2000, help='Total number of jobs to generate')
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

    # Validate jobs

    # call script to validate jobs
    from validate_jobs import validate_jobs
    validate_jobs(jobs_df)

    # Visualize job distributions 
    from validate_jobs import visualize_job_distributions
    visualize_job_distributions(jobs_df)

if __name__ == "__main__":
    main()