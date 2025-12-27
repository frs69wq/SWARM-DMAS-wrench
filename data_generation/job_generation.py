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
    scenario = kwargs.get('scenario', 'busy_day')
    
    # Define job types
    job_types = ['HPC', 'AI', 'STORAGE', 'HYBRID']
    # Job templates defining typical job characteristics
    job_templates = [{
        'type': 'HPC',
        'walltime': (1,24),         # in hours
        'node_range': (64, 2048),
        'requested_gpu': [False],
        'storage': (500, 50_000),   # in GB
        # Can be submitted at any site/system
        'sites': ['OLCF', 'ALCF', 'NERSC'],
        'systems': ['Frontier', 'Andes', 'Aurora', 'Crux', 'Perlmutter-Phase-1', 'Perlmutter-Phase-2']
    }, {
        'type': 'AI',
        'walltime': (4,120), # in hours
        'node_range': (4,256),   
        'requested_gpu': [True],
        'storage': (1000,100_000), # in GB
        'sites': ['OLCF', 'ALCF', 'NERSC'],
        'systems': ['Frontier', 'Andes', 'Aurora', 'Crux', 'Perlmutter-Phase-1', 'Perlmutter-Phase-2']
    }, {
        'type': 'STORAGE',
        'walltime': (1, 72),
        'node_range': (32, 512),
        'requested_gpu': [False],
        'storage': (10_000, 500_000),
        'sites': ['OLCF', 'ALCF', 'NERSC'],
        'systems': ['Frontier', 'Andes', 'Aurora', 'Crux', 'Perlmutter-Phase-1', 'Perlmutter-Phase-2']
    }, {
        'type': 'HYBRID',
        'walltime': (8,120),
        'node_range': (8, 512),
        # requested GPU for hybrid jobs could be either True or False
        'requested_gpu': [True, False],
        'storage': (2000, 50_000),
        'sites': ['OLCF', 'ALCF', 'NERSC'],
        'systems': ['Frontier', 'Andes', 'Aurora', 'Crux', 'Perlmutter-Phase-1', 'Perlmutter-Phase-2']
    }]
    
    site_configs = {
        'OLCF': {
            'machines': {
                'Frontier': {'type': 'HPC', 'has_gpu' : True, 'node_limit': 9472, 'memory_limit': 12000, 'storage_limit': 700000000},
                'Andes': {'type': 'STORAGE', 'has_gpu' : False, 'node_limit': 704, 'memory_limit': 256, 'storage_limit': 700000000}
            } # memory and processing speed is per node in GB
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

    if scenario == 'busy_day':
        # Simulate 27-hour period across 3 timezones (EST, CST, PST)
        # Generate submission times for n_jobs at the same time
        # EST: hours 0-24 (peak at 12 EST = hour 12)
        # CST: hours 0-24 but offset +1 from EST (peak at 12 CST = hour 13 in timeline)
        # PST: hours 0-24 but offset +3 from EST (peak at 12 PST = hour 15 in timeline)
            
        # Timezone configurations
        timezones = {
            'OLCF': {'name': 'EST', 'offset': 0},    # Oak Ridge, Tennessee
            'ALCF': {'name': 'CST', 'offset': 1},    # Argonne, Illinois  
            'NERSC': {'name': 'PST', 'offset': 3}    # Berkeley, California
        }
        
        # Distribute jobs across sites/timezones
        jobs_per_site = n_jobs // 3
        remainder = n_jobs % 3
        
        all_submission_times = []
        all_sites = []
        
        for idx, (site, tz_info) in enumerate(timezones.items()):
            # Calculate how many jobs for this site
            site_jobs = jobs_per_site + (1 if idx < remainder else 0)
            
            # Peak at noon local time = 12 + timezone offset in global timeline
            peak_center = 12.0 + tz_info['offset']
            std_hours = 4.0
            
            # Generate normally distributed submission times for this timezone
            site_submission_times = np.random.normal(
                loc=peak_center,
                scale=std_hours,
                size=site_jobs
            )
            
            # Clip to reasonable range around the peak (±12 hours from peak)
            site_submission_times = np.clip(
                site_submission_times, 
                tz_info['offset'],           # Start of day in this timezone
                tz_info['offset'] + 24.0     # End of day in this timezone
            )
            
            all_submission_times.extend(site_submission_times)
            all_sites.extend([site] * site_jobs)
        
        # Convert to numpy array and sort chronologically with sites
        submission_times = np.array(all_submission_times)
        sites_array = np.array(all_sites)
        
        # Sort both by submission time
        sort_idx = np.argsort(submission_times)
        submission_times = submission_times[sort_idx]
        sites_array = sites_array[sort_idx]
        
        print(f"Job submissions span: {submission_times[0]:.2f}h to {submission_times[-1]:.2f}h (27-hour period)")
        print(f"  OLCF (EST): {np.sum(sites_array == 'OLCF')} jobs, peak at hour 12")
        print(f"  ALCF (CST): {np.sum(sites_array == 'ALCF')} jobs, peak at hour 13")
        print(f"  NERSC (PST): {np.sum(sites_array == 'NERSC')} jobs, peak at hour 15")

    else:
        # Submission times ~ interarrival
        # Exponential distribution for inter-arrival times; does not consider timezones
        # not used
        submission_intervals = {
            'busy_day': 1 / 10,
        }
        arrival_rate = submission_intervals.get(scenario, 1 / 10)
        submission_times = np.cumsum(np.random.exponential(arrival_rate, n_jobs))
    
    # 1. Generate Job IDs for all jobs
    job_ids = np.arange(1, n_jobs + 1)
    
    # 2. Assign job-types randomly to all jobs first
    types = [random.choice(job_types) for _ in range(n_jobs)]
    
    # Initialize empty arrays to hold job characteristics 
    walltimes = np.zeros(n_jobs, dtype=int)
    nodes = np.zeros(n_jobs, dtype=int)
    memories = np.zeros(n_jobs, dtype=int)
    requested_gpus = np.zeros(n_jobs, dtype=bool)
    requested_storages = np.zeros(n_jobs, dtype=int)
    hpc_sites = np.empty(n_jobs, dtype=object)
    hpc_systems = np.empty(n_jobs, dtype=object)
    
    # 3. Generate job characteristics based on job-type template for EACH job and then assign to empty arrays (line #154-160)
    for i in range(n_jobs):
        # Get the job type
        job_type = types[i]

        # Get the job template
        template = job_templates[job_types.index(job_type)]

        # 3.1 Generate job parameters based on template
        job_nodes = random.randint(*template['node_range'])
        job_requested_storage = random.uniform(*template['storage'])
        job_hpc_system = random.choice(template['systems'])
        
        # Ensure resources don't exceed site limits 
        # 3.1.1 check job_nodes don't exceed node limits
        for site, config in site_configs.items():
            # get the system limits
            if job_hpc_system in config['machines']:
                machine_node_limit = config['machines'][job_hpc_system]['node_limit']
                job_nodes = min(job_nodes, machine_node_limit)
                # ensure storage does not exceed machine limits
                machine_storage_limit = config['machines'][job_hpc_system]['storage_limit']
                job_requested_storage = min(job_requested_storage, machine_storage_limit)
                break
               
        # 3.2 Walltime and other parameters
        job_walltime = random.uniform(*template['walltime'])
        job_requested_gpu = random.choice(template['requested_gpu'])
        
        # Find the site for the selected system
        for site, config in site_configs.items():
            if job_hpc_system in config['machines']:
                # 3.3 Job site assignment
                job_hpc_site = site
                memory_per_node = config['machines'][job_hpc_system]['memory_limit']
                break
        # 3.4 Total memory calculation based on nodes and per-node memory
        job_memory = job_nodes * memory_per_node # total memory per job in GB
        
        # Assign generated values back to arrays
        walltimes[i] = job_walltime
        nodes[i] = job_nodes 
        memories[i] = job_memory  
        requested_gpus[i] = job_requested_gpu
        requested_storages[i] = job_requested_storage
        hpc_sites[i] = job_hpc_site
        hpc_systems[i] = job_hpc_system
   
    # 4. Users & groups
    user_ids = [users for users in np.random.randint(1, max(2, n_jobs // 2 + 1), size=n_jobs)]
    group_ids = [groups for groups in np.random.randint(1, max(2, n_jobs // 4 + 1), size=n_jobs)]

    # Compile all job data into a DataFrame for all n_jobs.
    df = pd.DataFrame({
        'JobID': job_ids,
        'SubmissionTime': np.round(submission_times, 3),
        'Walltime': walltimes,
        'Nodes': nodes,
        'MemoryGB': memories,
        'RequestedGPU': requested_gpus,
        'RequestedStorageGB': requested_storages,
        'JobType': types,
        'UserID': user_ids,
        'GroupID': group_ids,
        'HPCSite': hpc_sites,
        'HPCSystem': hpc_systems
    })

    return df

def main():

    parser = argparse.ArgumentParser(description='Generate synthetic jobs for simulation')
    parser.add_argument('--n_jobs', type=int, default=999, help='Total number of jobs to generate')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility, -1 for no seed')
    parser.add_argument('--scenario', type=str, default='busy_day',
                    # following choices are not available atm
                    choices=['busy_day', 'homogeneous_short', 'heterogeneous_mix', 'long_job_dominant',
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

    # Call script to validate jobs
    # Uncomment the following lines to enable validation
    # from validate_generated_jobs import validate_jobs
    # validate_jobs(jobs_df)

    # Visualize job distributions
    # Uncomment the following lines to enable visualization 
    # from validate_generated_jobs import visualize_job_distributions, visualize_submission_times_by_timezone
    # visualize_job_distributions(jobs_df)
    # visualize_submission_times_by_timezone(jobs_df)


if __name__ == "__main__":
    main()
