import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# silent warnings
import warnings
warnings.filterwarnings("ignore")


def validate_jobs(jobs_df):
    for idx, row in jobs_df.iterrows():
        site = row['HPCSite']
        system = row['HPCSystem']
        site_configs = {
            'OLCF': ['Frontier', 'Andes'],
            'ALCF': ['Aurora', 'Crux'],
            'NERSC': ['Perlmutter-Phase-1', 'Perlmutter-Phase-2']
        }

        if system not in site_configs[site]:
            raise ValueError(f"Inconsistent HPCSite and HPCSystem for JobID {row['JobID']}: {site} - {system}")
        

    # check for resource limits
    site_resource_limits = {
        'OLCF': {
            'Frontier': {'node_limit': 9472, 'memory_limit': 12000, 'storage_limit': 700000000},
            'Andes': {'node_limit': 704, 'memory_limit': 256, 'storage_limit': 700000000}
        },
        'ALCF': {
            'Aurora': {'node_limit': 10624, 'memory_limit': 984, 'storage_limit': 220000000},
            'Crux': {'node_limit': 256, 'memory_limit': 512, 'storage_limit': 220000000}
        },
        'NERSC': {
            'Perlmutter-Phase-1': {'node_limit': 1536, 'memory_limit': 672, 'storage_limit': 35000000},
            'Perlmutter-Phase-2': {'node_limit': 3072, 'memory_limit': 512, 'storage_limit': 36000000}
        }
    }
    for idx, row in jobs_df.iterrows():
        site = row['HPCSite']
        system = row['HPCSystem']
        nodes = row['Nodes']
        memory = row['MemoryGB']
        storage = row['RequestedStorageGB']

        limits = site_resource_limits[site][system]
        if nodes > limits['node_limit']:
            raise ValueError(f"Node request exceeds limit for JobID {row['JobID']}: {nodes} > {limits['node_limit']}")
        if memory > limits['memory_limit']:
            raise ValueError(f"Memory request exceeds limit for JobID {row['JobID']}: {memory} > {limits['memory_limit']}")
        if storage > limits['storage_limit']:
            raise ValueError(f"Storage request exceeds limit for JobID {row['JobID']}: {storage} > {limits['storage_limit']}")
    
    # check for jobtype and system compatibility
    jobtype_system_map = {
        'HPC': ['Frontier'],
        'AI': ['Aurora'],
        'HYBRID': ['Perlmutter-Phase-1', 'Perlmutter-Phase-2'],
        'STORAGE': ['Andes', 'Crux']
    }
    for idx, row in jobs_df.iterrows():
        job_type = row['JobType']
        system = row['HPCSystem']
        if system not in jobtype_system_map[job_type]:
            raise ValueError(f"Incompatible JobType and HPCSystem for JobID {row['JobID']}: {job_type} - {system}")    

    # check for has_gpu requirement
    for idx, row in jobs_df.iterrows():
        system = row['HPCSystem']
        requested_gpu = row['RequestedGPU']
        site = row['HPCSite']
        site_configs = {
            'OLCF': {
                'Frontier': {'has_gpu' : True},
                'Andes': {'has_gpu' : False}
            },
            'ALCF': {
                'Aurora': {'has_gpu' : True},
                'Crux': {'has_gpu' : False}
            },
            'NERSC': {
                'Perlmutter-Phase-1': {'has_gpu' : True},
                'Perlmutter-Phase-2': {'has_gpu' : False}
            }
        }
        has_gpu = site_configs[site][system]['has_gpu']
        if requested_gpu and not has_gpu:
            raise ValueError(f"GPU requested but not available for JobID {row['JobID']}: {system}")
        
    
    # print all checks passed
    print("All job validations passed.")

# Visuale check
def visualize_job_distributions(jobs_df):
    # Visualize subplots of job characteristics

    fig, axs = plt.subplots(3, 2, figsize=(12, 10))
    sns.histplot(jobs_df['SubmissionTime'], bins=20, kde=True, ax=axs[0, 0], color='skyblue')
    axs[0, 0].set_title('Submission Time Distribution')
    sns.histplot(jobs_df['Walltime'], bins=20, kde=True, ax=axs[0, 1], color='salmon')
    axs[0, 1].set_title('Walltime Distribution')
    sns.histplot(jobs_df['Nodes'], bins=20, kde=True, ax=axs[1, 0], color='lightgreen')
    axs[1, 0].set_title('CPUs Distribution')
    sns.histplot(jobs_df['MemoryGB'], bins=20, kde=True, ax=axs[1, 1], color='plum')
    axs[1, 1].set_title('Memory Distribution (GB)') 
    # plot requested storage on log scale
    # sns.histplot(jobs_df['RequestedStorageGB'], bins=20, kde=True, ax=axs[0, 2], color='orange')
    # axs[2, 0].set_title('Storage Distribution (GB)')
    sns.countplot(data=jobs_df, x='HPCSite', order=jobs_df['HPCSite'].value_counts().index, ax=axs[2, 0], palette='pastel')
    axs[2, 0].set_title('HPC Site Distribution')
    # job type by hpc site
    sns.countplot(data=jobs_df, x='JobType', order=jobs_df['JobType'].value_counts().index, ax=axs[2, 1], palette='muted')
    axs[2, 1].set_title('Job Type by HPC Site')
    plt.tight_layout()
    plt.savefig('job_distributions.png')
    plt.show()



