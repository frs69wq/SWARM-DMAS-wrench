def compute_bid(job, machine, current_time=0):

    # Safely read job fields (job is a dict)
    nodes_req = job.get("Nodes", 0)
    requested_gpu = job.get("RequestedGPU", False)
    submission_time = job.get("SubmissionTime", 0)
    job_type = job.get("JobType", "")
    job_site = job.get("HPCSite", "")
    job_system = job.get("HPCSystem", "")
 

    # 1. Feasibility check
    # if job needs more nodes than available AND requests GPU but machine has no GPU => infeasible
    if nodes_req > machine.available_nodes and requested_gpu and not machine.has_gpu:
        return 0.0

    # 2. Utilization-based scores
    node_util = (machine.used_nodes / machine.node_limit) if machine.node_limit else 0.0
    node_score = 1 - node_util

    # 3. Compatibility (avoid division by zero)
    node_compat = min(1.0, machine.available_nodes / nodes_req) if nodes_req else 1.0

    # 4. Queue length factor
    queue_factor = max(0.1, 1 - 0.1 * machine.queue_length)

    # 5. Time-based priority factor (longer wait = higher priority)
    wait_time = current_time - submission_time
    # Scale wait time: 0-100 time units -> 1.0-2.0 multiplier
    time_factor = min(2.0, 1.0 + (wait_time / 100.0))

    # 6. Job-Resource compatibility factor
    machine_type = getattr(machine, 'type', '')
    if job_type == machine_type:
        resource_factor = 1.0  # Perfect match
    elif (job_type == 'HPC' and machine_type in ['AI', 'HYBRID']) or \
         (job_type == 'AI' and machine_type in ['HPC', 'HYBRID']) or \
         (job_type == 'HYBRID' and machine_type in ['HPC', 'AI']):
        resource_factor = 0.8  # Good compatibility
    elif job_type == 'STORAGE' and machine_type != 'STORAGE':
        resource_factor = 0.3  # Storage jobs prefer storage machines
    elif machine_type == 'STORAGE' and job_type != 'STORAGE':
        resource_factor = 0.5  # Storage machines can handle other jobs but not optimal
    else:
        resource_factor = 0.5  # Default compatibility

    # 7. Site/System preference factor
    machine_site = getattr(machine, 'site', job_site)  # Assume machine knows its site
    machine_name = getattr(machine, 'name', job_system)  # Assume machine knows its name
    
    if job_site == machine_site and job_system == machine_name:
        site_factor = 1.0  # Perfect match: same site and system
    elif job_site == machine_site:
        site_factor = 0.9  # Same site, different system
    else:
        # Different sites - consider site characteristics
        if job_site == 'OLCF' and machine_site in ['ALCF', 'NERSC']:
            site_factor = 0.7  # Cross-site compatibility
        elif job_site == 'ALCF' and machine_site in ['OLCF', 'NERSC']:
            site_factor = 0.7  # Cross-site compatibility  
        elif job_site == 'NERSC' and machine_site in ['OLCF', 'ALCF']:
            site_factor = 0.7  # Cross-site compatibility
        else:
            site_factor = 0.6  # Default cross-site penalty

    # 8. Combine all factors
    base_score = node_score * node_compat * resource_factor * time_factor * site_factor 
    final_bid = min(1.0, base_score * queue_factor)

    return round(final_bid, 2)

# bid = compute_bid(job, machine, current_time=0)
# print("Computed bid with user/group factors:", bid)
