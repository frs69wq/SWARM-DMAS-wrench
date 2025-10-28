def compute_bid(job, machine, current_time=0):
    """Compute a heuristic bid score for a job on a given machine.
    
    Args:
        job (dict): Job characteristics including 'Nodes', 'RequestedGPU', 'SubmissionTime', 'JobType', 'HPCSite', 'HPCSystem'.
        machine (object): Machine characteristics with attributes 'available_nodes', 'used_nodes', 'node_limit', 'has_gpu', 'queue_length'.
        current_time (int): Current time in the scheduling system for wait time calculations.
    """
    # FIXME Please use the names of the fields in JobDecription: "job_id", "user_id","group_id", "job_type",
    #       "submission_time", "walltime", "num_nodes", "needs_gpu", "requested_memory_gb", "requested_storage_gb",
    #       "hpc_site", "hpc_system"
    
    # FIXME Please use the names of the fields in HPC System description: "name", "type", "num_nodes",
    #       "memory_amount_in_gb", "storage_amount_in_gb", "has_gpu", "network_interconnect"
    
    # FIXME Please use the names of the fields in HPC System status: "current_num_available_nodes",
    #       "current_job_start_time_estimate", "queue_length"

    # FIXME we don't send the current time to python, I can fix this

    # FIXME we have an current estimation of the start time of the job, might be interesting to use it. 

    # FIXME The objective is to have this heuristic in python as a fallback of the LLM-bidding, it has to work with the
    # rest of the code, and it can't as it completely disregard what has been done/ 
    
    # Safely read job fields (job is a dict)
    nodes_req = job.get("Nodes", 0)
    requested_gpu = job.get("RequestedGPU", False)
    submission_time = job.get("SubmissionTime", 0)
    job_type = job.get("JobType", "")
    job_site = job.get("HPCSite", "")
    job_system = job.get("HPCSystem", "")
 

    # 1. Feasibility check
    # if job needs more nodes than available AND requests GPU but machine has no GPU => infeasible
    # FIXME also check that memory requirement is lower than machine available memory
    if nodes_req > machine.available_nodes and requested_gpu and not machine.has_gpu:
        return 0.0

    # 2. Utilization-based scores
    node_util = (machine.used_nodes / machine.node_limit) if machine.node_limit else 0.0
    node_score = 1 - node_util

    # 3. Compatibility (avoid division by zero)
    # FIXME jobs always have a number of nodes, can't have division by 0
    node_compat = min(1.0, machine.available_nodes / nodes_req) if nodes_req else 1.0

    # 4. Queue length factor
    queue_factor = max(0.1, 1 - 0.1 * machine.queue_length)

    # 5. Time-based priority factor (longer wait = higher priority)
    wait_time = current_time - submission_time
    # Scale wait time: 0-100 time units -> 1.0-2.0 multiplier
    # FIXME what is the rationale of that formula? 
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
    # FIXME that is not the case for the one above, I can fix it. 

    machine_name = getattr(machine, 'name', job_system)  # Assume machine knows its name
    
    # FIXME what the rationale of giving higher preference to local scheduling?
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
