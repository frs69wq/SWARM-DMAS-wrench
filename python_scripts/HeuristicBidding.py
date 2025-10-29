def compute_bid(job_description, system_description, system_status, current_time=0):
    """Compute a heuristic bid score for a job on a given system_description.
    
    Args:
        job (json): JobDescription "job_id", "user_id","group_id", "job_type",
            "submission_time", "walltime", "num_nodes", "needs_gpu", "requested_memory_gb", "requested_storage_gb",
            "hpc_site", "hpc_system"
        hpc_system (json): HPC System description: "name", "type", "num_nodes",
            "memory_amount_in_gb", "storage_amount_in_gb", "has_gpu", "network_interconnect"
        system_status (json): HPC System status: "current_num_current_num_available_nodes", "current_job_start_time_estimate", "queue_length"
        current_time (int): Current time in the scheduling system for wait time calculations.
    """

    # FIXME we don't send the current time to python, I can fix this
    # COMMENT: OK!
    # FIXME we have an current estimation of the start time of the job, might be interesting to use it. 
    # COMMENT: Can you point out where it is? (I could locate it in utils.cpp line#66)
    # COMMENT: Part of job status:  current_job_start_time_estimate

    # Safely read job fields (job is a dict)
    # FIXME system_description and system status are also dict, you cannot use properties
    nodes_req = job_description["num_nodes"]
    requested_gpu = job_description["requested_gpu"]
    submission_time = job_description["submission_time"]
    job_type = job_description["job_type"]
    job_site = job_description["hpc_site"]
    job_system = job_description["hpc_system"]
 

    # 1. Feasibility check
    # if job needs more nodes than available AND requests GPU but system_description has no GPU => infeasible
    # FIXME also check that memory requirement is lower than system_description available memory
    # COMMENT: Please confirm as calculation seems different from the heuristic (in the do_not_pass_acceptance_tests function) based on the hpc_system_description 
    # FIXME: job expresses a total memory request, the system is described with a memory amount *per node*
    node_per_memory_gb = system_description["memory_amount_in_gb"] / system_description["num_nodes"] 
    if nodes_req > system_description["current_num_available_nodes"] and requested_gpu and not system_description.has_gpu and job_description["requested_memoory_gb"] > node_per_memory_gb:
        return 0.0

    # 2. Utilization-based scores
    node_util = (system_description.used_nodes / system_description.node_limit) if system_description.node_limit else 0.0
    node_score = 1 - node_util

    # 3. Compatibility (avoid division by zero)
    node_compat = min(1.0, system_description.current_num_available_nodes / nodes_req) 

    # 4. Queue length factor
    queue_factor = max(0.1, 1 - 0.1 * system_status.queue_length)

    # 5. Time-based priority factor (longer wait = higher priority)
    wait_time = current_time - submission_time
    # Scale wait time: 0-100 time units -> 1.0-2.0 multiplier
    # Rationale: increase the bid for jobs that have been waiting longer, up to a maximum factor of 2.0. 
    time_factor = min(2.0, 1.0 + (wait_time / 100.0))

    # 6. Job-Resource compatibility factor
    system_description_type = getattr(system_description, 'type', '')
    if job_type == system_description_type:
        resource_factor = 1.0  # Perfect match
    elif (job_type == 'HPC' and system_description_type in ['AI', 'HYBRID']) or \
         (job_type == 'AI' and system_description_type in ['HPC', 'HYBRID']) or \
         (job_type == 'HYBRID' and system_description_type in ['HPC', 'AI']):
        resource_factor = 0.8  # Good compatibility
    elif job_type == 'STORAGE' and system_description_type != 'STORAGE':
        resource_factor = 0.3  # Storage jobs prefer storage system_descriptions
    elif system_description_type == 'STORAGE' and job_type != 'STORAGE':
        resource_factor = 0.5  # Storage system_descriptions can handle other jobs but not optimal
    else:
        resource_factor = 0.5  # Default compatibility

    # 7. Site/System preference factor
    system_description_site = getattr(system_description, 'site', job_site)  # Assume system_description knows its site
    # FIXME that is not the case for the one above, I can fix it. 

    system_description_name = getattr(system_description, 'name', job_system)  # Assume system_description knows its name
    
    # Apply penalty for moving a job from its initial submission site. Higher if moved to a different site than to a
    # different system. (rational: account for network latency and data transfer cost)
    if job_site == system_description_site and job_system == system_description_name:
        site_factor = 1.0  # Perfect match: same site and system
    elif job_site == system_description_site:
        site_factor = 0.9  # Same site, different system
    else:
        site_factor = 0.7
        
    # 8. Combine all factors
    base_score = node_score * node_compat * resource_factor * time_factor * site_factor 
    final_bid = min(1.0, base_score * queue_factor)

    return round(final_bid, 2)

# bid = compute_bid(job_description, system_description, system_status, current_time=0)
# print("Computed bid with user/group factors:", bid)
