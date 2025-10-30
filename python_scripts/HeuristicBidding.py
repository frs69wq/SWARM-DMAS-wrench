def compute_bid(job_description, system_description, system_status, current_simulated_time=0):
    """Compute a heuristic bid score for a job on a given system_description.
    
    Args:
        job (dict): JobDescription "job_id", "user_id","group_id", "job_type",
            "submission_time", "walltime", "num_nodes", "needs_gpu", "requested_memory_gb", "requested_storage_gb",
            "hpc_site", "hpc_system"
        hpc_system (dict): HPC System description: "name", "type", "num_nodes",
            "memory_amount_in_gb", "storage_amount_in_gb", "has_gpu", "network_interconnect"
        system_status (dict): HPC System status: "current_num_current_num_available_nodes", "current_job_start_time_estimate", "queue_length"
        current_time (int): Current time in the scheduling system for wait time calculations.
    """

    # Safely read job fields (job is a dict)
    nodes_req = job_description["num_nodes"]
    requested_gpu = job_description["requested_gpu"]
    submission_time = job_description["submission_time"]
    job_type = job_description["job_type"]
    job_site = job_description["hpc_site"]
    job_system = job_description["hpc_system"]
 
    # 1. Feasibility check
    # if job needs more nodes than available AND requests GPU but system_description has no GPU => infeasible AND job
    # needs more memory than available.
    # Note: job expresses a total memory request, the system is described with a memory amount *per node*
    if (
        nodes_req > system_description["current_num_available_nodes"] 
        and requested_gpu 
        and not system_description["has_gpu"] 
        and job_description["requested_memoory_gb"] 
            > system_description["memory_amount_in_gb"] * system_description["num_nodes"]
    ):
        return 0.0

    # 2. Utilization-based scores
    used_nodes = system_description["num_nodes"] - system_status["current_num_available_nodes"]
    node_util = (used_nodes / system_description["num_nodes"]) if system_description["num_nodes"] else 0.0
    node_score = 1 - node_util

    # 3. Compatibility 
    node_compat = min(1.0, system_status["current_num_available_nodes"] / nodes_req) 

    # 4. Queue length factor
    queue_factor = max(0.1, 1 - 0.1 * system_status["queue_length"])

    # 5. Time-based priority factor (longer wait = higher priority) <-- COMMENT: Removed because the job gets broadcasted as soon as it is submitted
    # wait_time = current_time - submission_time
    # Scale wait time: 0-100 time units -> 1.0-2.0 multiplier
    # Rationale: increase the bid for jobs that have been waiting longer, up to a maximum factor of 2.0. 
    # time_factor = min(2.0, 1.0 + (wait_time / 100.0))

    # 6. Job-Resource compatibility factor
    system_description_type = system_description["type"]
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
    system_description_site = system_description["site"]
    system_description_name = system_description["name"]
    
    # 8. Apply penalty for moving a job from its initial submission site. Higher if moved to a different site than to a
    # different system. (rationale: account for network latency and data transfer cost)
    if job_site == system_description_site and job_system == system_description_name:
        site_factor = 1.0  # Perfect match: same site and system
    elif job_site == system_description_site:
        site_factor = 0.9  # Same site, different system
    else:
        site_factor = 0.7
        
    # 9. Delay penalty based on estimated job start time
    job_start_estimate = system_status['current_job_start_time_estimate']
    
    # Calculate delay penalty based on estimated start time
    estimated_delay = job_start_estimate - current_simulated_time
    # Apply penalty for longer delays - systems with longer queues get lower bids
    # Scale: 0-100 time units delay -> 1.0-0.1 multiplier (exponential decay)
    # Rationale: This penalty reduces the bid for systems that would start the job much later
    # Applies a linear penalty: Systems with longer delays get progressively lower bids
    # Please confirm
    # FIXME is exponential decay really computed here? Plus, the same max is computed twice
    delay_penalty = max(0.1, 1.0 - (estimated_delay / 100.0))
    delay_penalty = max(0.1, delay_penalty)  # Ensure minimum penalty of 0.1
    
    # 8. Combine all factors
    base_score = node_score * node_compat * resource_factor * site_factor * delay_penalty
    final_bid = min(1.0, base_score * queue_factor)

    return round(final_bid, 2)
