import argparse
import json
import os
import random
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Helpers
# ----------------------------
import xml.etree.ElementTree as ET

def _parse_radical_upper(radical: str) -> int:
    # "1-5312" -> 5312
    if "-" in radical:
        return int(radical.split("-", 1)[1].strip())
    return int(radical.strip())

def parse_site_configs_from_platform_xml(xml_path: str) -> dict:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    site_configs = {}

    for cluster in root.findall(".//cluster"):
        machine = cluster.attrib["id"]
        node_limit = _parse_radical_upper(cluster.attrib["radical"])

        props = {p.attrib["id"]: p.attrib.get("value", "") for p in cluster.findall("./prop")}
        site = props["site"]

        site_configs.setdefault(site, {"machines": {}})
        site_configs[site]["machines"][machine] = {
            "type": props["type"],
            "has_gpu": props["has_gpu"].strip().lower() == "true",
            "node_limit": node_limit,
            "memory_limit": float(props["memory_amount_in_gb"]),
            "storage_limit": float(props["storage_amount_in_gb"]),
        }

    return site_configs
def _normalize_probs(d: Dict[str, float]) -> Dict[str, float]:
    s = sum(float(v) for v in d.values())
    if s <= 0:
        raise ValueError("jobtype_proportions must sum to > 0")
    return {k: float(v) / s for k, v in d.items()}

def _parse_json_arg(s: str) -> Dict:
    try:
        return json.loads(s)
    except Exception as e:
        raise ValueError(f"Invalid JSON arg: {s}. Error: {e}")

def _scenario_short_frac(scenario: str) -> float:
    if scenario == "homogeneous_short":
        return 1.00
    if scenario == "only_large_long":
        return 0.00
    if scenario == "mixed_80_20":
        return 0.80
    if scenario == "mixed_20_80":
        return 0.20
    return 0.80

def _sample_uniform_int(rng: random.Random, lo: int, hi: int) -> int:
    lo = max(1, int(lo))
    hi = max(lo, int(hi))
    return rng.randint(lo, hi)

def _sample_lognormal_int(rng: random.Random, lo: int, hi: int, mu: float, sigma: float) -> int:
    lo = max(1, int(lo))
    hi = max(lo, int(hi))
    # stats.lognorm(s=sigma, scale=np.exp(mu))
    sample = int(round(rng.lognormvariate(mu, sigma)))
    # sample = stats.lognorm(s=sigma, scale=np.exp(mu)).rvs(random_state=rng)
    return max(lo, min(hi, sample))

def _format_scale_tag(value: float) -> str:
    return f"{float(value):g}".replace(".", "p")

def _busy_day_times_by_site(n_jobs: int, sites: list, seed: int = 42) -> np.ndarray:
    """
    Busy day: for each site, draw local-time submissions ~ Normal(noon, 4h),
    then map into a shared timeline with offsets (EST=0, CST=+1, PST=+3).
    Output is per-job times in SECONDS (aligned with input job order).
    """
    rng = np.random.default_rng(seed)
    hour = 3600.0
    tz = {'OLCF': 0.0, 'ALCF': 1.0 * hour, 'NERSC': 3.0 * hour}  # offsets in seconds

    sites_arr = np.array(sites, dtype=object)
    if len(sites_arr) != n_jobs:
        raise ValueError(f"Expected {n_jobs} sites, got {len(sites_arr)}")

    submission_times = np.empty(n_jobs, dtype=float)

    for site in np.unique(sites_arr):
        mask = (sites_arr == site)
        cnt = int(np.sum(mask))
        if cnt == 0:
            continue
        offset = tz.get(str(site), 0.0)
        peak = 12.0 * hour + offset
        t = rng.normal(loc=peak, scale=4.0 * hour, size=cnt)
        t = np.clip(t, offset, offset + 24.0 * hour)
        submission_times[mask] = t

    return submission_times

def _idle_day_times_by_site(n_jobs: int, sites: list, seed: int = 42) -> np.ndarray:
    """
    Idle/sparse: long gaps + small bursts (heavy-tailed inter-arrival).
    Site-aware version: sample local idle arrivals per site, then shift by timezone.
    Output is per-job times in SECONDS (aligned with input job order).
    """
    rng = np.random.default_rng(seed)
    hour = 3600.0
    tz = {'OLCF': 0.0, 'ALCF': 1.0 * hour, 'NERSC': 3.0 * hour}  # offsets in seconds

    sites_arr = np.array(sites, dtype=object)
    if len(sites_arr) != n_jobs:
        raise ValueError(f"Expected {n_jobs} sites, got {len(sites_arr)}")

    submission_times = np.empty(n_jobs, dtype=float)
    day_seconds = 24 * 3600.0

    for site in np.unique(sites_arr):
        mask = (sites_arr == site)
        cnt = int(np.sum(mask))
        if cnt == 0:
            continue

        offset = tz.get(str(site), 0.0)
        times = []
        t = 0.0

        while len(times) < cnt and t < day_seconds:
            if rng.random() < 0.7:
                gap = rng.lognormal(mean=4.0, sigma=0.8) * 60.0  # long gaps (seconds)
            else:
                gap = rng.exponential(scale=8.0) * 60.0          # short gaps (seconds)
            t += gap
            if t <= day_seconds:
                times.append(t)

        while len(times) < cnt:
            times.append(rng.uniform(0, day_seconds))

        local_times = np.array(sorted(times[:cnt]), dtype=float)
        submission_times[mask] = local_times + offset

    return submission_times

# ----------------------------
# Per-job-type size bands
# ----------------------------

# Global node bands across all job types
GLOBAL_NODE_BANDS: Dict[str, Tuple[int, int]] = {
    "small_nodes": (1, 256),
    "large_nodes": (257, 10624),
}

# Global node sampling config
# For log-normal: mode = exp(mu - sigma^2) -> mu = log(mode) + sigma^2
NODE_SAMPLING = {
    "dist": "lognormal",
    "sigma": 0.8,
    "desired_mode": 700, # calculated based on 512 peak for (64-10624) 
}

def _sample_nodes(
    rng: random.Random,
    size_class: str,
    node_bands: Optional[Dict[str, Tuple[int, int]]] = None,
    desired_mode: Optional[float] = None,
) -> int:
    bands = node_bands or GLOBAL_NODE_BANDS
    lo, hi = bands[size_class]

    # Requirement:
    # - small_nodes: uniformly sampled integers
    # - large_nodes: lognormal with peak near desired_mode
    if size_class == "small_nodes":
        return _sample_uniform_int(rng, lo, hi)

    sigma = float(NODE_SAMPLING.get("sigma", 0.8))
    mode = float(desired_mode if desired_mode is not None else NODE_SAMPLING.get("desired_mode", 512))
    mu = float(np.log(mode) + sigma ** 2)
    return _sample_lognormal_int(rng, lo, hi, mu, sigma)

JOB_TYPE_BANDS = {
    "HPC": {
        "short_wall": (0.25, 4),   # hours
        "long_wall": (12, 72),     # hours
        "small_storage": (50, 10_000),      # GB - small HPC jobs
        "large_storage": (5_000, 50_000)     
    },
    "AI": {
        "short_wall": (1, 12),
        "long_wall": (12, 120),
        "small_storage": (500, 50_000),     # GB - small AI jobs (datasets, models)
        "large_storage": (10_000, 200_000)  # GB - large AI jobs (large models, training data)

    },
    "HYBRID": {
        "short_wall": (1, 12),
        "long_wall": (12, 120),
        "small_storage": (100, 20_000),     # GB - small hybrid jobs
        "large_storage": (5_000, 100_000)   # GB - large hybrid jobs
    },
    "STORAGE": {
        "short_wall": (0.25, 6),
        "long_wall": (6, 24),
        "small_storage": (10_000, 100_000),  # GB - small storage jobs (still substantial)
        "large_storage": (50_000, 500_000)   # GB - large storage jobs (massive I/O)
    },
}

# ----------------------------
# Main generator
# ----------------------------

def generate_synthetic_jobs_v3(
    n_jobs: int = 100,
    seed: int = 42,
    day: str = "busy",  # busy|idle
    scenario: str = "mixed_80_20",
    jobs_per_site: Optional[Dict[str, int]] = None,
    jobtype_proportions: Optional[Dict[str, float]] = None,
    sfactor: float = 1.0,
) -> pd.DataFrame:

    # Apply scale divisors to capacities if provided.
    # Example: sfactor=8 -> node_limit = node_limit / 8
    sfactor = max(float(sfactor), 1e-9)

    if seed > 0:
        random.seed(seed)
        np.random.seed(seed)

    
    if sfactor != 1.0:
        site_configs = parse_site_configs_from_platform_xml(f"platforms/AmSC_scaled_down_{int(sfactor)}.xml")
    else:
        site_configs = parse_site_configs_from_platform_xml("platforms/AmSC.xml")

    
    # prettier print of platform config
    print("=== Original Platform Config ===")
    for site_name, site_cfg in site_configs.items():
        print(f"Site: {site_name}")
        for machine_name, machine_cfg in site_cfg["machines"].items():
            print(
                f"  Machine: {machine_name:22s} | "
                f"type={machine_cfg['type']:10s} | "
                f"GPU={'Yes' if machine_cfg['has_gpu'] else 'No ':3s} | "
                f"nodes={machine_cfg['node_limit']:5d} | "
                f"mem/node={machine_cfg['memory_limit']:6.1f} GB | "
                f"storage={machine_cfg['storage_limit']:.2e} GB"
            )
    
    # Adapt global/common node sampling to scaled platform limits
    max_scaled_nodes = max(
        machine_cfg["node_limit"]
        for site_cfg in site_configs.values()
        for machine_cfg in site_cfg["machines"].values()
    )
    # scale down small node bands proportionally, but keep at least 1 node
    # small_lo, small_hi = GLOBAL_NODE_BANDS["small_nodes"]
    small_lo = max(1, int(GLOBAL_NODE_BANDS["small_nodes"][0] / sfactor))
    small_hi = max(small_lo, int(GLOBAL_NODE_BANDS["small_nodes"][1] / sfactor))
    
    # scale down large_lo and large_hi proportionally, but keep at least 1 node
    large_lo = min(GLOBAL_NODE_BANDS["large_nodes"][0], small_hi + 1)
    large_hi = max(large_lo, int(max_scaled_nodes))
    scaled_node_bands = {
        "small_nodes": (small_lo, small_hi),
        "large_nodes": (large_lo, large_hi),
    }
    # edge case: if small_hi == small_lo, keep small_hi+1 and accordingly adjust large_lo to maintain the gap
    if small_hi == small_lo:
        small_hi = small_lo + 1
        large_lo = max(large_lo, small_hi + 1)
        scaled_node_bands["small_nodes"] = (small_lo, small_hi)
        scaled_node_bands["large_nodes"] = (large_lo, large_hi)

    
    # sanity check: ensure scaled bands fit within platform limits
    if large_hi < large_lo:
        raise ValueError(
            f"Invalid scaled node bands after applying sfactor={sfactor}: "
            f"large_hi ({large_hi}) < large_lo ({large_lo}). "
            f"Check platform limits and scaling logic."
        )
    
    # Keep desired mode semantics (peak near 512 on baseline), but scale with platform size
    base_mode = float(NODE_SAMPLING.get("desired_mode", 700))
    scaled_desired_mode = max(float(large_lo), min(float(large_hi), base_mode / sfactor))

    print("\n=== Node Sampling (Effective) ===")
    print(f"small_nodes band: {scaled_node_bands['small_nodes']}")
    print(f"large_nodes band: {scaled_node_bands['large_nodes']}")
    print(
        f"large_nodes lognormal sigma={NODE_SAMPLING.get('sigma', 0.8)}, "
        f"base_mode={base_mode}, scaled_mode={scaled_desired_mode:.2f}"
    )
    
    job_types = ['HPC', 'AI', 'HYBRID', 'STORAGE']

    if jobtype_proportions is None:
        jobtype_proportions = {"HPC": 0.3, "AI": 0.3, "HYBRID": 0.25, "STORAGE": 0.15}
    jobtype_proportions = _normalize_probs(jobtype_proportions)
    proportions = [jobtype_proportions[jt] for jt in job_types]
   
    # Scenario mixing
    short_frac = _scenario_short_frac(scenario)

    # Generate job types
    types = random.choices(job_types, weights=proportions, k=n_jobs)

    # Arrays
    job_ids = np.arange(1, n_jobs + 1)
    walltimes_min = np.zeros(n_jobs, dtype=int)
    nodes = np.zeros(n_jobs, dtype=int)
    memories = np.zeros(n_jobs, dtype=float)
    requested_gpus = np.zeros(n_jobs, dtype=bool)
    requested_storages = np.zeros(n_jobs, dtype=float)
    origin_sites = np.empty(n_jobs, dtype=object)
    origin_systems = np.empty(n_jobs, dtype=object)

    # Generate per job
    rng = random.Random(seed + 123)
    for i in range(n_jobs):
        jt = types[i]
        bands = JOB_TYPE_BANDS[jt]

        is_short = (rng.random() < short_frac)
        
        # GPU policy - determine first
        if jt == "AI":
            job_gpu = True
        elif jt == "STORAGE":
            job_gpu = False
        else:
            job_gpu = (rng.random() < (0.5 if jt == "HYBRID" else 0.3))  # hybrid more likely GPU than HPC


        if job_gpu: # ai, hpc, hybrid with gpu
            # GPU jobs: uniform across GPU systems
            available_systems = ['Frontier', 'Aurora', 'Perlmutter-Phase-1']
            # define per system weights (higher = more jobs)
            system_weights = {
                'Frontier': 1,           
                'Aurora': 1,             
                'Perlmutter-Phase-1': 0.8   
            }
            weights = [system_weights.get(sys, 1.0) for sys in available_systems]
            system = random.choices(available_systems, weights=weights, k=1)[0]
        else: # storage, hpc, hybrid without gpu
            # Non-GPU jobs: prefer CPU systems with weights based on system size
            # Define per-system weights (higher = more jobs)
            available_systems = ['Frontier', 'Andes', 'Aurora', 'Crux', 'Perlmutter-Phase-1','Perlmutter-Phase-2']
            system_weights = {
                'Perlmutter-Phase-2': 1.7,  # Largest CPU system (3072 nodes)
                'Andes': 1.5,               # Medium CPU system (704 nodes)
                'Crux': 1,                # Smallest CPU system (256 nodes)
                'Frontier': 1,           # GPU-capable but can be used for non-GPU jobs
                'Aurora': 1,             # GPU-capable but can be used for non-GPU jobs
                'Perlmutter-Phase-1': 1   # GPU-capable but can be used for non-GPU jobs
            }
            weights = [system_weights.get(sys, 1.0) for sys in available_systems]
            system = random.choices(available_systems, weights=weights, k=1)[0]
        
        # Find the site for the selected system
        for site_name, site_config in site_configs.items():
            if system in site_config["machines"]:
                site = site_name
                break

        # Get system capabilities
        caps = site_configs[site]["machines"][system]
        node_cap = int(caps["node_limit"])
        storage_cap = float(caps["storage_limit"])
        mem_per_node_cap = float(caps["memory_limit"])

        # Nodes (global/common bands across job types)
        size_class = "small_nodes" if is_short else "large_nodes"
        job_nodes = _sample_nodes(rng, size_class, node_bands=scaled_node_bands, desired_mode=scaled_desired_mode)
        job_nodes = min(job_nodes, node_cap)

        # Walltime (log-uniform), store in minutes
        wlo, whi = bands["short_wall"] if is_short else bands["long_wall"]
        wall_h = float(np.exp(np.random.uniform(np.log(wlo), np.log(whi))))
        wall_h = max(wlo, min(whi, wall_h))
        job_wall_min = int(round(wall_h * 60))

        # Storage (log-uniform) - use small/large bands based on is_short
        slo, shi = bands["small_storage"] if is_short else bands["large_storage"]
        storage = float(np.exp(np.random.uniform(np.log(slo), np.log(shi))))
        storage = min(storage, storage_cap)

        total_mem = job_nodes * mem_per_node_cap

        # Assign
        nodes[i] = job_nodes
        walltimes_min[i] = job_wall_min
        requested_gpus[i] = bool(job_gpu)
        requested_storages[i] = round(storage, 2)
        memories[i] = total_mem
        origin_sites[i] = site  
        origin_systems[i] = system  

    # Users/groups
    user_ids = np.random.randint(1, max(2, n_jobs // 2 + 1), size=n_jobs)
    group_ids = np.random.randint(1, max(2, n_jobs // 4 + 1), size=n_jobs)

     # Submission times
    if day == "busy":
        submission_times = _busy_day_times_by_site(n_jobs, origin_sites, seed=seed)  # seconds
    elif day == "idle":
        submission_times = _idle_day_times_by_site(n_jobs, origin_sites, seed=seed)  # seconds
    else:
        raise ValueError("day must be 'busy' or 'idle'")

    print("\n=== Submission Time Sampling ===")
    print(
        f"unit=seconds, min={submission_times.min():.3f}, "
        f"max={submission_times.max():.3f}, mean={submission_times.mean():.3f}"
    )

    
    # DEBUG: Analyze system distribution
    print("\n=== System Distribution Analysis ===")
    system_counts = pd.Series(origin_systems).value_counts()
    print("\nSystem counts:")
    for system, count in system_counts.items():
        pct = (count / n_jobs) * 100
        print(f"  {system:25s}: {count:4d} ({pct:5.1f}%)")
    
    # Break down by job type
    print("\n=== Distribution by Job Type ===")
    for jt in job_types:
        jt_mask = [types[i] == jt for i in range(n_jobs)]
        jt_systems = [origin_systems[i] for i in range(n_jobs) if jt_mask[i]]
        jt_gpus = [requested_gpus[i] for i in range(n_jobs) if jt_mask[i]]
        
        print(f"\n{jt} jobs: {len(jt_systems)} total")
        if len(jt_systems) > 0:
            sys_counts = pd.Series(jt_systems).value_counts()
            for sys, cnt in sys_counts.items():
                # Count GPU jobs for this system
                gpu_for_sys = sum(1 for i in range(n_jobs) if types[i] == jt and origin_systems[i] == sys and requested_gpus[i])
                pct = (cnt / len(jt_systems)) * 100
                print(f"  {sys:25s}: {cnt:4d} ({pct:5.1f}%), GPU: {gpu_for_sys}/{cnt}")
            
            gpu_count = sum(jt_gpus)
            print(f"  Total GPU jobs: {gpu_count}/{len(jt_systems)} ({100*gpu_count/len(jt_systems):.1f}%)")
    
    df = pd.DataFrame({
        'JobID': job_ids,
        'SubmissionTime': np.round(submission_times, 3),  # seconds
        'Walltime': walltimes_min*60,                        # seconds
        'Nodes': nodes,
        'MemoryGB': memories,
        'RequestedGPU': requested_gpus,
        'RequestedStorageGB': requested_storages,
        'JobType': types,
        'UserID': user_ids,
        'GroupID': group_ids,
        'HPCSite': np.array(origin_sites, dtype=object),
        'HPCSystem': np.array(origin_systems, dtype=object),
    })

    # Ensure jobs are ordered by submission time and IDs follow that order.
    df = df.sort_values(by="SubmissionTime", kind="mergesort").reset_index(drop=True)
    df["JobID"] = np.arange(1, len(df) + 1)

    return df

# ----------------------------
# CLI
# ----------------------------

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic jobs for simulation')

    parser.add_argument('--n_jobs', type=int, default=32_000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--day', type=str, default='busy', choices=['busy', 'idle'])
    parser.add_argument('--scenario', type=str, default='mixed_80_20',
                        choices=['homogeneous_short', 'only_large_long', 'mixed_80_20', 'mixed_20_80'])
    parser.add_argument('--jobtype_proportions', type=str, default='',
                        help='JSON string, e.g. \'{"HPC":0.3,"AI":0.3,"HYBRID":0.25,"STORAGE":0.15}\'')
    parser.add_argument("--sfactor", type=float, required=False, default=1.0, help="Radical scale divisor (default: 1.0)")

    args = parser.parse_args()

    jobtype_proportions = _parse_json_arg(args.jobtype_proportions) if args.jobtype_proportions else None

    # Adjust number of jobs based on day type and scaling factor
    if args.sfactor != 1.0:
        if args.day == "busy":
            # Busy day: scale down proportionally to platform size
            args.n_jobs = max(1, int(args.n_jobs / args.sfactor))
        elif args.day == "idle":
            # Idle day: keep more jobs to avoid long inter-arrival times
            args.n_jobs = max(1, int(args.n_jobs / (args.sfactor)))
    elif args.day == "idle":
        # For idle day with no scaling, we can still reduce jobs to avoid excessively long gaps
        args.n_jobs = max(1, int(args.n_jobs / 1))

    print(f"Generating {args.n_jobs} jobs , day={args.day}, sfactor={args.sfactor}")

    df = generate_synthetic_jobs_v3(
        n_jobs=args.n_jobs,
        seed=args.seed,
        day=args.day,
        scenario=args.scenario,
        jobtype_proportions=jobtype_proportions,
        sfactor=args.sfactor,
    )

    os.makedirs("./data", exist_ok=True)
    r_tag = _format_scale_tag(args.sfactor)
 
    out = f"./data/{args.day}_{args.scenario}_{len(df)}_r{r_tag}.json"
    with open(out, "w") as f:
        json.dump(df.to_dict("records"), f, indent=2)

    print(f"Generated {len(df)} jobs.")
    print(f"Saved: {out}")

if __name__ == "__main__":
    main()
