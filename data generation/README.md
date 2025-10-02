# Synthetic HPC Job Generator

This tool generates synthetic HPC (High-Performance Computing) job datasets for scheduler simulation and analysis. The generator creates realistic workloads with various job characteristics across different scenarios, with **site-specific resource constraints**.

## Usage

```bash
python data_gen.py --scenario <scenario_name> --n_jobs <number> [options]
```

### Command Line Arguments

- `--scenario`: Workload scenario type (required)
- `--n_jobs`: Number of jobs to generate (default: 100)
- `--seed`: Random seed for reproducibility (default: 42, -1 for no seed)

### Available Scenarios

- `homogeneous_short`: Consistent short-duration jobs
- `heterogeneous_mix`: Mixed workload with varied requirements
- `long_job_dominant`: Mix with occasional very long jobs
- `high_parallelism`: High-node, compute-intensive workloads
- `resource_sparse`: Minimal resource requirements
- `bursty_idle`: Alternating burst and idle phases
- `adversarial`: Stress test with one extremely demanding job

## HPC Site Configurations

Jobs are randomly assigned to HPC sites with different resource limits:

| Site | Machine | Node Limit | Memory Limit (TB) | Storage Limit (PB) | Description |
|------|------|-----------|-------------------|-------------------|-------------|
| **ALCF** | Polaris | 560 | 38 | 1120 | Argonne Leadership Computing Facility |
| **OLCF** | Frontier |9408 | 4600 | 695 | Oak Ridge Leadership Computing Facility |
| **NERSC** | Perlmutter |4864 | 2312 | 44 | National Energy Research Scientific Computing Center |

**Important**: All resource requests (Nodes, memory, storage) are constrained by the assigned site's limits using `min(requested_value, site_limit)`.

## Generated Variables

Each job includes the following attributes:

| Variable | Type | Description |
|----------|------|-------------|
| `JobID` | Integer | Unique job identifier (1, 2, 3, ...) |
| `SubmissionTime` | Integer | Job arrival time (time units) |
| `Walltime` | Integer | Job execution duration (time units) |
| `Nodes` | Integer | Number of nodes requested (site-constrained) |
| `MemoryGB` | Integer | Memory requirement in GB (site-constrained) |
| `RequestedGPU` | Boolean | Whether job requires GPU access |
| `RequestedStorageGB` | Integer | Storage requirement in GB (site-constrained) |
| `JobType` | String | HPC system type: HPC, AI, HYBRID, GPU, MEMORY, STORAGE |
| `UserID` | String | User identifier (user_1, user_2, ...) |
| `GroupID` | String | Group identifier (group_1, group_2, ...) |
| `HPCSite` | String | HPC site name (ALCF, OLCF, NERSC) |

## Statistical Distributions by Scenario

**Note**: All resource values are subject to site-specific constraints: `min(generated_value, site_limit)`

### 1. Homogeneous Short (`homogeneous_short`)
*Consistent short-duration jobs with minimal resource variation*

**Submission Times:**
```
SubmissionTime ~ Gamma(n, λ=0.05)  # where n = job_index + 1
```

**Job Characteristics:**
```
Walltime ~ DiscreteUniform(30, 120)
Nodes ~ PointMass(2)
MemoryGB ~ PointMass(4)
RequestedGPU ~ Bernoulli(p=0.1)
RequestedStorageGB ~ min(Gamma(shape=2, scale=5), site_storage_limit)
JobType ~ Categorical(HPC: 70%, AI: 20%, MEMORY: 10%)
HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

### 2. Heterogeneous Mix (`heterogeneous_mix`)
*Varied workload with diverse resource requirements*

**Submission Times:**
```
SubmissionTime ~ Gamma(n, λ=0.10)
```

**Job Characteristics:**
```
Walltime ~ Gamma(shape=1.5, scale=300)
Nodes ~ min(2^DiscreteUniform(1, min(log₂(site_node_limit), 8)), site_node_limit)
MemoryGB ~ min(2^DiscreteUniform(1, min(log₂(site_memory_limit), 7)), site_memory_limit)
RequestedGPU ~ Bernoulli(p=0.3)
RequestedStorageGB ~ min(LogNormal(μ=3, σ=1.5), site_storage_limit)
JobType ~ Categorical(HPC: 25%, AI: 20%, HYBRID: 15%, GPU: 15%, MEMORY: 15%, STORAGE: 10%)
HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

### 3. Long Job Dominant (`long_job_dominant`)
*Bimodal distribution with occasional very long jobs*

**Submission Times:**
```
SubmissionTime ~ Gamma(n, λ=0.125)
```

**Job Characteristics:**
```
Walltime ~ MixtureDistribution:
  - 20%: DiscreteUniform(10000, 50000)  # Long jobs
  - 80%: DiscreteUniform(100, 500)      # Short jobs

Nodes ~ ConditionalDistribution:
  - min(128, site_node_limit) if walltime > 10000
  - 2 if walltime ≤ 10000

MemoryGB ~ min(Nodes × DiscreteUniform(2, 8), site_memory_limit)

RequestedGPU ~ ConditionalBernoulli:
  - Bernoulli(p=0.8) if walltime > 10000
  - Bernoulli(p=0.1) if walltime ≤ 10000

RequestedStorageGB ~ ConditionalGamma:
  - min(Gamma(shape=3, scale=200), site_storage_limit) if walltime > 10000
  - min(Gamma(shape=1.5, scale=10), site_storage_limit) if walltime ≤ 10000

JobType ~ ConditionalCategorical:
  - Categorical(HPC: 60%, HYBRID: 30%, MEMORY: 10%) if walltime > 10000
  - Categorical(HPC: 50%, AI: 30%, GPU: 20%) if walltime ≤ 10000

HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

### 4. High Parallelism (`high_parallelism`)
*High-node, compute-intensive workloads*

**Submission Times:**
```
SubmissionTime ~ Gamma(n, λ=0.20)
```

**Job Characteristics:**
```
Walltime ~ Gamma(shape=1, scale=800)  # Exponential distribution
Nodes ~ min(2^DiscreteUniform(6, min(log₂(site_node_limit), 9)), site_node_limit)
MemoryGB ~ min(Nodes × ContinuousUniform(2, 6), site_memory_limit)
RequestedGPU ~ Bernoulli(p=0.6)
RequestedStorageGB ~ min(Nodes × ContinuousUniform(1, 5), site_storage_limit)
JobType ~ Categorical(GPU: 40%, HYBRID: 30%, HPC: 20%, AI: 10%)
HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

### 5. Resource Sparse (`resource_sparse`)
*Minimal resource requirements*

**Submission Times:**
```
SubmissionTime ~ Gamma(n, λ=0.083)
```

**Job Characteristics:**
```
Walltime ~ DiscreteUniform(30, 300)
Nodes ~ PointMass(1)
MemoryGB ~ min(DiscreteUniform(1, 8), site_memory_limit)
RequestedGPU ~ Bernoulli(p=0.05)
RequestedStorageGB ~ min(Exponential(scale=2) + 1, site_storage_limit)
JobType ~ Categorical(HPC: 60%, MEMORY: 30%, AI: 10%)
HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

### 6. Bursty Idle (`bursty_idle`)
*Alternating burst and idle phases*

**Submission Times:**
```
SubmissionTime ~ Gamma(n, λ=0.167) + BurstDelay
where BurstDelay = 50 for non-burst phases, 0 for burst phases
```

**Job Characteristics:**
```
Walltime ~ Gamma(shape=1, scale=600)  # Exponential distribution
Nodes ~ min(2^DiscreteUniform(1, min(log₂(site_node_limit), 6)), site_node_limit)
MemoryGB ~ min(Nodes × DiscreteUniform(1, 4), site_memory_limit)

RequestedGPU ~ ConditionalBernoulli:
  - Bernoulli(p=0.4) during burst phases
  - Bernoulli(p=0.1) during idle phases

RequestedStorageGB ~ ConditionalGamma:
  - min(Gamma(shape=2, scale=30), site_storage_limit) during burst phases
  - min(Gamma(shape=1, scale=8), site_storage_limit) during idle phases

JobType ~ ConditionalCategorical:
  - Categorical(AI: 40%, GPU: 40%, HYBRID: 20%) during burst phases
  - Categorical(HPC: 50%, MEMORY: 30%, STORAGE: 20%) during idle phases

HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

### 7. Adversarial (`adversarial`)
*Stress test with one extremely demanding job*

**Submission Times:**
```
SubmissionTime ~ Deterministic: {0, 1, 2, 3, ..., n-1}
```

**Job Characteristics:**
```
Walltime ~ PointMassWithOutlier:
  - 100000 for Job[0]
  - 60 for all other jobs

Nodes ~ PointMassWithOutlier:
  - min(128, site_node_limit) for Job[0]
  - 1 for all other jobs

MemoryGB ~ PointMassWithOutlier:
  - min(256, site_memory_limit) for Job[0]
  - 4 for all other jobs

RequestedGPU ~ PointMassWithOutlier:
  - True for Job[0]
  - False for all other jobs

RequestedStorageGB ~ PointMassWithOutlier:
  - min(10000, site_storage_limit) for Job[0]
  - 1 for all other jobs

JobType ~ PointMassWithOutlier:
  - 'HYBRID' for Job[0]
  - 'HPC' for all other jobs

HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

## Site-Dependent Resource Constraints

All scenarios apply site-specific limits to resource requests:

| Resource | ALCF Limit | OLCF Limit | NERSC Limit | Constraint Applied |
|----------|------------|------------|-------------|-------------------|
| Nodes | 560 | 1024 | 768 | `min(requested_nodes, site_node_limit)` |
| Memory (GB) | 68 | 128 | 96 | `min(requested_memory, site_memory_limit)` |
| Storage (GB) | 200 | 500 | 300 | `min(requested_storage, site_storage_limit)` |

## User and Group Assignments

```
UserID ~ DiscreteUniform(1, max(2, n_jobs // 2 + 1))
GroupID ~ DiscreteUniform(1, max(2, n_jobs // 4 + 1))
HPCSite ~ DiscreteUniform({ALCF, OLCF, NERSC})
```

## Output

The generator creates CSV files in the `data/` directory with the naming convention:
```
{scenario}_{n_jobs}.csv
```

Example: `heterogeneous_mix_100.csv`

## Example Usage

```bash
# Generate 1000 heterogeneous jobs across different HPC sites
python data_gen.py --scenario heterogeneous_mix --n_jobs 1000

# Generate adversarial workload for stress testing
python data_gen.py --scenario adversarial --n_jobs 100

# Generate high-parallelism workload
python data_gen.py --scenario high_parallelism --n_jobs 500

# Generate resource-sparse workload with reproducible seed
python data_gen.py --scenario resource_sparse --n_jobs 200 --seed 123
```

## Statistical Properties Summary

**Note**: All ranges are subject to site-specific constraints

| Scenario | Job Duration | Node Range* | Memory Range* | GPU Rate | Storage Range* | Primary Job Types |
|----------|-------------|------------|---------------|----------|----------------|-------------------|
| homogeneous_short | 30-120 | {2} | {4} | 10% | 1-30 GB | HPC, AI |
| heterogeneous_mix | Variable | 2-site_limit | 2-site_limit | 30% | 1-site_limit | Balanced mix |
| long_job_dominant | Bimodal | {2, min(128,site_limit)} | 4-site_limit | 10-80% | 5-site_limit | HPC, HYBRID |
| high_parallelism | Exponential | 64-site_limit | 128-site_limit | 60% | 64-site_limit | GPU, HYBRID |
| resource_sparse | 30-300 | {1} | 1-min(8,site_limit) | 5% | 1-site_limit | HPC, MEMORY |
| bursty_idle | Exponential | 2-site_limit | 2-site_limit | 10-40% | 2-site_limit | Phase-dependent |
| adversarial | Extreme outlier | {1, min(128,site_limit)} | {4, min(256,site_limit)} | 1% | {1, min(10000,site_limit)} | HPC, HYBRID |

*Actual values constrained by: ALCF (560 nodes, 68 GB mem, 200 GB storage), OLCF (1024 nodes, 128 GB mem, 500 GB storage), NERSC (768 nodes, 96 GB mem, 300 GB storage)


## Notes

- All time units are abstract 
- Memory (TB) and Storage (PB)
- Node values represent compute nodes (without login nodes)
- **Site assignment is random** - each job is independently assigned to ALCF, OLCF, or NERSC
- **Resource constraints are strictly enforced** - no job can exceed its assigned site's limits
- The generator creates realistic heterogeneity by varying both scenario patterns and site constraints
- Job types (HPC, AI, HYBRID, GPU, MEMORY, STORAGE) indicate intended system requirements
- Random seeds ensure reproducible datasets for consistent testing and comparison
