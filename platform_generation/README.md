# FABRIC platform generation

The WAN topology is generated independently from resource population:

```bash
python platform_generation/generate_fabric_wan.py \
  --input-snapshot platform_generation/fabric_data/us_topology.json \
  --sites ATLA CLEM DALL EDUKY FIU GATECH GPN HAWI INDI KANS LOSA MASS MAX MICH NCSA NEWY PRIN PSC RUTG SALT SEAT SRI STAR TACC UCSD UTAH WASH \
  --default-latency-ms 10 \
  --output platform_generation/generated/fabric_wan.json \
  --manifest-out platform_generation/generated/fabric_wan_manifest.json
```

Populate a WAN JSON with facility and cluster resources using the fixed-value
specification:

```bash
python platform_generation/populate_fabric_platform.py
```

The default input and output paths are:

- WAN input: `platform_generation/generated/fabric_wan.json`
- Resource spec: `platform_generation/fabric_resource_spec.json`
- Populated output: `platform_generation/generated/fabric_platform.json`

Pass `--wan`, `--spec`, and `--output` to select other files. The population
script does not modify either input.

The [SWARM extension runner](runner/README.md) checks the generated JSON through
WRENCH's real platform-loading entry point. Use the repository's
[WRENCH compatibility build](compat/wrench/README.md) to load the existing FSMod
JBOD stores and attach storage services to those same filesystems. Loading and
resource checks pass for both two-facility fixtures and the full platform.
Unpatched WRENCH rejects the member disks as having duplicate default `/` mounts.

## Resource specification

`fabric_resource_spec.json` uses schema version 1. Its initial `small` profile
and facility defaults are fixed values to keep generation deterministic; they
are an editable starting configuration, not measurements of deployed FABRIC
resources.

- `defaults.facility_control_host` describes one control host per facility.
  Its `memory_gb` value specifies RAM for that host.
- `defaults.host_network` provides the JSON loader's required cluster-zone
  backbone, private-link, and loopback settings for those one-host zones.
- `defaults.facility_shared_storage` defines facility-wide storage. Storage
  performance and capacity are specified together; the generator places
  performance fields in `storage_systems` and translates `capacity` to the
  filesystem's `size`.
  `server_memory_gb` specifies the storage server's RAM separately from its
  filesystem capacity. The same field is required for cluster scratch servers.
- `defaults.facility_network` defines facility-local inter-zone link
  bandwidth and latency.
- `defaults.wan_gateway_access` optionally defines one shared WAN access link
  per facility. A facility's `wan_gateway_access` override can replace its
  bandwidth or latency. These are modeling assumptions, not measurements.
  Files using this extension require the repository-local loader below.
- `defaults.cluster_generation` provides the profile, count, and name template
  used for any facility that omits `clusters`. Template placeholders are
  `{facility}`, `{profile}`, and `{index}`; the generated cluster index starts
  at 1.
- `cluster_profiles` contains reusable management-host, compute-node, scratch,
  and network resources. `compute_nodes.count` does not include the separate
  management host.
  Both `management_host.memory_gb` and `compute_nodes.memory_gb` are required;
  compute memory is the capacity of each compute host.
- `facilities` optionally overrides facility storage or network fields and
  provides explicit cluster lists. An explicit list replaces the generated
  default list. Every WAN facility must end up with at least one cluster.
- A cluster instance uses `{"name": "...", "profile": "small"}` and may add an
  `overrides` object. Overrides merge recursively: omitted keys inherit from
  the profile; arrays and scalar values replace their parent value.

The generator rejects unknown facility names, unknown profile references,
malformed resource values, duplicate generated zone/link/filesystem names,
and clusters that produce invalid resource counts.

## Memory metadata

The development specification uses the following modeling assumptions:

| Host role | RAM per host |
|---|---:|
| Facility controller | 8 GB |
| Cluster management | 32 GB |
| Compute node | 256 GB |
| Facility or scratch storage server | 64 GB |

These values are editable profiles, not measurements of FABRIC hosts. Facility
and cluster overrides use the normal recursive merge rules. RAM values must be
positive integers no greater than 2,147,483,647 GB, matching the existing SWARM
integer field. GB means exactly 1,000,000,000 bytes.

The populated JSON carries `node.memory_gb` on cluster zones and
`server_memory_gb` on storage systems. Our local loader translates these to the
SimGrid host property `mem` with an explicit `GB` unit, which WRENCH uses as RAM
capacity. New generation requires RAM for every host. Legacy JSON documents
without RAM remain accepted by the loader and retain WRENCH's unlimited default.

Cluster-zone `properties` are objects of strings and are applied by the local
loader. Generated zones identify `role` and `site`; management and compute zones
also identify their logical `cluster_id`. Compute zones publish
`memory_amount_in_gb`, the per-compute-node value read by SWARM's cluster-property
API. The loader rejects disagreement between that property and `node.memory_gb`.

`swarm.memory` declares units and scope. The existing workload generator records
`MemoryGB` / `requested_memory_gb` as total memory across a job's requested compute
nodes. Cluster capacity metadata remains per compute node. For example, eight
256 GB compute nodes provide 2,048 GB compute memory, with management/controller
RAM accounted for separately.

The extension runner reports every host's WRENCH `memory_bytes` and verifies the
full RAM path in `memory_metadata`. It reads the per-node field through the same
cluster-property API as SWARM and selects compute hosts from explicit deployment
mappings, preserving all eight execution hosts. The full platform has 216 compute
hosts and 55,296 GB compute RAM; all 324 hosts together have 59,832 GB RAM.

This is resource metadata validation. Agent deployment, the remaining cluster
attributes needed by `HPCSystemDescription`, and job memory admission per
allocation remain subsequent work. The existing legacy acceptance test still
compares a request with aggregate cluster capacity.

## Platform zones, storage, and network boundaries

The loader supports clusters of uniform nodes and shared storage systems; it
does not have a dedicated facility-agent or heterogeneous-host schema. The
generator represents each facility control host and each cluster management
host as a one-node cluster zone. Compute nodes form a separate cluster zone.
Consequently, a management host is not counted as a compute node.

Each facility has one shared facility-storage zone. Each logical cluster gets
its own shared scratch storage zone and filesystem. Scratch is deliberately
not represented with a filesystem `"cluster"` field, which the loader expands
into per-node partitions.

`facility_network` links the facility control-host zone to the facility shared
storage zone and to each cluster management-host zone. A cluster profile's
`network.access` links its management-host zone to its compute zone and shared
scratch zone. The generator computes routes between all facility-local zones
over these links. Thus the two network sections have disjoint direct links,
with the management-host zone as their boundary.

Local and WAN route records are symmetric: each unordered pair has one route
record. The JSON loader creates the reverse route automatically, traversing the
same physical links in reverse order. Emitting a second record for the reverse
direction would cause a duplicate-route error during loading.

The WAN platform supplies inter-facility links and routes between facility
zones. The population step preserves those top-level physical resources.
When `wan_gateway_access` is configured, it adds one local shared access link
and a `wan_gateway` description per facility. The gateway attaches at the
control-host zone, and all child zones receive explicit paths to it. Existing
local paths, hosts, and storage resources are preserved.

The original external loader cannot consume gateway descriptions. Use the
[repository-local loader](loader/README.md) for these files. Documents generated
without gateway access retain the original loader schema and its missing WAN
gateway connectivity.

## Two-facility gateway fixture

The ATLA/DALL fixture is separate from the previously generated full platform:

```bash
python3 platform_generation/generate_fabric_wan.py \
  --input-snapshot platform_generation/fabric_data/us_topology.json \
  --sites ATLA DALL --strict --default-latency-ms 10 \
  --output platform_generation/generated/two_facilities/fabric_wan.json \
  --manifest-out platform_generation/generated/two_facilities/fabric_wan_manifest.json
python3 platform_generation/populate_fabric_platform.py \
  --wan platform_generation/generated/two_facilities/fabric_wan.json \
  --output platform_generation/generated/two_facilities/fabric_platform.json
```

The fallback is explicitly supplied because the WAN script validates all input
links before subset routing. The ATLA/DALL link itself uses its archived direct
measurement. The fixture has 24 hosts, 24 disks, 4 filesystems and 77 configured
links (including one physical WAN link and two WAN access links). SimGrid adds
one built-in loopback. The full platform uses the same gateway schema, described
below.

To inspect this fixture after building the repository-local loader:

```bash
PLATFORM_CONFIG="$PWD/platform_generation/generated/two_facilities/fabric_platform.json" \
/path/to/simgrid_platform_inspector \
  --platform "$PWD/build/facility-loader/libswarm_platform.so" \
  --show_hosts --show_disks --show_links
```

Use the installed Inspector path in place of the placeholder. The local loader
retains its module through engine cleanup. Inspecting older files with the
external loader may still require `LD_PRELOAD=/path/to/libfsmod.so` to avoid a
separate shutdown crash.

## Multi-hop gateway fixture

ATLA/CLEM uses two physical links through WASH. Only ATLA and CLEM receive
facility resources; WASH is recorded as a transit site in the WAN manifest.
The fixture is separate from both the direct ATLA/DALL fixture and the full
platform:

```bash
python3 platform_generation/generate_fabric_wan.py \
  --input-snapshot platform_generation/fabric_data/us_topology.json \
  --sites ATLA CLEM --strict --default-latency-ms 10 \
  --output platform_generation/generated/multihop_facilities/fabric_wan.json \
  --manifest-out platform_generation/generated/multihop_facilities/fabric_wan_manifest.json
python3 platform_generation/populate_fabric_platform.py \
  --wan platform_generation/generated/multihop_facilities/fabric_wan.json \
  --output platform_generation/generated/multihop_facilities/fabric_platform.json
```

This fixture has 24 hosts, 24 disks, 4 filesystems and 78 configured links,
plus SimGrid's built-in loopback. Its two physical WAN links are
`fabric_ATLA_WASH` (1200 Gbps, 6.368 ms) and `fabric_CLEM_WASH` (40 Gbps,
8.1845 ms). Both latencies use archived direct measurements; the generated
route lists those same shared resources in order, with reversed order for
return traffic. The sum of physical WAN latencies is 14.5525 ms, before local
network and gateway access delays.

The loader CTest suite checks both fixtures, including all directed host routes
and remote storage I/O. Its multi-hop contention check sends two independent
compute-host flows through the same 40 Gbps physical link and verifies their
aggregate transfer rate stays within that shared capacity. No resource values
are changed to create the test. Only that contention check uses a larger
simulated TCP window to avoid the default transport window masking link
contention; the loader README records the exact runtime setting.

## Full 27-facility platform

`generated/fabric_platform.json` is populated with gateway support from the
existing full WAN input and resource specification:

```bash
python3 platform_generation/populate_fabric_platform.py
cmake --build build/facility-loader -j 2
ctest --test-dir build/facility-loader -R '^full_' --output-on-failure
```

The archived topology has 28 sites, but EDC has no physical connection. The
selected full WAN therefore contains 27 connected facilities; no synthetic
attachment for EDC is introduced.

The populated platform contains 324 hosts (27 controllers, 27 managers,
216 compute nodes and 54 storage servers), 324 disks, 54 filesystems, and
1059 configured links plus SimGrid's built-in loopback. All 33 physical WAN
links and 351 facility-pair paths are preserved. Gateway access adds one shared
link per facility; existing local routes and compute/storage definitions are
unchanged.

The full checks audit every resource and filesystem capacity, then all 104652
directed host-to-host routes for link identity, order, and summed latency.
They also perform controller round-trip transfers covering all 27 facilities
and remote simulated writes/reads against all 54 filesystems. Clients obtain
filesystem handles explicitly by stable ID. These are platform checks; SWARM
agent startup and scheduling behavior are separate work.

The separate `full_cross_facility_pair_contention` check runs ATLA-to-CLEM and
DALL-to-CLEM compute-host transfers concurrently. Both routes use the same
`fabric_CLEM_WASH` physical link object. It measures each flow alone, starts both
together, then verifies that each slows down and their aggregate rate stays
within the shared 40 Gbps capacity. It uses the documented larger TCP window
for this isolated contention check; platform resource definitions are unchanged.

Full audit evidence is written under `build/platform-validation/full-platform/`.
CTest's detailed remote-access coverage is under
`build/facility-loader/full-platform-coverage/`. Both locations are ignored
build artifacts, not experiment results to commit.

The output includes a `swarm` metadata object that maps facility agents to
control hosts and scheduling agents to cluster management hosts, along with
the corresponding compute cluster and scratch storage names. The generic
SimGrid JSON loader constructs the referenced platform resources but does not
instantiate SWARM agents; SWARM startup must consume this metadata to deploy
them.

No random resource ranges are used in schema version 1, so a generation seed
is not applicable. If resource bands or random sampling are added later, the
seed should be part of the specification and recorded in generated metadata.
