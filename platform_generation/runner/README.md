# SWARM extension runner

This is the entry point for integrating the generated platform with SWARM's
WRENCH lifecycle. The current `--check-platform` mode initializes WRENCH, loads
the repository-local JSON loader through `Simulation::instantiatePlatform()`,
and reports the resources visible to WRENCH, SimGrid, and FSMod. Agent deployment
and workload execution are subsequent stages. The existing `src/swarm_dmas.cpp`
entry point is independent of this runner.

## Build

First build the [patched WRENCH dependency](../compat/wrench/README.md). Then,
from the repository root:

```bash
cmake -S platform_generation/runner -B build/extension-runner-compat \
  -DSIMGRID_PATH=/path/to/simgrid-install \
  -DFSMOD_PATH=/path/to/fsmod-install \
  -DWRENCH_PATH="$PWD/build/wrench-compat-v1/install" \
  -DCMAKE_PREFIX_PATH=/path/to/dependency-prefix
cmake --build build/extension-runner-compat -j 2
```

Use `-DCMAKE_CXX_COMPILER` for the compiler matching the installed dependencies.
The local Conda environment also needs
`-DCMAKE_EXE_LINKER_FLAGS=-Wl,-rpath-link,/path/to/conda/env/lib`.
The runner's build includes the local loader at
`build/extension-runner-compat/loader/libswarm_platform.so`. Its default loader path is
set by CMake. WRENCH is a required dependency of this separate build; building
the loader alone still only requires SimGrid and FSMod.

## Check a platform

```bash
build/extension-runner-compat/swarm_extension \
  --check-platform \
  --platform platform_generation/generated/two_facilities/fabric_platform.json \
  --report build/extension-runner-compat/two-facilities-load.json
```

Select the full platform with
`--platform platform_generation/generated/fabric_platform.json`.
`--loader /absolute/path/to/libswarm_platform.so` explicitly selects another
build of the loader. Relative paths are resolved against the current working
directory. The selected JSON is passed using the loader's `PLATFORM_CONFIG`
environment variable, overriding a stale inherited value. `--cfg=...` and
`--log=...` arguments are forwarded to WRENCH/SimGrid. The report's parent
directory must already exist.

The console prints a summary. `--report` saves the complete JSON inventory of
host cores/speeds/properties, disk read/write bandwidth and raw mount properties,
link bandwidth/latency/sharing, and FSMod filesystem capacity/free space. It also
checks that WRENCH and SimGrid expose the same host IDs. Inventory collection is
read-only. A report cannot overwrite the selected platform or loader.

Exit code 0 means WRENCH accepted the platform and inventory collection finished.
Exit code 1 means an input, loading, inventory, or reporting error. The report
includes the failure stage and original exception. For a sanity-check failure
after resources were created, it also attempts a diagnostic inventory. That
inventory does not establish successful WRENCH acceptance. The report identifies
the WRENCH header version used for the build and explicitly states whether
agents were attached or the simulation was launched. `wrench_fsmod_compat_version`
is 1 with the local compatibility patch and 0 with unpatched WRENCH.

For platforms with `swarm.memory`, the runner also checks every host's configured
RAM against `Simulation::getHostMemoryCapacity()` and reports the actual byte
capacity in the inventory. `memory_metadata` checks the same per-node cluster
property used by SWARM, compute-node membership/counts, and aggregate compute
memory. It excludes management and controller RAM from compute totals. GB is
decimal; memory scope and profile defaults are documented in the parent README.

## WRENCH and FSMod storage compatibility

Unpatched WRENCH exits with code 1 at `instantiate-platform` with this error:

```text
Simulation::platformSanityCheck(): Host cluster_ATLA_small_1_scratch_server has two disks with the same mount point '/'
```

The local JSON loader creates real SimGrid disks and combines them into FSMod
JBOD stores. Filesystem partitions define the capacity and mount path of each
store. The raw member disks have no individual `mount` property. The installed
WRENCH sanity check interprets every missing disk mount as `/` and rejects
multiple disks on the same storage host. This happens before agents or storage
services can be attached.

The local compatibility patch adds explicit validation for the actual FSMod
filesystem and its backing disks. The runner loads the module and registers
those objects inside WRENCH's platform-creation callback, before sanity checking.
Ordinary disks retain their existing mount checks. Both two-facility platforms
and the full platform now load successfully with this build.

The loaded inventories match the existing SimGrid resource audit for all
three generated fixtures:

| Platform | Hosts | Disks | Filesystems | Links including built-in loopback |
|---|---:|---:|---:|---:|
| ATLA/DALL | 24 | 24 | 4 | 78 |
| ATLA/CLEM via WASH | 24 | 24 | 4 | 79 |
| Full 27 facilities | 324 | 324 | 54 | 1,060 |

The existing CPU, network, disk, and filesystem definitions are preserved. WRENCH storage-service tests
also verify reads, writes, capacity rejection, shared occupancy, and deletion
using the same FSMod filesystems. RAM metadata is now loaded and checked;
remaining cluster attributes, role-based service and agent deployment,
workload configuration, and experiment execution remain
later work. The check-only runner does not attach storage services or agents;
the runtime checks attach test storage services and client controllers.

## Tests

```bash
ctest --test-dir build/extension-runner-compat --output-on-failure
```

The patched build runs 62 checks: the loader's 37 existing checks, six runner
checks, 13 compatibility-validation cases, and six WRENCH storage-I/O checks.

- Three loading checks load each generated platform through real WRENCH and
  compare its inventory with the independent SimGrid resource audit. Both default and
  explicit loader paths are exercised from a different working directory.
- Invalid input checks exercise missing files, malformed JSON, unsafe report
  paths, unsupported schema versions, and unsupported execution modes.
- An independent one-host test platform exercises successful WRENCH loading
  and clean exit. It has no storage and establishes only the runner lifecycle.
- The memory pipeline check generates independent profile/facility overrides,
  verifies WRENCH byte capacities and SWARM compute totals, rejects invalid
  values and inconsistent metadata, and checks legacy documents without RAM.

The storage checks use facility-shared and cluster-scratch stores on direct,
multi-hop, and long-distance paths. They verify object identity, capacity,
occupancy, remote clients, and I/O completions on every member disk. Synthetic
platforms verify that ordinary mount checks and malformed-binding rejection
remain active. See the compatibility README for their scope.

Building against unpatched WRENCH instead selects known-incompatibility checks
for the generated platforms and omits the new storage tests. A passing test in
that build confirms truthful rejection reporting; it does not imply successful
loading.

Reports live under `build/extension-runner-compat/checks/`, which is ignored by Git.
