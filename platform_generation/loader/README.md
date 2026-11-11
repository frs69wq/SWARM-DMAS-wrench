# Repository-local JSON platform loader

This loader adds explicit WAN gateway attachments to the existing JSON loader.
The external loader repository is read-only input; no build writes there.

## Source provenance

`json_platform_loader.cpp` was copied from
https://github.com/frs69wq/SimGrid-JSON-platform-loader at commit
`a44eb704c0d843afb0eed1de8292b3e2835d833c`.
The original file's SHA-256 is
`85876c140399923b872000477adae361abbddf121163fbed46b262608c41e99e`.
Its copyright notices are retained and its LGPL 2.1 license is in
`COPYING.LESSER`. Local changes add `FacilityZone.hpp`, consume gateway
descriptions, and reject unsupported extension versions and missing references.
The local loader also exports `swarm_storage_bindings_v1()` from
`StorageBindings.hpp`. It records the existing filesystem, storage, and mount
objects at partition creation for the [WRENCH compatibility integration](../compat/wrench/README.md).
This interface adds no simulated resources and keeps the loader independent of
WRENCH; standalone Inspector and SimGrid checks use the same loader as before.

## RAM and cluster properties

`clusters[].node.memory_gb` and `storage_systems[].server_memory_gb` optionally
set host RAM. Values are positive integer decimal GB within `INT_MAX`. The
loader sets the WRENCH-recognized host property `mem`, for example `256GB`.
Documents without RAM keep their previous behavior; the population generator
requires explicit RAM in all current profiles.

`clusters[].properties` is an optional object of string values applied to the
actual cluster zone. When `memory_amount_in_gb` is present, it must equal
`node.memory_gb` as a canonical integer string. Generated compute zones use this
property for memory per compute node. Role/site/cluster identity properties
are kept separate from host RAM. See the parent README for units and scope.

## Gateway schema

The generated document sets `swarm_platform_schema_version` to `1`. A facility
with a gateway contains, for example:

```json
"wan_gateway": {
  "name": "ATLA_router",
  "attachment_zone": "facility_ATLA_control",
  "access_link": "local_ATLA_wan_access",
  "routes": [
    {"zone": "facility_ATLA_control", "links": ["local_ATLA_wan_access"]}
  ]
}
```

The example shows one route; the actual array must include exactly one route
from each immediate child zone to the facility router. Each route lists the
existing local path to the attachment zone followed by the shared access link.
That link is defined once in the facility's `links`, with configurable bandwidth
and latency. The attachment zone is a network junction; traffic does not execute
on the control host or involve its facility agent.

All routes are symmetric. Local child-to-child routes keep their original
paths. Top-level physical WAN links and routes are unchanged. No pair-specific
virtual WAN resources, extra hosts, or extra disks are created.

The upstream Full routing implementation rejects mixed child-zone/router
endpoints. `FacilityZone` supplies an explicit route table for those endpoints,
using the installed SimGrid routing extension API. It also handles the upward
link-order convention of SimGrid 4.1. This is tested with SimGrid 4.1.1; other
versions need validation. Host-to-host route tests check link order, link object
identity, and latency in both directions. No SimGrid library is modified.

Legacy documents without `wan_gateway` still use the upstream Full routing
behavior. The upstream loader ignores the new fields and cannot provide WAN
connectivity for this schema; use this repository's library for gateway files.

## Build and check

From the repository root, using your installed dependency prefixes:

```bash
cmake -S platform_generation/loader -B build/facility-loader \
  -DSIMGRID_PATH=/path/to/simgrid-install \
  -DFSMOD_PATH=/path/to/fsmod-install \
  -DCMAKE_PREFIX_PATH=/path/to/dependency-prefix
cmake --build build/facility-loader -j 2
ctest --test-dir build/facility-loader --output-on-failure
```

Use `-DCMAKE_CXX_COMPILER` to select the compiler matching your dependencies if
needed. Some Conda installations also require
`-DCMAKE_EXE_LINKER_FLAGS=-Wl,-rpath-link,/path/to/dependency-prefix/lib`.
These checks expect the generated ATLA/DALL and ATLA/CLEM fixtures; see the
generation commands in the parent README. `SWARM_TEST_PLATFORM` and
`SWARM_MULTIHOP_TEST_PLATFORM` can override their respective paths.
The full 27-facility fixture is selected by `SWARM_FULL_TEST_PLATFORM`.

Each fixture receives the resource audit, all directed host-route checks,
round-trip transfers, remote filesystem I/O, separate-client file sharing,
and malformed-schema checks. The multi-hop fixture additionally checks two
independent flows sharing the unchanged 40 Gbps CLEM/WASH physical link.
Both flows reference that same link object, and slower completion plus bounded
aggregate bandwidth confirms actual contention. Payload byte counts are
simulated, so the contention test does not allocate multi-gigabyte buffers.
Both contention tests use `--cfg=network/TCP-gamma:1073741824` (a simulated 1 GiB
maximum TCP window). SimGrid's default 4 MiB window limits individual flows
below the physical bottleneck on this high-latency path. The larger window
isolates link sharing; it is a test transport assumption, not a change to any
generated host, storage, bandwidth or latency definition. Communication and
storage checks outside the contention tests use default transport settings.

The full-platform suite repeats the resource and route audits on all 27
facilities, adds a long-distance round trip, and remotely exercises every
facility controller and all facility-shared/scratch filesystems. To run only
those checks:

```bash
ctest --test-dir build/facility-loader -R '^full_' --output-on-failure
```

`full_cross_facility_pair_contention` uses the unchanged full platform to run
ATLA-to-CLEM and DALL-to-CLEM transfers through the shared 40 Gbps WASH/CLEM
physical resource. Each transfer has its own isolated baseline. Two actors then
start simultaneously at a barrier, record their individual completion times,
and verify that both slow down. Aggregate throughput, including latency, must
remain below the physical link capacity. The probe checks actual link object
identity and distinct facility pairs before measuring. To run this case alone:

```bash
ctest --test-dir build/facility-loader -R '^full_cross_facility_pair_contention$' --output-on-failure
```

`tests/check_full_platform.py` uses the generated role mappings to select
clients in other facilities and saves per-case output plus a structured
`remote-coverage.json` under the build tree. That coverage check uses default
transport settings. No generated resource definitions are changed by any test.

```bash
PLATFORM_CONFIG="$PWD/platform_generation/generated/two_facilities/fabric_platform.json" \
/path/to/simgrid_platform_inspector \
  --platform "$PWD/build/facility-loader/libswarm_platform.so" \
  --show_hosts --show_disks --show_links --show_routes all
```

On Linux the loader is linked with `NODELETE`, keeping its routing implementation
and FSMod mapped through engine cleanup. Unlike the external loader, this build
does not require FSMod preloading in the Inspector. The integration probe links
FSMod directly. Client filesystem tests resolve handles from storage zones by
stable ID: actor-local filesystem discovery remains unchanged. I/O checks model
byte counts and occupancy, not real file contents or SWARM agent deployment.
