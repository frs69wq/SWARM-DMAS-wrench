# WRENCH compatibility for existing FSMod stores

`fsmod-storage.patch` adapts WRENCH's platform validation and storage-service
factory to the FSMod stores created by our JSON loader. The platform JSON keeps
its disk counts, filesystem capacities, bandwidths, and routes.

`upstream.json` pins WRENCH commit
`831b959bb9ed9aa174d862958ebbbc15c2d540e8` and the patch checksum. This revision
works with the FSMod API installed in this workspace. More recent local WRENCH
revisions call `Partition::erase_all_content()`, which that installation lacks.
The source copy retains the upstream notices. `LICENSE.md` is copied verbatim
from the pinned source.

## What the patch changes

1. Adds `Simulation::registerExistingFileSystem(filesystem, storage, mount)`.
   Call it during platform creation with the exact objects passed to FSMod's
   `mount_partition()`. WRENCH checks the partition and capacity, registration
   at the storage host's zone, disk identity and attachment, disk bandwidth,
   and duplicate ownership. This version supports one partition and one
   storage host per store, matching the generated platform.
2. Uses those validated bindings when checking disk mounts. Capacity and
   occupancy belong to the filesystem partition. Unregistered disks retain
   the ordinary WRENCH mount and capacity checks, including rejection of
   duplicate mounts. Registering one store does not exempt other disks on
   its host.
3. Adds public `SimpleStorageService::createFsmodStorageService()`, which wraps
   WRENCH's private existing-filesystem factory. It reuses the provided FSMod filesystem rather than creating another
   filesystem. This API defaults to a 1 MB (1,000,000 byte) buffer and rejects
   zero-buffer operation. WRENCH's buffered path calls FSMod file read/write
   methods; the unbuffered path assumes a single raw disk. The buffer is a
   service setting, separate from physical disk and network performance. Existing internal
   factory callers retain their original behavior.

`WRENCH_SWARM_FSMOD_COMPAT_VERSION=1` identifies the patched headers. The runner
reports the compatibility version and CMake selects successful-loading tests
when it finds the marker. Ordinary WRENCH installations still build the runner
and exercise the known duplicate-mount rejection.

## Build the patched dependency

Use Python 3.12+, Git, `patch`, CMake, a C++ compiler, and the existing SimGrid,
FSMod, Boost, and nlohmann-json installations. From the repository root:

```bash
python3 platform_generation/compat/wrench/build_wrench.py \
  --source-repo /path/to/wrench-checkout \
  --simgrid /path/to/simgrid-install \
  --fsmod /path/to/fsmod-install \
  --dependency-prefix /path/to/dependency-prefix \
  --cxx /path/to/c++ \
  --jobs 4
```

In this workspace the dependency prefix is the `swarm-resilience` Conda
environment. Also pass
`--cmake-arg=-DCMAKE_EXE_LINKER_FLAGS=-Wl,-rpath-link,/path/to/swarm/env/lib`
when needed to resolve transitive dependencies.

The helper reads the pinned commit with `git archive`, applies the checked
patch, builds the WRENCH and workflow-parser libraries, and stages their headers,
libraries, patch, and license under `build/wrench-compat-v1/install/`. It performs
no network downloads. The external source checkout and installed WRENCH remain
unchanged. All output is constrained to this repository's ignored `build/`
directory.

Repeated invocations reuse a matching prepared source copy and CMake build.
An existing source with a different provenance stamp is rejected; select a new
`--output build/another-name` for a different patch/base. `--prepare-only`
checks extraction and patch application without compiling.

## Use it with the extension runner

```bash
cmake -S platform_generation/runner -B build/extension-runner-compat \
  -DCMAKE_CXX_COMPILER=/path/to/c++ \
  -DSIMGRID_PATH=/path/to/simgrid-install \
  -DFSMOD_PATH=/path/to/fsmod-install \
  -DWRENCH_PATH="$PWD/build/wrench-compat-v1/install" \
  -DCMAKE_PREFIX_PATH=/path/to/dependency-prefix
cmake --build build/extension-runner-compat -j 4
ctest --test-dir build/extension-runner-compat --output-on-failure
```

For the same local Conda setup, add
`-DCMAKE_EXE_LINKER_FLAGS=-Wl,-rpath-link,/path/to/swarm/env/lib`.
The runner uses WRENCH's public platform-creation callback overload. That callback
loads our module, reads `swarm_storage_bindings_v1()`, and registers the exact
filesystem/storage objects before WRENCH performs its sanity checks. The loader
records those bindings when it mounts the actual partition and has no WRENCH
dependency. FSMod's binaries are unchanged.

## Validation

The patched build has successful loading and resource-inventory comparisons for
ATLA/DALL, ATLA/CLEM via WASH, and the complete 27-facility platform. Existing
SimGrid/FSMod resource, route, filesystem, and link-contention checks also run.
The current suite also verifies explicit host RAM and SWARM memory metadata;
these use the local loader and require no additional WRENCH patch changes.

Six WRENCH storage checks cover shared facility storage and cluster scratch
storage across direct, multi-hop, and long-distance routes. Each uses two
distinct remote clients and checks:

- Storage-service and loader filesystem object identity.
- Matching capacity/free space, rejected over-capacity writes, and unchanged
  occupancy after rejection.
- An 8 MiB write, discovery and read by a second client, deletion, and restored
  free space through both WRENCH and FSMod.
- Completed read and write I/O on every backing disk, with nine chunks per
  direction for the default 1 MB buffer.

Thirteen synthetic validation cases check ordinary disks and malformed FSMod
registrations, including duplicate mounts, overlapping mounts, invalid sizes,
missing registration, duplicate disk ownership, and extra ordinary disks on a
host that also has FSMod storage. Test actors are not SWARM facility or
scheduling agents; attaching those agents and running jobs remain later work.

Validation reports and dependency builds belong under `build/` and stay out of
Git. Commit the patch, build helper, loader binding interface, runner integration,
tests, and documentation.
