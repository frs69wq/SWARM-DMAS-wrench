#pragma once

#include <fsmod/FileSystem.hpp>
#include <fsmod/Storage.hpp>
#include <string>
#include <vector>

namespace swarm_platform {
// Records are created at the actual mount_partition call. They describe the
// existing objects, without introducing another capacity or occupancy model.
struct StorageBinding {
  std::shared_ptr<simgrid::fsmod::FileSystem> filesystem;
  std::shared_ptr<simgrid::fsmod::Storage> storage;
  std::string mount_point;
};
using StorageBindings = std::vector<StorageBinding>;
}

// Versioned entry point for the repository-local WRENCH integration. Both
// modules must use matching C++ dependencies, as for the loader itself.
extern "C" const swarm_platform::StorageBindings* swarm_storage_bindings_v1();
