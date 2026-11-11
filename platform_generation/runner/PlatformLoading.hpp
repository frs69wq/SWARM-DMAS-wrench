#pragma once

#include <wrench.h>
#include <simgrid/s4u.hpp>
#include <dlfcn.h>
#include <memory>
#include <stdexcept>
#include "../loader/StorageBindings.hpp"

namespace swarm_extension {
inline swarm_platform::StorageBindings storage_bindings(const std::string& library) {
  std::unique_ptr<void, decltype(&dlclose)> handle(dlopen(library.c_str(), RTLD_NOW | RTLD_NOLOAD), dlclose);
  if (!handle) throw std::runtime_error("Cannot access loaded platform module: " + std::string(dlerror()));
  auto bindings = reinterpret_cast<decltype(&swarm_storage_bindings_v1)>(
      dlsym(handle.get(), "swarm_storage_bindings_v1"));
  if (!bindings) throw std::runtime_error("Loader lacks swarm_storage_bindings_v1");
  return *bindings();
}

inline void load_platform(const std::shared_ptr<wrench::Simulation>& simulation, const std::string& library) {
#ifdef WRENCH_SWARM_FSMOD_COMPAT_VERSION
  simulation->instantiatePlatform([&] {
    simgrid::s4u::Engine::get_instance()->load_platform(library);
    for (const auto& binding : storage_bindings(library))
      simulation->registerExistingFileSystem(binding.filesystem, binding.storage, binding.mount_point);
  });
#else
  simulation->instantiatePlatform(library);
#endif
}
}
