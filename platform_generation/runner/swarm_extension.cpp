#include <wrench.h>
#include <simgrid/s4u.hpp>
#include <fsmod/FileSystem.hpp>
#include <nlohmann/json.hpp>
#include "PlatformLoading.hpp"
#include "MemoryMetadata.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace path = std::filesystem;
using json = nlohmann::json;

struct Options {
  path::path platform;
  path::path loader = SWARM_DEFAULT_LOADER;
  path::path report;
  bool check_platform = false;
  std::vector<std::string> simulation_arguments;
};

void usage(const char* executable) {
  std::cout << "Usage: " << executable
            << " --check-platform --platform PLATFORM.json [--loader LOADER.so]"
               " [--report REPORT.json] [--cfg=...] [--log=...]\n";
}

Options parse_options(int argc, char** argv) {
  Options options;
  options.simulation_arguments.emplace_back(argv[0]);
  for (int i = 1; i < argc; ++i) {
    const std::string argument = argv[i];
    if (argument == "--check-platform") {
      options.check_platform = true;
    } else if (argument == "--platform" || argument == "--loader" || argument == "--report") {
      if (++i >= argc) throw std::invalid_argument("Missing value for " + argument);
      if (argument == "--platform") options.platform = argv[i];
      else if (argument == "--loader") options.loader = argv[i];
      else options.report = argv[i];
    } else if (argument.rfind("--cfg=", 0) == 0 || argument.rfind("--log=", 0) == 0) {
      options.simulation_arguments.push_back(argument);
    } else {
      throw std::invalid_argument("Unknown argument: " + argument);
    }
  }
  if (!options.check_platform)
    throw std::invalid_argument("This stage requires --check-platform; workload execution is not implemented yet");
  if (options.platform.empty()) throw std::invalid_argument("--platform is required");
  options.platform = path::canonical(options.platform);
  options.loader = path::canonical(options.loader);
  if (!options.report.empty()) {
    options.report = path::weakly_canonical(options.report);
    if (options.report == options.platform || options.report == options.loader ||
        (path::exists(options.report) &&
         (path::equivalent(options.report, options.platform) || path::equivalent(options.report, options.loader))))
      throw std::invalid_argument("Report must not overwrite the platform or loader");
  }
  return options;
}

void write_report(const Options& options, const json& report) {
  if (!options.report.empty()) {
    std::ofstream output(options.report);
    if (!output) throw std::runtime_error("Cannot write report: " + options.report.string());
    output << report.dump(2) << '\n';
    if (!output) throw std::runtime_error("Failed writing report: " + options.report.string());
  }
  // Keep the console readable for the full platform. --report retains the
  // complete inventory, including individual disks and link settings.
  auto summary = report;
  summary.erase("inventory");
  if (!options.report.empty()) summary["report"] = options.report.string();
  std::cout << summary.dump(2) << std::endl;
}

void collect_inventory(json& report, const json& config) {
  auto* engine = simgrid::s4u::Engine::get_instance();
  if (!engine->get_netzone_root())
    throw std::runtime_error("The loader did not create a platform root zone");
  json inventory = {{"hosts", json::array()}, {"disks", json::array()},
                    {"links", json::array()}, {"filesystems", json::array()}};
  std::set<std::string> simgrid_hosts;
  for (const auto* host : engine->get_all_hosts()) {
    simgrid_hosts.insert(host->get_name());
    inventory["hosts"].push_back({{"name", host->get_name()}, {"cores", host->get_core_count()},
                                  {"speed", host->get_speed()}, {"properties", *host->get_properties()},
                                  {"memory_bytes", wrench::Simulation::getHostMemoryCapacity(host->get_name())}});
    for (const auto* disk : host->get_disks()) {
      const auto* mount = disk->get_property("mount");
      inventory["disks"].push_back({{"name", disk->get_name()}, {"host", host->get_name()},
          {"read_bytes_per_second", disk->get_read_bandwidth()},
          {"write_bytes_per_second", disk->get_write_bandwidth()},
          {"mount_property", mount ? json(mount) : json(nullptr)}});
    }
  }
  const auto wrench_hosts = wrench::Simulation::getHostnameList();
  if (simgrid_hosts != std::set<std::string>(wrench_hosts.begin(), wrench_hosts.end()))
    throw std::runtime_error("WRENCH and SimGrid expose different host lists");
  for (const auto* link : engine->get_all_links()) {
    inventory["links"].push_back({{"name", link->get_name()},
        {"bandwidth_bytes_per_second", link->get_bandwidth()}, {"latency_seconds", link->get_latency()},
        {"sharing_policy", link->get_sharing_policy() == simgrid::s4u::Link::SharingPolicy::SHARED
                               ? "SHARED" : "FATPIPE"}});
  }
  std::map<std::string, std::shared_ptr<simgrid::fsmod::FileSystem>> filesystems;
  for (const auto* zone : engine->get_all_netzones())
    for (const auto& [name, filesystem] : simgrid::fsmod::FileSystem::get_file_systems_by_netzone(zone))
      if (!filesystems.emplace(name, filesystem).second)
        throw std::runtime_error("Duplicate filesystem ID: " + name);
  for (const auto& entry : config.at("filesystems")) {
    const auto name = entry.at("name").get<std::string>();
    const auto mount = entry.at("mount_point").get<std::string>();
    const auto partition = filesystems.at(name)->get_partition_for_path_or_null(mount);
    if (!partition) throw std::runtime_error("Missing filesystem partition: " + name);
    inventory["filesystems"].push_back({{"name", name}, {"mount_point", mount},
        {"capacity_bytes", partition->get_size()}, {"free_bytes", partition->get_free_space()}});
  }
  report["counts"] = {{"facilities", config.at("facilities").size()},
      {"hosts", inventory["hosts"].size()}, {"disks", inventory["disks"].size()},
      {"links_including_simgrid_loopback", inventory["links"].size()},
      {"filesystems", filesystems.size()}};
  report["wrench_and_simgrid_hosts_agree"] = true;
  report["inventory"] = std::move(inventory);
}

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "--help") {
    usage(argv[0]);
    return 0;
  }
  Options options;
  json report = {{"mode", "check-platform"}, {"status", "failed"},
                 {"wrench_header_version", WRENCH_VERSION_STRING},
                 {"wrench_platform_accepted", false},
                 {"agents_attached", false}, {"simulation_launched", false}};
  json config;
#ifdef WRENCH_SWARM_FSMOD_COMPAT_VERSION
  report["wrench_fsmod_compat_version"] = WRENCH_SWARM_FSMOD_COMPAT_VERSION;
#else
  report["wrench_fsmod_compat_version"] = 0;
#endif
  std::shared_ptr<wrench::Simulation> simulation;
  try {
    options = parse_options(argc, argv);
    report["platform"] = options.platform.string();
    report["loader"] = options.loader.string();
    report["stage"] = "read-platform";
    std::ifstream input(options.platform);
    if (!input) throw std::runtime_error("Cannot read platform: " + options.platform.string());
    input >> config;

    // The loader's documented input mechanism. Explicit CLI selection takes
    // precedence over an inherited PLATFORM_CONFIG from another experiment.
    if (setenv("PLATFORM_CONFIG", options.platform.c_str(), 1) != 0)
      throw std::runtime_error("Cannot set PLATFORM_CONFIG");
    std::vector<char*> simulation_argv;
    for (auto& argument : options.simulation_arguments) simulation_argv.push_back(argument.data());
    simulation_argv.push_back(nullptr);
    int simulation_argc = static_cast<int>(simulation_argv.size()) - 1;
    report["stage"] = "initialize-wrench";
    simulation = wrench::Simulation::createSimulation();
    simulation->init(&simulation_argc, simulation_argv.data());

    // Use the real SWARM/WRENCH entry point, including its platform sanity
    // checks. Loading the same library directly through Engine is insufficient
    // to establish compatibility with WRENCH.
    report["stage"] = "instantiate-platform";
    swarm_extension::load_platform(simulation, options.loader.string());
    report["wrench_platform_accepted"] = true;
    report["stage"] = "inventory";
    collect_inventory(report, config);
    report["stage"] = "memory-metadata";
    const auto memory = swarm_extension::memory_metadata(config);
    if (!memory.is_null()) report["memory_metadata"] = memory;
    report["status"] = "passed";
    report["stage"] = "platform-loaded";
    write_report(options, report);
    return 0;
  } catch (const std::exception& error) {
    report["status"] = "failed";
    report["error"] = error.what();
    std::cerr << "SWARM extension: " << error.what() << '\n';
    if (report.value("stage", "") == "instantiate-platform") {
      // A WRENCH sanity-check failure can occur after the loader has created
      // all SimGrid/FSMod resources. Preserve that inventory as diagnostics;
      // it does not imply that WRENCH accepted the platform.
      try {
        collect_inventory(report, config);
      } catch (const std::exception& inventory_error) {
        report["inventory_error"] = inventory_error.what();
      }
    }
    try {
      write_report(options, report);
    } catch (const std::exception& report_error) {
      std::cerr << "SWARM extension: " << report_error.what() << '\n';
    }
    return 1;
  }
}
