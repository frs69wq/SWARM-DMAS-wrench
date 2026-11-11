#pragma once

#include <wrench.h>
#include <wrench/simgrid_S4U_util/S4U_Simulation.h>
#include "../loader/ResourceMetadata.hpp"
#include <map>
#include <set>

namespace swarm_extension {
inline nlohmann::json memory_metadata(const nlohmann::json& config) {
  using json = nlohmann::json;
  if (!config.contains("swarm") || !config.at("swarm").contains("memory")) return nullptr;
  const auto& contract = config.at("swarm").at("memory");
  if (contract.at("unit") != "GB" || contract.at("bytes_per_gb") != 1000000000 ||
      contract.at("cluster_capacity_scope") != "per_compute_node" ||
      contract.at("job_request_scope") != "total_across_requested_compute_nodes")
    throw std::invalid_argument("Unsupported SWARM memory units or capacity/request scope");
  constexpr sg_size_t bytes_per_gb = 1000000000;
  std::map<std::string, int> expected;
  std::map<std::string, json> compute_specs;
  auto add = [&](const std::string& name, const std::optional<int>& gb) {
    if (!gb || !expected.emplace(name, *gb).second)
      throw std::invalid_argument("Missing or duplicate host memory metadata: " + name);
  };
  auto add_storage = [&](const auto& storage) {
    const std::string name = storage.at("name");
    add(name + "_server", swarm_platform::memory_gb(storage, "server_memory_gb", name));
  };
  for (const auto& facility : config.at("facilities")) {
    for (const auto& cluster : facility.at("clusters")) {
      const std::string name = cluster.at("name");
      compute_specs.emplace(name, cluster);
      const auto gb = swarm_platform::memory_gb(cluster.at("node"), "memory_gb", name);
      for (int i = 0; i < cluster.at("count").get<int>(); ++i)
        add(cluster.at("prefix").get<std::string>() + std::to_string(i) + cluster.at("suffix").get<std::string>(), gb);
    }
    for (const auto& storage : facility.at("storage_systems")) add_storage(storage);
  }
  for (const auto& storage : config.value("storage_systems", json::array())) add_storage(storage);
  const auto actual_hosts = wrench::Simulation::getHostnameList();
  if (actual_hosts.size() != expected.size()) throw std::runtime_error("Memory metadata host count mismatch");
  uint64_t all_host_gb = 0;
  for (const auto& name : actual_hosts) {
    const auto gb = expected.at(name);
    if (wrench::Simulation::getHostMemoryCapacity(name) != static_cast<sg_size_t>(gb) * bytes_per_gb)
      throw std::runtime_error("WRENCH RAM differs from platform JSON: " + name);
    all_host_gb += gb;
  }
  json report = {{"status", "passed"}, {"unit", "GB"}, {"bytes_per_gb", bytes_per_gb},
                 {"hosts_checked", actual_hosts.size()}, {"all_host_memory_gb", all_host_gb},
                 {"clusters", json::array()}, {"job_request_scope", contract.at("job_request_scope")}};
  const auto by_cluster = wrench::Simulation::getHostnameListByCluster();
  uint64_t total_compute_gb = 0, total_compute_hosts = 0;
  for (const auto& mapping : config.at("swarm").at("scheduling_agents")) {
    const std::string zone = mapping.at("compute_cluster"), manager = mapping.at("management_host");
    const auto& spec = compute_specs.at(zone);
    const auto& hosts = by_cluster.at(zone);
    const auto gb = spec.at("node").at("memory_gb").get<int>();
    const auto property = wrench::S4U_Simulation::getClusterProperty(zone, "memory_amount_in_gb");
    if (property != std::to_string(gb) || wrench::S4U_Simulation::getClusterProperty(zone, "role") != "compute" ||
        wrench::S4U_Simulation::getClusterProperty(zone, "site") != mapping.at("facility").get<std::string>() ||
        wrench::S4U_Simulation::getClusterProperty(zone, "cluster_id") != mapping.at("cluster").get<std::string>())
      throw std::runtime_error("SWARM compute-cluster memory/identity properties disagree: " + zone);
    std::set<std::string> expected_compute;
    for (int i = 0; i < spec.at("count").get<int>(); ++i)
      expected_compute.insert(spec.at("prefix").get<std::string>() + std::to_string(i) + spec.at("suffix").get<std::string>());
    if (std::set<std::string>(hosts.begin(), hosts.end()) != expected_compute ||
        hosts.size() != mapping.at("compute_node_count").get<size_t>() || expected_compute.count(manager))
      throw std::runtime_error("SWARM compute-node mapping includes wrong hosts: " + zone);
    for (const auto& host : hosts)
      if (expected.at(host) != gb) throw std::runtime_error("Nonuniform compute-node memory: " + zone);
    const auto total_gb = static_cast<uint64_t>(gb) * hosts.size();
    total_compute_gb += total_gb;
    total_compute_hosts += hosts.size();
    report["clusters"].push_back({{"cluster", mapping.at("cluster")}, {"compute_cluster", zone},
        {"compute_nodes", hosts.size()}, {"memory_per_compute_node_gb", gb}, {"total_compute_memory_gb", total_gb},
        {"management_host", manager}, {"management_memory_gb", expected.at(manager)}});
  }
  report["compute_hosts"] = total_compute_hosts;
  report["total_compute_memory_gb"] = total_compute_gb;
  return report;
}
}
