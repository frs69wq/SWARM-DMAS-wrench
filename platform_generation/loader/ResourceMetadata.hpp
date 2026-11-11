#pragma once

#include <nlohmann/json.hpp>
#include <simgrid/s4u.hpp>
#include <limits>
#include <optional>
#include <stdexcept>

namespace swarm_platform {
inline std::optional<int> memory_gb(const nlohmann::json& config, const std::string& key,
                                    const std::string& context) {
  if (!config.contains(key)) return std::nullopt;
  const auto& value = config.at(key);
  if (!value.is_number_integer() || value <= 0 || value > std::numeric_limits<int>::max())
    throw std::invalid_argument(context + "." + key + " must be a positive integer GB value within INT_MAX");
  return value.get<int>();
}

inline void set_memory(simgrid::s4u::Host* host, const std::optional<int>& memory) {
  if (memory) host->set_property("mem", std::to_string(*memory) + "GB");
}

inline void apply_cluster_properties(simgrid::s4u::NetZone* zone, const nlohmann::json& config,
                                     const std::optional<int>& memory) {
  if (!config.contains("properties")) return;
  const auto& properties = config.at("properties");
  if (!properties.is_object()) throw std::invalid_argument("Cluster properties must be an object of strings");
  for (const auto& [key, value] : properties.items()) {
    if (!value.is_string()) throw std::invalid_argument("Cluster property " + key + " must be a string");
    if (key == "memory_amount_in_gb" && (!memory || value.get<std::string>() != std::to_string(*memory)))
      throw std::invalid_argument("memory_amount_in_gb must match node.memory_gb (GB per compute node)");
    zone->set_property(key, value.get<std::string>());
  }
}
}
