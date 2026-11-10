// SWARM facility routing: explicit child-zone routes plus WAN-router access.
// Uses the installed SimGrid 4.1 routing extension API; no SimGrid modifications.
#pragma once

#include <simgrid/kernel/routing/NetPoint.hpp>
#include <simgrid/kernel/routing/NetZoneImpl.hpp>
#include <algorithm>
#include <map>
#include <stdexcept>

namespace swarm_platform {
namespace routing = simgrid::kernel::routing;
namespace sg = simgrid::s4u;

class FacilityZone final : public routing::NetZoneImpl {
  struct Entry {
    routing::NetPoint* source_gateway;
    routing::NetPoint* destination_gateway;
    std::vector<const sg::Link*> links;
  };
  using Key = std::pair<const routing::NetPoint*, const routing::NetPoint*>;
  std::map<Key, Entry> routes_;

  void validate_endpoint(routing::NetPoint* endpoint, routing::NetPoint* gateway) const
  {
    if (!endpoint || endpoint->get_englobing_zone() != this)
      throw std::invalid_argument("Facility route endpoint must belong to " + get_name());
    if (endpoint->is_netzone()) {
      const auto* child = get_netzone_recursive(endpoint);
      if (!gateway || !child || !child->is_component_recursive(gateway))
        throw std::invalid_argument("Invalid child gateway for " + endpoint->get_name());
    } else if (gateway && gateway != endpoint) {
      throw std::invalid_argument("Router endpoint must be its own gateway");
    }
  }

public:
  explicit FacilityZone(const std::string& name) : NetZoneImpl(name) {}

  void add_route(routing::NetPoint* source, routing::NetPoint* destination,
                 routing::NetPoint* source_gateway, routing::NetPoint* destination_gateway,
                 const std::vector<sg::LinkInRoute>& links, bool symmetric) override
  {
    validate_endpoint(source, source_gateway);
    validate_endpoint(destination, destination_gateway);
    if (source == destination || links.empty())
      throw std::invalid_argument("Facility route must join distinct endpoints over explicit links");
    if (routes_.count({source, destination}) || (symmetric && routes_.count({destination, source})))
      throw std::invalid_argument("Duplicate facility route in " + get_name());
    Entry entry{source_gateway ? source_gateway : source,
                destination_gateway ? destination_gateway : destination, {}};
    for (const auto& hop : links) {
      if (!hop.get_link() || hop.get_direction() != sg::LinkInRoute::Direction::NONE ||
          hop.get_link()->get_englobing_zone() != get_iface())
        throw std::invalid_argument("Facility routes require local, undirected links");
      entry.links.push_back(hop.get_link());
    }
    routes_.emplace(Key{source, destination}, entry);
    if (symmetric) {
      std::swap(entry.source_gateway, entry.destination_gateway);
      std::reverse(entry.links.begin(), entry.links.end());
      routes_.emplace(Key{destination, source}, entry);
    }
    on_route_creation(symmetric, source, destination, source_gateway, destination_gateway,
                      get_link_list_impl(links, false));
  }

  void get_local_route(const routing::NetPoint* source, const routing::NetPoint* destination,
                       routing::Route* result, double* latency) override
  {
    if (source == destination && !source->is_netzone())
      return;
    const auto found = routes_.find({source, destination});
    if (found == routes_.end())
      throw std::runtime_error("Missing facility route: " + source->get_name() + " -> " + destination->get_name());
    result->gw_src_ = found->second.source_gateway;
    result->gw_dst_ = found->second.destination_gateway;
    auto append = [&](const sg::Link* link) {
      result->link_list_.push_back(link->get_impl());
      if (latency)
        *latency += link->get_latency();
    };
    // SimGrid 4.1 get_interzone_route() reverses the upward child-to-router
    // segment before prepending it. Compensate here so host-to-host routes
    // follow the canonical zone-to-gateway link order declared in the JSON.
    if (source->is_netzone() && destination->is_router()) {
      for (auto link = found->second.links.rbegin(); link != found->second.links.rend(); ++link)
        append(*link);
    } else {
      for (const auto* link : found->second.links)
        append(link);
    }
  }
};
} // namespace swarm_platform
