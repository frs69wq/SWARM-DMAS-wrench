#!/usr/bin/env python3
"""Populate a FABRIC WAN platform with fixed facility and cluster resources."""

from __future__ import annotations

import argparse
import copy
import heapq
import json
import math
import re
import sys
from itertools import combinations
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Add facility control hosts, shared storage, cluster management and "
            "compute hosts, per-cluster shared scratch, and internal networks."
        )
    )
    parser.add_argument(
        "--wan",
        type=Path,
        default=Path("platform_generation/generated/fabric_wan.json"),
        help="Input WAN-only platform JSON.",
    )
    parser.add_argument(
        "--spec",
        type=Path,
        default=Path("platform_generation/fabric_resource_spec.json"),
        help="Fixed resource specification JSON.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("platform_generation/generated/fabric_platform.json"),
        help="Output populated platform JSON.",
    )
    return parser.parse_args()


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def require_object(value: Any, description: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{description} must be a JSON object.")
    return value


def require_nonempty_string(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{description} must be a non-empty string.")
    return value.strip()


def require_memory_gb(value: Any, description: str) -> int:
    # GB is decimal. Keep values representable by SWARM's existing int field.
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 2**31 - 1:
        raise ValueError(f"{description} must be a positive integer GB value no greater than {2**31 - 1}.")
    return value


def merge_objects(parent: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge object fields; arrays and scalar values replace."""

    merged = copy.deepcopy(parent)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_objects(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def safe_component(value: str) -> str:
    component = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    component = re.sub(r"_+", "_", component)
    if not component:
        raise ValueError(f"Cannot create a platform name from {value!r}.")
    return component


def parse_milliseconds(value: Any, description: str) -> float:
    text = require_nonempty_string(value, description)
    if text.endswith("ms"):
        text = text[:-2]
    try:
        milliseconds = float(text)
    except ValueError as exc:
        raise ValueError(f"{description} must be a numeric millisecond value.") from exc
    if not math.isfinite(milliseconds) or milliseconds <= 0:
        raise ValueError(f"{description} must be a finite value greater than zero.")
    return milliseconds


def storage_definition(
    name: str,
    spec: dict[str, Any],
    filesystem_name: str,
    default_mount_point: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    storage = require_object(spec, f"Storage specification '{name}'")
    required = (
        "server_speed",
        "server_memory_gb",
        "type",
        "disk_count",
        "read_bandwidth",
        "write_bandwidth",
        "capacity",
    )
    missing = [key for key in required if key not in storage]
    if missing:
        raise ValueError(f"Storage '{name}' is missing fields: {', '.join(missing)}.")

    storage_type = require_nonempty_string(storage["type"], f"{name}.type")
    if storage_type not in {"JBOD", "OneDisk"}:
        raise ValueError(f"{name}.type must be 'JBOD' or 'OneDisk'.")
    disk_count = storage["disk_count"]
    if isinstance(disk_count, bool) or not isinstance(disk_count, int) or disk_count < 1:
        raise ValueError(f"{name}.disk_count must be a positive integer.")

    platform_storage = {
        "name": name,
        "server_speed": require_nonempty_string(storage["server_speed"], f"{name}.server_speed"),
        "server_memory_gb": require_memory_gb(storage["server_memory_gb"], f"{name}.server_memory_gb"),
        "type": storage_type,
        "disk_count": disk_count,
        "read_bandwidth": require_nonempty_string(storage["read_bandwidth"], f"{name}.read_bandwidth"),
        "write_bandwidth": require_nonempty_string(storage["write_bandwidth"], f"{name}.write_bandwidth"),
    }
    filesystem = {
        "name": filesystem_name,
        "storage_system": name,
        "mount_point": require_nonempty_string(
            storage.get("mount_point", default_mount_point),
            f"{name}.mount_point",
        ),
        "size": require_nonempty_string(storage["capacity"], f"{name}.capacity"),
    }
    return platform_storage, filesystem


def make_loader_cluster(
    *,
    zone_name: str,
    host_prefix: str,
    count: int,
    speed: str,
    cores: int,
    memory_gb: int,
    network: dict[str, Any],
    properties: dict[str, str],
    node_storage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError(f"Cluster '{zone_name}' count must be a positive integer.")
    if isinstance(cores, bool) or not isinstance(cores, int) or cores < 1:
        raise ValueError(f"Cluster '{zone_name}' cores must be a positive integer.")

    backbone = require_object(network.get("backbone"), f"{zone_name}.network.backbone")
    node_network = require_object(network.get("node"), f"{zone_name}.network.node")
    private_link = require_object(
        node_network.get("private_link"),
        f"{zone_name}.network.node.private_link",
    )
    loopback = require_object(
        node_network.get("loopback"),
        f"{zone_name}.network.node.loopback",
    )
    node: dict[str, Any] = {
        "speed": require_nonempty_string(speed, f"{zone_name}.speed"),
        "cores": cores,
        "memory_gb": require_memory_gb(memory_gb, f"{zone_name}.memory_gb"),
        "private_link": {
            "bandwidth": require_nonempty_string(
                private_link.get("bandwidth"), f"{zone_name}.private_link.bandwidth"
            ),
            "latency": require_nonempty_string(
                private_link.get("latency", "0s"), f"{zone_name}.private_link.latency"
            ),
        },
        "loopback": {
            "bandwidth": require_nonempty_string(
                loopback.get("bandwidth"), f"{zone_name}.loopback.bandwidth"
            ),
            "latency": require_nonempty_string(
                loopback.get("latency", "0s"), f"{zone_name}.loopback.latency"
            ),
        },
    }
    if private_link.get("sharing_policy") is not None:
        node["private_link"]["sharing_policy"] = require_nonempty_string(
            private_link["sharing_policy"],
            f"{zone_name}.private_link.sharing_policy",
        )
    if node_storage is not None:
        node["storage"] = copy.deepcopy(node_storage)

    return {
        "name": zone_name,
        "prefix": host_prefix,
        "suffix": "",
        "count": count,
        "properties": properties,
        "node": node,
        "backbone": {
            "bandwidth": require_nonempty_string(
                backbone.get("bandwidth"), f"{zone_name}.backbone.bandwidth"
            ),
            "latency": require_nonempty_string(
                backbone.get("latency", "0s"), f"{zone_name}.backbone.latency"
            ),
        },
    }


def shortest_path(
    graph: dict[str, list[tuple[str, str, float]]],
    source: str,
    destination: str,
) -> list[str]:
    queue: list[tuple[float, tuple[str, ...], str]] = [(0.0, (), source)]
    best: dict[str, tuple[float, tuple[str, ...]]] = {source: (0.0, ())}
    while queue:
        cost, path, node = heapq.heappop(queue)
        if best.get(node) != (cost, path):
            continue
        if node == destination:
            return list(path)
        for neighbor, link_name, edge_cost in graph.get(node, []):
            candidate = (cost + edge_cost, path + (link_name,))
            if neighbor not in best or candidate < best[neighbor]:
                best[neighbor] = candidate
                heapq.heappush(queue, (*candidate, neighbor))
    raise ValueError(f"No facility-local network path from '{source}' to '{destination}'.")


def populate_platform(wan: Any, spec: Any) -> dict[str, Any]:
    platform = require_object(copy.deepcopy(wan), "WAN platform")
    resource_spec = require_object(spec, "Resource specification")
    if resource_spec.get("schema_version") != 1:
        raise ValueError("Resource specification schema_version must be 1.")

    facilities = platform.get("facilities")
    if not isinstance(facilities, list) or not facilities:
        raise ValueError("WAN platform must contain a non-empty facilities array.")
    facility_by_name: dict[str, dict[str, Any]] = {}
    for facility in facilities:
        facility_obj = require_object(facility, "Facility entry")
        name = require_nonempty_string(facility_obj.get("name"), "Facility name")
        if name in facility_by_name:
            raise ValueError(f"WAN platform has duplicate facility '{name}'.")
        facility_by_name[name] = facility_obj

    defaults = require_object(resource_spec.get("defaults"), "defaults")
    control_defaults = require_object(
        defaults.get("facility_control_host"),
        "defaults.facility_control_host",
    )
    storage_defaults = require_object(
        defaults.get("facility_shared_storage"),
        "defaults.facility_shared_storage",
    )
    facility_network_defaults = require_object(
        defaults.get("facility_network"),
        "defaults.facility_network",
    )
    cluster_generation = require_object(
        defaults.get("cluster_generation"),
        "defaults.cluster_generation",
    )
    profiles = require_object(resource_spec.get("cluster_profiles"), "cluster_profiles")
    facility_specs = require_object(resource_spec.get("facilities", {}), "facilities")

    unknown_facilities = sorted(set(facility_specs) - set(facility_by_name))
    if unknown_facilities:
        raise ValueError(
            "Resource specification contains unknown facilities: "
            + ", ".join(unknown_facilities)
        )

    platform.setdefault("storage_systems", [])
    platform.setdefault("links", [])
    platform.setdefault("routes", [])
    platform.setdefault("filesystems", [])
    for field in ("storage_systems", "links", "routes", "filesystems"):
        if not isinstance(platform[field], list):
            raise ValueError(f"WAN platform field '{field}' must be an array.")

    zone_names = set(facility_by_name)
    host_names: set[str] = set()
    link_names = {
        require_nonempty_string(link.get("name"), "WAN link name")
        for link in platform["links"]
        if isinstance(link, dict)
    }
    if len(link_names) != len(platform["links"]):
        raise ValueError("WAN platform contains malformed or duplicate link names.")
    filesystem_names = {
        require_nonempty_string(fs.get("name"), "WAN filesystem name")
        for fs in platform["filesystems"]
        if isinstance(fs, dict)
    }
    if len(filesystem_names) != len(platform["filesystems"]):
        raise ValueError("WAN platform contains malformed or duplicate filesystem names.")
    storage_names: set[str] = set()
    for storage in platform["storage_systems"]:
        storage_obj = require_object(storage, "Top-level storage system")
        storage_name = require_nonempty_string(storage_obj.get("name"), "Storage name")
        if storage_name in storage_names:
            raise ValueError(f"WAN platform has duplicate storage system '{storage_name}'.")
        storage_names.add(storage_name)
        host_names.add(f"{storage_name}_server")

    swarm_facility_agents: list[dict[str, str]] = []
    swarm_scheduling_agents: list[dict[str, Any]] = []

    def reserve_zone(name: str) -> None:
        if name in zone_names:
            raise ValueError(f"Generated zone name '{name}' is not unique.")
        zone_names.add(name)

    def reserve_filesystem(name: str) -> None:
        if name in filesystem_names:
            raise ValueError(f"Generated filesystem name '{name}' is not unique.")
        filesystem_names.add(name)

    def reserve_storage(name: str) -> None:
        if name in storage_names:
            raise ValueError(f"Generated storage-system name '{name}' is not unique.")
        storage_names.add(name)
        reserve_zone(name)
        reserve_host(f"{name}_server")

    def reserve_host(name: str) -> None:
        if name in host_names:
            raise ValueError(f"Generated hostname '{name}' is not unique.")
        host_names.add(name)

    for facility_name in sorted(facility_by_name):
        facility = facility_by_name[facility_name]
        for field in ("clusters", "storage_systems", "links", "routes"):
            facility.setdefault(field, [])
            if not isinstance(facility[field], list):
                raise ValueError(
                    f"WAN facility '{facility_name}' field '{field}' must be an array."
                )
            if facility[field]:
                raise ValueError(
                    f"WAN facility '{facility_name}' already contains '{field}'; "
                    "the population input must be a WAN-only platform."
                )
        facility_spec = require_object(
            facility_specs.get(facility_name, {}),
            f"facilities.{facility_name}",
        )
        facility_storage_spec = merge_objects(
            storage_defaults,
            require_object(
                facility_spec.get("shared_storage", {}),
                f"facilities.{facility_name}.shared_storage",
            ),
        )
        facility_network = merge_objects(
            facility_network_defaults,
            require_object(
                facility_spec.get("facility_network", {}),
                f"facilities.{facility_name}.facility_network",
            ),
        )
        control_host = merge_objects(
            control_defaults,
            require_object(
                facility_spec.get("control_host", {}),
                f"facilities.{facility_name}.control_host",
            ),
        )

        slug = safe_component(facility_name)
        control_zone = f"facility_{slug}_control"
        reserve_zone(control_zone)
        control_hostname_prefix = f"facility_{slug}_control_host_"
        reserve_host(f"{control_hostname_prefix}0")
        network_template = require_object(
            defaults.get("host_network"),
            "defaults.host_network",
        )
        facility["clusters"].append(
            make_loader_cluster(
                zone_name=control_zone,
                host_prefix=control_hostname_prefix,
                count=1,
                speed=require_nonempty_string(
                    control_host.get("speed"), f"{facility_name}.control_host.speed"
                ),
                cores=control_host.get("cores"),
                memory_gb=control_host.get("memory_gb"),
                network=network_template,
                properties={"role": "facility_control", "site": facility_name},
            )
        )
        control_hostname = f"{control_hostname_prefix}0"
        swarm_facility_agents.append(
            {"facility": facility_name, "host": control_hostname}
        )

        facility_storage_name = f"facility_{slug}_shared"
        reserve_storage(facility_storage_name)
        reserve_filesystem(f"{facility_storage_name}_filesystem")
        facility_storage, facility_filesystem = storage_definition(
            facility_storage_name,
            facility_storage_spec,
            f"{facility_storage_name}_filesystem",
            f"/{facility_name}/shared/",
        )
        facility["storage_systems"].append(facility_storage)
        platform["filesystems"].append(facility_filesystem)

        if "clusters" in facility_spec:
            cluster_instances = facility_spec["clusters"]
            if not isinstance(cluster_instances, list) or not cluster_instances:
                raise ValueError(
                    f"facilities.{facility_name}.clusters must be a non-empty array "
                    "when specified."
                )
        else:
            try:
                profile_name = require_nonempty_string(
                    cluster_generation.get("profile"),
                    "defaults.cluster_generation.profile",
                )
                count = cluster_generation.get("count")
                if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                    raise ValueError("defaults.cluster_generation.count must be positive.")
                name_template = require_nonempty_string(
                    cluster_generation.get(
                        "name_template", "{facility}-{profile}-{index}"
                    ),
                    "defaults.cluster_generation.name_template",
                )
                cluster_instances = [
                    {
                        "name": name_template.format(
                            facility=facility_name,
                            profile=profile_name,
                            index=index,
                        ),
                        "profile": profile_name,
                    }
                    for index in range(1, count + 1)
                ]
            except (KeyError, IndexError, ValueError) as exc:
                raise ValueError(f"Invalid default cluster generation rule: {exc}") from exc

        facility_nodes = [control_zone, facility_storage_name]
        facility_edges: list[tuple[str, str, str, float]] = []
        facility_network_bw = require_nonempty_string(
            facility_network.get("bandwidth"),
            f"{facility_name}.facility_network.bandwidth",
        )
        facility_network_latency = require_nonempty_string(
            facility_network.get("latency"),
            f"{facility_name}.facility_network.latency",
        )
        facility_network_latency_ms = parse_milliseconds(
            facility_network_latency,
            f"{facility_name}.facility_network.latency",
        )

        facility_scratch_storages: list[dict[str, Any]] = []
        facility_clusters: list[dict[str, Any]] = []
        for cluster_instance in cluster_instances:
            instance = require_object(
                cluster_instance,
                f"facilities.{facility_name}.clusters entry",
            )
            logical_name = require_nonempty_string(
                instance.get("name"), f"{facility_name} cluster name"
            )
            profile_name = require_nonempty_string(
                instance.get("profile"), f"{logical_name}.profile"
            )
            if profile_name not in profiles:
                raise ValueError(
                    f"Cluster '{logical_name}' references missing profile '{profile_name}'."
                )
            profile = require_object(profiles[profile_name], f"cluster_profiles.{profile_name}")
            overrides = require_object(
                instance.get("overrides", {}),
                f"{logical_name}.overrides",
            )
            resolved = merge_objects(profile, overrides)
            instance_slug = safe_component(logical_name)
            management_zone = f"cluster_{instance_slug}_management"
            compute_zone = f"cluster_{instance_slug}_compute"
            scratch_zone = f"cluster_{instance_slug}_scratch"
            for name in (management_zone, compute_zone):
                reserve_zone(name)

            network = require_object(resolved.get("network"), f"{logical_name}.network")
            management = require_object(
                resolved.get("management_host"),
                f"{logical_name}.management_host",
            )
            compute = require_object(
                resolved.get("compute_nodes"),
                f"{logical_name}.compute_nodes",
            )
            management_prefix = f"host_{instance_slug}_manager_"
            compute_prefix = f"host_{instance_slug}_compute_"
            reserve_host(f"{management_prefix}0")
            compute_count = compute.get("count")
            if isinstance(compute_count, bool) or not isinstance(compute_count, int) or compute_count < 1:
                raise ValueError(
                    f"{logical_name}.compute_nodes.count must be a positive integer."
                )
            for node_index in range(compute_count):
                reserve_host(f"{compute_prefix}{node_index}")
            facility_clusters.append(
                make_loader_cluster(
                    zone_name=management_zone,
                    host_prefix=management_prefix,
                    count=1,
                    speed=require_nonempty_string(
                        management.get("speed"), f"{logical_name}.management_host.speed"
                    ),
                    cores=management.get("cores"),
                    memory_gb=management.get("memory_gb"),
                    network=network,
                    properties={"role": "cluster_management", "site": facility_name, "cluster_id": logical_name},
                )
            )
            compute_storage = compute.get("node_storage")
            if compute_storage is not None:
                compute_storage = require_object(
                    compute_storage,
                    f"{logical_name}.compute_nodes.node_storage",
                )
                compute_storage = {
                    "name": require_nonempty_string(
                        compute_storage.get("name"),
                        f"{logical_name}.compute_nodes.node_storage.name",
                    ),
                    "read_bandwidth": require_nonempty_string(
                        compute_storage.get("read_bandwidth"),
                        f"{logical_name}.compute_nodes.node_storage.read_bandwidth",
                    ),
                    "write_bandwidth": require_nonempty_string(
                        compute_storage.get("write_bandwidth"),
                        f"{logical_name}.compute_nodes.node_storage.write_bandwidth",
                    ),
                }
            facility_clusters.append(
                make_loader_cluster(
                    zone_name=compute_zone,
                    host_prefix=compute_prefix,
                    count=compute.get("count"),
                    speed=require_nonempty_string(
                        compute.get("speed"), f"{logical_name}.compute_nodes.speed"
                    ),
                    cores=compute.get("cores"),
                    memory_gb=compute.get("memory_gb"),
                    network=network,
                    properties={
                        "role": "compute",
                        "site": facility_name,
                        "cluster_id": logical_name,
                        "memory_amount_in_gb": str(require_memory_gb(
                            compute.get("memory_gb"), f"{logical_name}.compute_nodes.memory_gb")),
                    },
                    node_storage=compute_storage,
                )
            )

            scratch_name = f"cluster_{instance_slug}_scratch"
            reserve_storage(scratch_name)
            reserve_filesystem(f"{scratch_name}_filesystem")
            scratch_spec = require_object(resolved.get("scratch"), f"{logical_name}.scratch")
            scratch_storage, scratch_filesystem = storage_definition(
                scratch_name,
                scratch_spec,
                f"{scratch_name}_filesystem",
                f"/scratch/{logical_name}/",
            )
            facility_scratch_storages.append(scratch_storage)
            platform["filesystems"].append(scratch_filesystem)

            access = require_object(network.get("access"), f"{logical_name}.network.access")
            access_bw = require_nonempty_string(
                access.get("bandwidth"), f"{logical_name}.network.access.bandwidth"
            )
            access_latency = parse_milliseconds(
                access.get("latency"),
                f"{logical_name}.network.access.latency",
            )
            facility_edges.extend(
                (
                    (management_zone, compute_zone, access_bw, access_latency),
                    (management_zone, scratch_name, access_bw, access_latency),
                )
            )
            facility_nodes.extend((management_zone, compute_zone, scratch_name))
            swarm_scheduling_agents.append(
                {
                    "facility": facility_name,
                    "cluster": logical_name,
                    "management_host": f"{management_prefix}0",
                    "compute_cluster": compute_zone,
                    "compute_node_count": compute_count,
                    "scratch_storage_system": scratch_name,
                }
            )

            # Facility-network links attach the control plane to each cluster's
            # management host; cluster access links attach its compute/scratch.
            facility_edges.append(
                (
                    control_zone,
                    management_zone,
                    facility_network_bw,
                    facility_network_latency_ms,
                )
            )

        facility["clusters"].extend(facility_clusters)
        facility["storage_systems"].extend(facility_scratch_storages)

        # Connect facility shared storage to the control host. Cluster
        # management hosts connect to the control host through facility_network.
        facility_edges.append(
            (
                control_zone,
                facility_storage_name,
                facility_network_bw,
                facility_network_latency_ms,
            )
        )

        graph: dict[str, list[tuple[str, str, float]]] = {
            node: [] for node in facility_nodes
        }
        for left, right, bandwidth, latency in facility_edges:
            if left == right:
                raise ValueError(f"Facility network contains a self-link at '{left}'.")
            edge_name = (
                f"local_{safe_component(facility_name)}_"
                f"{safe_component(left)}_{safe_component(right)}"
            )
            if edge_name in link_names:
                raise ValueError(f"Generated link name '{edge_name}' is not unique.")
            link_names.add(edge_name)
            facility["links"].append(
                {"name": edge_name, "bandwidth": bandwidth, "latency": f"{latency:g}ms"}
            )
            graph[left].append((right, edge_name, latency))
            graph[right].append((left, edge_name, latency))

        for left, right in combinations(sorted(facility_nodes), 2):
            path = shortest_path(graph, left, right)
            # The JSON loader creates the reverse route automatically.
            facility["routes"].append({"src": left, "dst": right, "links": path})

        # Optional extension consumed by our repository-local loader. Keep the
        # existing local routes and resources; add one shared WAN access link.
        gateway_defaults = defaults.get("wan_gateway_access")
        gateway_override = facility_spec.get("wan_gateway_access")
        if gateway_defaults is not None or gateway_override is not None:
            gateway_access = merge_objects(
                require_object(
                    gateway_defaults if gateway_defaults is not None else {},
                    "defaults.wan_gateway_access",
                ),
                require_object(
                    gateway_override if gateway_override is not None else {},
                    f"facilities.{facility_name}.wan_gateway_access",
                ),
            )
            gateway_bw = require_nonempty_string(
                gateway_access.get("bandwidth"), f"{facility_name}.wan_gateway_access.bandwidth"
            )
            gateway_latency = parse_milliseconds(
                gateway_access.get("latency"), f"{facility_name}.wan_gateway_access.latency"
            )
            access_link = f"local_{slug}_wan_access"
            if access_link in link_names:
                raise ValueError(f"Generated link name '{access_link}' is not unique.")
            link_names.add(access_link)
            facility["links"].append(
                {"name": access_link, "bandwidth": gateway_bw, "latency": f"{gateway_latency:g}ms"}
            )
            facility["wan_gateway"] = {
                "name": f"{facility_name}_router",
                "attachment_zone": control_zone,
                "access_link": access_link,
                "routes": [
                    {
                        "zone": node,
                        "links": shortest_path(graph, node, control_zone) + [access_link],
                    }
                    for node in sorted(facility_nodes)
                ],
            }
            platform["swarm_platform_schema_version"] = 1

    platform["swarm"] = {
        "memory": {
            "unit": "GB",
            "bytes_per_gb": 1000000000,
            "cluster_capacity_scope": "per_compute_node",
            "job_request_scope": "total_across_requested_compute_nodes",
        },
        "facility_agents": swarm_facility_agents,
        "scheduling_agents": swarm_scheduling_agents,
        "note": (
            "Deployment metadata for SWARM; the generic JSON platform loader "
            "constructs the referenced hosts and zones but does not instantiate agents."
        ),
    }
    return platform


def main() -> int:
    args = parse_args()
    try:
        populated = populate_platform(read_json(args.wan), read_json(args.spec))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(populated, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Wrote populated platform: {args.output}")
        return 0
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
