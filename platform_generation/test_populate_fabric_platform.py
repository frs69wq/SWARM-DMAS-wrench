import unittest

from populate_fabric_platform import populate_platform


def small_wan():
    return {
        "facilities": [
            {
                "name": "ATLA",
                "storage_systems": [],
                "clusters": [],
                "links": [],
                "routes": [],
            },
            {
                "name": "DALL",
                "storage_systems": [],
                "clusters": [],
                "links": [],
                "routes": [],
            },
        ],
        "storage_systems": [],
        "links": [{"name": "wan_ATLA_DALL", "bandwidth": "100Gbps", "latency": "8ms"}],
        "routes": [{"src": "ATLA", "dst": "DALL", "links": ["wan_ATLA_DALL"]}],
        "filesystems": [],
    }


def small_spec():
    return {
        "schema_version": 1,
        "defaults": {
            "facility_control_host": {"speed": "2Gf", "cores": 2},
            "host_network": {
                "backbone": {"bandwidth": "10Gbps", "latency": "0.1ms"},
                "node": {
                    "private_link": {"bandwidth": "10Gbps", "latency": "0.1ms"},
                    "loopback": {"bandwidth": "1Gbps", "latency": "0.01ms"},
                },
            },
            "facility_shared_storage": {
                "server_speed": "10Gf",
                "type": "JBOD",
                "disk_count": 2,
                "read_bandwidth": "2GBps",
                "write_bandwidth": "1GBps",
                "capacity": "20TB",
            },
            "facility_network": {"bandwidth": "10Gbps", "latency": "1ms"},
            "cluster_generation": {
                "profile": "small",
                "count": 2,
                "name_template": "{facility}-{profile}-{index}",
            },
        },
        "cluster_profiles": {
            "small": {
                "management_host": {"speed": "2Gf", "cores": 2},
                "compute_nodes": {"count": 2, "speed": "1Tf", "cores": 8},
                "scratch": {
                    "server_speed": "10Gf",
                    "type": "OneDisk",
                    "disk_count": 1,
                    "read_bandwidth": "1GBps",
                    "write_bandwidth": "1GBps",
                    "capacity": "10TB",
                },
                "network": {
                    "backbone": {"bandwidth": "10Gbps", "latency": "0.1ms"},
                    "node": {
                        "private_link": {"bandwidth": "2Gbps", "latency": "0.1ms"},
                        "loopback": {"bandwidth": "1Gbps", "latency": "0.01ms"},
                    },
                    "access": {"bandwidth": "5Gbps", "latency": "0.5ms"},
                },
            }
        },
        "facilities": {},
    }


class PopulateFabricPlatformTests(unittest.TestCase):
    def test_gateway_access_preserves_existing_resources_and_paths(self):
        spec = small_spec()
        original = populate_platform(small_wan(), spec)
        spec["defaults"]["wan_gateway_access"] = {"bandwidth": "40Gbps", "latency": "0.25ms"}
        spec["facilities"] = {"DALL": {"wan_gateway_access": {"bandwidth": "50Gbps"}}}
        platform = populate_platform(small_wan(), spec)
        self.assertEqual(platform["swarm_platform_schema_version"], 1)
        for field in ("links", "routes", "filesystems", "swarm"):
            self.assertEqual(platform[field], original[field])
        for old, new in zip(original["facilities"], platform["facilities"]):
            for field in ("name", "clusters", "storage_systems", "routes"):
                self.assertEqual(new[field], old[field])
            self.assertEqual(new["links"][:-1], old["links"])
            access = new["links"][-1]
            self.assertEqual(access["bandwidth"], "50Gbps" if new["name"] == "DALL" else "40Gbps")
            self.assertEqual(access["latency"], "0.25ms")
            gateway = new["wan_gateway"]
            attachment = gateway["attachment_zone"]
            self.assertEqual(gateway["access_link"], access["name"])
            self.assertEqual(gateway["name"], new["name"] + "_router")
            paths = {}
            for route in new["routes"]:
                paths[route["src"], route["dst"]] = route["links"]
                paths[route["dst"], route["src"]] = list(reversed(route["links"]))
            nodes = {zone["name"] for zone in new["clusters"] + new["storage_systems"]}
            self.assertEqual({route["zone"] for route in gateway["routes"]}, nodes)
            self.assertEqual(len(gateway["routes"]), len(nodes))
            for route in gateway["routes"]:
                expected = [] if route["zone"] == attachment else paths[route["zone"], attachment]
                self.assertEqual(route["links"], expected + [access["name"]])

    def test_invalid_gateway_latency_is_rejected(self):
        spec = small_spec()
        spec["defaults"]["wan_gateway_access"] = {"bandwidth": "40Gbps", "latency": "-1ms"}
        with self.assertRaisesRegex(ValueError, "wan_gateway_access.latency"):
            populate_platform(small_wan(), spec)

    def test_missing_gateway_bandwidth_is_rejected(self):
        spec = small_spec()
        spec["defaults"]["wan_gateway_access"] = {"latency": "1ms"}
        with self.assertRaisesRegex(ValueError, "wan_gateway_access.bandwidth"):
            populate_platform(small_wan(), spec)

    def test_defaults_overrides_storage_and_network_routes(self):
        spec = small_spec()
        spec["facilities"] = {
            "ATLA": {
                "shared_storage": {"capacity": "25TB"},
                "facility_network": {"bandwidth": "20Gbps"},
                "clusters": [
                    {
                        "name": "atla-special",
                        "profile": "small",
                        "overrides": {"scratch": {"capacity": "15TB"}},
                    }
                ],
            }
        }

        platform = populate_platform(small_wan(), spec)
        facilities = {facility["name"]: facility for facility in platform["facilities"]}
        atla = facilities["ATLA"]
        dall = facilities["DALL"]

        atla_clusters = {cluster["name"]: cluster for cluster in atla["clusters"]}
        dall_cluster_names = {cluster["name"] for cluster in dall["clusters"]}
        self.assertIn("cluster_atla_special_management", atla_clusters)
        self.assertIn("cluster_atla_special_compute", atla_clusters)
        self.assertEqual(
            dall_cluster_names,
            {
                "facility_DALL_control",
                "cluster_DALL_small_1_management",
                "cluster_DALL_small_1_compute",
                "cluster_DALL_small_2_management",
                "cluster_DALL_small_2_compute",
            },
        )
        self.assertEqual(
            atla_clusters["cluster_atla_special_compute"]["count"],
            2,
        )

        storage_by_name = {
            storage["name"]: storage
            for facility in platform["facilities"]
            for storage in facility["storage_systems"]
        }
        facility_storage = storage_by_name["facility_ATLA_shared"]
        scratch_storage = storage_by_name["cluster_atla_special_scratch"]
        self.assertEqual(facility_storage["read_bandwidth"], "2GBps")
        self.assertEqual(scratch_storage["read_bandwidth"], "1GBps")

        filesystems = {fs["name"]: fs for fs in platform["filesystems"]}
        self.assertEqual(filesystems["facility_ATLA_shared_filesystem"]["size"], "25TB")
        self.assertEqual(filesystems["cluster_atla_special_scratch_filesystem"]["size"], "15TB")
        self.assertNotIn("cluster", filesystems["cluster_atla_special_scratch_filesystem"])

        atla_local_links = {link["name"]: link for link in atla["links"]}
        self.assertTrue(
            any(link["bandwidth"] == "20Gbps" for link in atla_local_links.values())
        )
        self.assertEqual(platform["routes"][0]["src"], "ATLA")
        self.assertEqual(platform["routes"][0]["links"], ["wan_ATLA_DALL"])

        for facility in platform["facilities"]:
            links = {link["name"] for link in facility["links"]}
            route_pairs = set()
            reachable_pairs = set()
            zones = {
                cluster["name"] for cluster in facility["clusters"]
            } | {
                storage["name"] for storage in facility["storage_systems"]
            }
            self.assertEqual(len(facility["routes"]), len(zones) * (len(zones) - 1) // 2)
            for route in facility["routes"]:
                # Each record is symmetric in the loader; emitting its reverse
                # separately would try to register the same connection twice.
                pair = frozenset((route["src"], route["dst"]))
                self.assertNotIn(pair, route_pairs)
                route_pairs.add(pair)
                reachable_pairs.add((route["src"], route["dst"]))
                reachable_pairs.add((route["dst"], route["src"]))
                self.assertTrue(route["links"])
                self.assertTrue(set(route["links"]) <= links)
            self.assertEqual(
                reachable_pairs,
                {(left, right) for left in zones for right in zones if left != right},
            )

        self.assertEqual(len(platform["swarm"]["facility_agents"]), 2)
        self.assertEqual(len(platform["swarm"]["scheduling_agents"]), 3)

    def test_unknown_facility_is_rejected(self):
        spec = small_spec()
        spec["facilities"] = {"NOPE": {}}
        with self.assertRaisesRegex(ValueError, "unknown facilities: NOPE"):
            populate_platform(small_wan(), spec)

    def test_missing_cluster_profile_is_rejected(self):
        spec = small_spec()
        spec["facilities"] = {
            "ATLA": {
                "clusters": [{"name": "atla-unknown", "profile": "missing"}]
            }
        }
        with self.assertRaisesRegex(ValueError, "missing profile 'missing'"):
            populate_platform(small_wan(), spec)

    def test_duplicate_cluster_names_are_rejected(self):
        spec = small_spec()
        spec["facilities"] = {
            "ATLA": {
                "clusters": [
                    {"name": "duplicate", "profile": "small"},
                    {"name": "duplicate", "profile": "small"},
                ]
            }
        }
        with self.assertRaisesRegex(ValueError, "not unique"):
            populate_platform(small_wan(), spec)

    def test_empty_explicit_cluster_list_does_not_fall_back_to_default(self):
        spec = small_spec()
        spec["facilities"] = {"ATLA": {"clusters": []}}
        with self.assertRaisesRegex(ValueError, "must be a non-empty array"):
            populate_platform(small_wan(), spec)


if __name__ == "__main__":
    unittest.main()
