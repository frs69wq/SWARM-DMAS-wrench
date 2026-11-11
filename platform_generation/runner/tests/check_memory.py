"""Verify profile overrides, loaded RAM bytes, SWARM cluster scope, and invalid inputs."""

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import subprocess


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    args = parser.parse_args()
    runner = args.runner.resolve()
    output = args.report_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    generation = Path(__file__).resolve().parents[2]
    module = importlib.util.spec_from_file_location("populate_fabric", generation / "populate_fabric_platform.py")
    populate = importlib.util.module_from_spec(module)
    module.loader.exec_module(populate)
    spec = json.loads((generation / "fabric_resource_spec.json").read_text())
    wan = json.loads((generation / "generated/two_facilities/fabric_wan.json").read_text())
    spec["facilities"] = {"ATLA": {
        "control_host": {"memory_gb": 12}, "shared_storage": {"server_memory_gb": 96},
        "clusters": [{"name": "ATLA-memory-test", "profile": "small", "overrides": {
            "compute_nodes": {"memory_gb": 512}, "management_host": {"memory_gb": 48},
            "scratch": {"server_memory_gb": 128}}}]}}
    platform = populate.populate_platform(wan, spec)

    def load(config, name, error=None, stage="instantiate-platform"):
        path = output / f"{name}.json"
        report_path = output / f"{name}-report.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        result = subprocess.run([str(runner), "--check-platform", "--platform", str(path),
                                 "--report", str(report_path)], cwd=output,
                                capture_output=True, text=True, timeout=15)
        require(result.returncode == (1 if error else 0), f"{name}: {result.stderr}")
        report = json.loads(report_path.read_text())
        if error:
            require(report["status"] == "failed" and report["stage"] == stage
                    and error in report["error"], f"{name}: {report}")
        else:
            require(report["status"] == "passed" and report["wrench_platform_accepted"], name)
        return report

    report = load(platform, "overridden-memory")
    loaded = {h["name"]: h for h in report["inventory"]["hosts"]}
    for facility in platform["facilities"]:
        for zone in facility["clusters"]:
            for i in range(zone["count"]):
                host = loaded[f"{zone['prefix']}{i}{zone['suffix']}"]
                gb = zone["node"]["memory_gb"]
                require(host["memory_bytes"] == gb * 10**9 and host["properties"]["mem"] == f"{gb}GB",
                        f"Host RAM was not applied: {host['name']}")
        for store in facility["storage_systems"]:
            require(loaded[store["name"] + "_server"]["memory_bytes"] == store["server_memory_gb"] * 10**9,
                    "Storage server RAM was not applied")
    memory = report["memory_metadata"]
    require(memory["status"] == "passed" and memory["compute_hosts"] == 16, "Compute counts include management hosts")
    clusters = {c["cluster"]: c for c in memory["clusters"]}
    require(clusters["ATLA-memory-test"]["memory_per_compute_node_gb"] == 512
            and clusters["ATLA-memory-test"]["management_memory_gb"] == 48
            and clusters["ATLA-memory-test"]["total_compute_memory_gb"] == 4096, "ATLA memory scope/override differs")
    require(clusters["DALL-small-1"]["memory_per_compute_node_gb"] == 256
            and clusters["DALL-small-1"]["total_compute_memory_gb"] == 2048, "Default compute memory scope differs")

    for key, target in (("memory_gb", "node"), ("server_memory_gb", "storage")):
        for index, invalid in enumerate([True, 0, -1, 1.5, "256", 2**31]):
            bad = copy.deepcopy(platform)
            resource = (bad["facilities"][0]["clusters"][0]["node"] if target == "node"
                        else bad["facilities"][0]["storage_systems"][0])
            resource[key] = invalid
            load(bad, f"invalid-{target}-{index}", f"{key} must be a positive integer GB")
    for key, value, message in (("memory_amount_in_gb", "999", "must match node.memory_gb"),
                                ("memory_amount_in_gb", 512, "must be a string")):
        bad = copy.deepcopy(platform)
        compute = next(z for z in bad["facilities"][0]["clusters"] if z["properties"]["role"] == "compute")
        compute["properties"][key] = value
        load(bad, "invalid-property-" + str(value), message)
    bad = copy.deepcopy(platform)
    bad["facilities"][0]["clusters"][0]["properties"] = []
    load(bad, "invalid-properties-object", "properties must be an object")
    bad = copy.deepcopy(platform)
    bad["swarm"]["memory"]["bytes_per_gb"] = 1024**3
    load(bad, "conflicting-gb-unit", "Unsupported SWARM memory units", "memory-metadata")
    bad = copy.deepcopy(platform)
    bad["swarm"]["scheduling_agents"][0]["compute_node_count"] = 7
    load(bad, "wrong-compute-count", "compute-node mapping", "memory-metadata")

    # Legacy documents without explicit RAM remain loadable. New generation
    # requires RAM; this compatibility check does not supply replacement values.
    legacy = copy.deepcopy(platform)
    del legacy["swarm"]["memory"]
    for facility in legacy["facilities"]:
        for zone in facility["clusters"]:
            del zone["node"]["memory_gb"]
            zone["properties"].pop("memory_amount_in_gb", None)
        for store in facility["storage_systems"]:
            del store["server_memory_gb"]
    legacy_report = load(legacy, "legacy-no-memory")
    require("memory_metadata" not in legacy_report, "Legacy document claimed explicit memory validation")
    require(all(h["memory_bytes"] == 2**63 - 1 for h in legacy_report["inventory"]["hosts"]),
            "Legacy memory default changed")
    print("Memory profiles reach WRENCH and SWARM consistently; invalid and legacy cases verified")


if __name__ == "__main__":
    main()
