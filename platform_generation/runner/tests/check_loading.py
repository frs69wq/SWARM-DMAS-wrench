"""Exercise real WRENCH loading and compare its resources with the SimGrid audit.

Patched WRENCH accepts the generated FSMod-backed platform. Unpatched WRENCH
fails its legacy disk-mount check; that mode verifies truthful failure reporting
and intact resources rather than claiming successful loading.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def run(arguments, cwd, env):
    result = subprocess.run(arguments, cwd=cwd, env=env, capture_output=True, text=True, timeout=25)
    require(result.returncode >= 0, f"Process terminated by signal: {result.stderr}")
    return result


def compare_inventory(actual, reference):
    for section in ("hosts", "disks", "links", "filesystems"):
        observed = {item["name"]: item for item in actual[section]}
        expected = {item["name"]: item for item in reference[section]}
        if section == "links":
            require("__loopback__" in observed, "Missing SimGrid loopback")
            del observed["__loopback__"]
        require(observed.keys() == expected.keys(), f"Different {section} IDs")
        for name, item in expected.items():
            for key, value in item.items():
                if key == "occupancy_checks":
                    continue  # The runner inventory is read-only.
                found = observed[name][key]
                same = (math.isclose(found, value, rel_tol=1e-10, abs_tol=1e-12)
                        if isinstance(value, float) else found == value)
                require(same, f"Different {section}/{name}/{key}: {found} != {value}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--loader", type=Path, required=True)
    parser.add_argument("--probe", type=Path)
    parser.add_argument("--platform", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--invalid-inputs", action="store_true")
    parser.add_argument("--compatible-smoke", action="store_true")
    parser.add_argument("--expect-compatible", action="store_true")
    args = parser.parse_args()
    runner, loader, platform = (p.resolve() for p in (args.runner, args.loader, args.platform))
    output = args.report_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    before = hashlib.sha256(platform.read_bytes()).hexdigest()
    env = os.environ.copy()
    # Explicit --platform must override a stale inherited loader environment.
    env["PLATFORM_CONFIG"] = str(output / "nonexistent-inherited-platform.json")
    base = [str(runner), "--check-platform", "--platform", str(platform)]

    if args.compatible_smoke:
        # An independent one-host test input exercises the runner's successful
        # WRENCH lifecycle. It is not a reduced version of the FABRIC platform
        # and makes no claim about JBOD/storage compatibility.
        link = {"bandwidth": "1Gbps", "latency": "1us"}
        fixture = {"swarm_platform_schema_version": 1, "facilities": [{
            "name": "test_facility", "clusters": [{
                "name": "test_control", "prefix": "test_host_", "suffix": "", "count": 1,
                "node": {"speed": "1Gf", "cores": 1, "private_link": link, "loopback": link},
                "backbone": link}]}], "filesystems": [], "links": [], "routes": []}
        smoke_path = output / "minimal-test-platform.json"
        smoke_path.write_text(json.dumps(fixture), encoding="utf-8")
        report_path = output / "wrench-load.json"
        result = run([str(runner), "--check-platform", "--platform", str(smoke_path),
                      "--report", str(report_path)], output, env)
        report = json.loads(report_path.read_text())
        require(result.returncode == 0 and report["status"] == "passed"
                and report["wrench_platform_accepted"], f"Minimal WRENCH loading failed: {result.stderr}")
        require(report["stage"] == "platform-loaded" and report["wrench_and_simgrid_hosts_agree"],
                "Incomplete loading success")
        require(report["counts"] == {"facilities": 1, "hosts": 1, "disks": 0,
                                      "filesystems": 0, "links_including_simgrid_loopback": 5},
                "Unexpected minimal fixture inventory")
        require(not report["agents_attached"] and not report["simulation_launched"], "Unexpected execution")
        print("The independent one-host smoke platform passes the real WRENCH loading lifecycle")
    elif args.invalid_inputs:
        malformed = output / "invalid.json"
        malformed.write_text("{", encoding="utf-8")
        for extra in (["--unknown"], ["--loader", str(output / "absent.so")],
                      ["--platform", str(output / "absent.json")],
                      ["--platform", str(malformed)], ["--report", str(platform)]):
            result = run(base + extra, output, env)
            require(result.returncode == 1, f"Invalid input was accepted: {extra}")
            require(json.loads(result.stdout)["status"] == "failed", "False success report")
        no_mode = run([str(runner), "--platform", str(platform)], output, env)
        require(no_mode.returncode == 1 and "--check-platform" in no_mode.stderr,
                "Unsupported workload execution was accepted")
        help_result = run([str(runner), "--help"], output, env)
        require(help_result.returncode == 0 and "--check-platform" in help_result.stdout, "Help failed")
        config = json.loads(platform.read_text())
        config["swarm_platform_schema_version"] = 99
        unsupported = output / "unsupported-schema.json"
        unsupported.write_text(json.dumps(config), encoding="utf-8")
        result = run([str(runner), "--check-platform", "--platform", str(unsupported),
                      "--loader", str(loader)], output, env)
        report = json.loads(result.stdout)
        require(result.returncode == 1 and report["stage"] == "instantiate-platform"
                and "version" in report["error"].lower(), "Loader schema error was hidden")
        print("Input errors and loader schema errors are reported without changing the platform")
    else:
        report_path = output / "wrench-load.json"
        # Exercise both the built-in local loader path and an explicit override.
        for arguments in ([], ["--loader", str(loader)]):
            result = run(base + arguments + ["--report", str(report_path)], output, env)
            report = json.loads(report_path.read_text())
            if args.expect_compatible:
                require(result.returncode == 0 and report["status"] == "passed"
                        and report["wrench_platform_accepted"] and report["stage"] == "platform-loaded",
                        f"Patched WRENCH loading failed: {report.get('error')}")
                require(report["wrench_fsmod_compat_version"] == 1, "Wrong compatibility patch version")
                require(all(disk["mount_property"] is None for disk in report["inventory"]["disks"]),
                        "Raw disk mounts were changed to bypass compatibility checks")
            else:
                require(result.returncode == 1, "Unpatched WRENCH unexpectedly accepts this platform")
                require(report["status"] == "failed" and not report["wrench_platform_accepted"],
                        "A rejected WRENCH platform was reported as accepted")
                require(report["stage"] == "instantiate-platform" and
                        "two disks with the same mount point '/'" in report["error"],
                        f"Unexpected loading failure: {report.get('error')}")
            require(not report["agents_attached"] and not report["simulation_launched"], "Unexpected execution")
            require(report["wrench_and_simgrid_hosts_agree"], "Different WRENCH host list")
            require(report["platform"] == str(platform) and report["loader"] == str(loader), "Wrong input paths")
        probe_env = env | {"PLATFORM_CONFIG": str(platform)}
        probe = run([str(args.probe.resolve()), str(loader), "audit"], output, probe_env)
        require(probe.returncode == 0, f"SimGrid resource audit failed: {probe.stderr}")
        reference = json.loads(probe.stdout)
        require(reference["status"] == "passed", "Reference resource audit failed")
        compare_inventory(report["inventory"], reference)
        (output / "simgrid-audit.json").write_text(json.dumps(reference, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"test_status": "passed", "wrench_platform_accepted": args.expect_compatible,
                          "inventory_matches_simgrid_audit": True, "counts": report["counts"]}))
    require(hashlib.sha256(platform.read_bytes()).hexdigest() == before, "Generated platform was modified")


if __name__ == "__main__":
    main()
