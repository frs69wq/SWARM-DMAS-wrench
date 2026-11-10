"""Exercise every facility controller and storage filesystem remotely."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("probe", "loader", "platform", "report-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    platform = args.platform.resolve()
    data = json.loads(platform.read_text())
    output = args.report_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    controls = {entry["facility"]: entry["host"] for entry in data["swarm"]["facility_agents"]}
    facilities = {entry["name"]: entry for entry in data["facilities"]}
    names = sorted(facilities)
    if len(names) != 27 or set(controls) != set(names):
        raise ValueError("Full-platform checks expect 27 facilities with control-host mappings")
    remote = {name: names[(i + 1) % len(names)] for i, name in enumerate(names)}
    storage_facility = {
        storage["name"]: name
        for name, facility in facilities.items()
        for storage in facility["storage_systems"]
    }
    compute = {}
    for entry in data["swarm"]["scheduling_agents"]:
        facility = facilities[entry["facility"]]
        cluster = next(zone for zone in facility["clusters"] if zone["name"] == entry["compute_cluster"])
        compute.setdefault(entry["facility"], cluster["prefix"] + "0" + cluster["suffix"])
    tasks = []
    for name in names:
        tasks.append(("controller-" + name, ["comm", controls[name], controls[remote[name]]]))
    for filesystem in data["filesystems"]:
        owner = storage_facility[filesystem["storage_system"]]
        source = remote[owner]
        # Exercise both controller-to-shared and compute-to-scratch data access.
        client = controls[source] if filesystem["storage_system"].startswith("facility_") else compute[source]
        tasks.append(("filesystem-" + filesystem["name"], ["filesystem", filesystem["name"], client]))
    env = dict(os.environ, PLATFORM_CONFIG=str(platform))

    def run(task):
        label, probe_args = task
        result = subprocess.run([str(args.probe.resolve()), str(args.loader.resolve()), *probe_args],
                                env=env, capture_output=True, text=True, timeout=30)
        (output / f"{label}.stdout.txt").write_text(result.stdout)
        (output / f"{label}.stderr.txt").write_text(result.stderr)
        if result.returncode != 0:
            raise AssertionError((label, result.returncode, result.stderr))
        record = json.loads(result.stdout.strip().splitlines()[-1])
        if record.get("status") != "passed":
            raise AssertionError((label, record))
        return {"case": label, "arguments": probe_args, "result": record}

    results = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = [pool.submit(run, task) for task in tasks]
        for future in as_completed(pending):
            results.append(future.result())
    results.sort(key=lambda entry: entry["case"])
    summary = {
        "status": "passed",
        "controller_roundtrips": len(names),
        "remote_filesystems_checked": len(data["filesystems"]),
        "checks_passed": len(results),
        "platform": str(platform),
        "results": results,
    }
    (output / "remote-coverage.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({key: value for key, value in summary.items() if key != "results"}), flush=True)


if __name__ == "__main__":
    main()
