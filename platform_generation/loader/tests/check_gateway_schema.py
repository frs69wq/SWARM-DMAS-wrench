"""Ensure malformed gateway descriptions fail explicitly during loading."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    probe, loader, platform = sys.argv[1:]
    original = json.loads(Path(platform).read_text())
    cases = []

    def case(name, message, mutate):
        data = copy.deepcopy(original)
        mutate(data)
        cases.append((name, message, data))

    case("unsupported-version", "Unsupported SWARM platform schema version",
         lambda data: data.update(swarm_platform_schema_version=2))
    case("missing-version", "wan_gateway requires swarm_platform_schema_version=1",
         lambda data: data.pop("swarm_platform_schema_version"))
    case("missing-child-route", "Missing gateway route",
         lambda data: data["facilities"][0]["wan_gateway"]["routes"].pop())
    case("duplicate-child-route", "Invalid or duplicate gateway route",
         lambda data: data["facilities"][0]["wan_gateway"]["routes"].append(
             copy.deepcopy(data["facilities"][0]["wan_gateway"]["routes"][0])))
    case("unknown-attachment", "Unknown or non-local gateway attachment",
         lambda data: data["facilities"][0]["wan_gateway"].update(attachment_zone="missing"))
    case("unknown-access-link", "Unknown or non-local gateway access link",
         lambda data: data["facilities"][0]["wan_gateway"].update(access_link="missing"))
    case("missing-access-hop", "Gateway route must end with the configured access link",
         lambda data: data["facilities"][0]["wan_gateway"]["routes"][0]["links"].pop())

    with tempfile.TemporaryDirectory(prefix="gateway-schema-", dir=Path.cwd()) as temporary:
        for name, message, data in cases:
            path = Path(temporary) / f"{name}.json"
            path.write_text(json.dumps(data))
            env = dict(os.environ, PLATFORM_CONFIG=str(path))
            result = subprocess.run([probe, loader, "audit"], env=env, capture_output=True,
                                    text=True, timeout=20)
            if result.returncode != 1 or message not in result.stderr:
                raise AssertionError((name, result.returncode, result.stderr))
            print(f"PASS: {name}")


if __name__ == "__main__":
    main()
