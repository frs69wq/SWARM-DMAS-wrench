#!/usr/bin/env python3
"""Build a pinned, patched WRENCH in this repository's ignored build directory."""

import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile


def run(arguments, **kwargs):
    subprocess.run([str(arg) for arg in arguments], check=True, **kwargs)


def main():
    here = Path(__file__).resolve().parent
    root = here.parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repo", type=Path, required=True, help="Read-only local WRENCH Git checkout")
    parser.add_argument("--output", type=Path, default=root / "build/wrench-compat-v1")
    parser.add_argument("--simgrid", type=Path, required=True)
    parser.add_argument("--fsmod", type=Path, required=True)
    parser.add_argument("--dependency-prefix", type=Path, required=True)
    parser.add_argument("--cxx", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--cmake-arg", action="append", default=[])
    parser.add_argument("--prepare-only", action="store_true", help="Extract and apply the patch without building")
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(root / "build"):
        parser.error("--output must be inside this repository's build/ directory")
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    upstream = json.loads((here / "upstream.json").read_text())
    patch = here / upstream["patch"]
    digest = hashlib.sha256(patch.read_bytes()).hexdigest()
    if digest != upstream["patch_sha256"]:
        raise SystemExit("Patch checksum does not match upstream.json")
    output.mkdir(parents=True, exist_ok=True)
    source = output / "source"
    stamp = output / "prepared-source.json"
    identity = {"commit": upstream["commit"], "patch_sha256": digest}
    if source.exists():
        if not stamp.exists() or json.loads(stamp.read_text()) != identity:
            raise SystemExit("Existing source has no matching provenance stamp; choose a fresh --output directory")
    else:
        # git archive reads the pinned commit; working-tree changes and local
        # build files in the external repository are never copied or modified.
        archive = subprocess.check_output(["git", "-C", str(args.source_repo.resolve()),
                                           "archive", upstream["commit"]])
        source.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as package:
            package.extractall(source, filter="data")
        run(["patch", "--batch", "--forward", "--dry-run", "-p1", "-i", patch], cwd=source)
        run(["patch", "--batch", "--forward", "-p1", "-i", patch], cwd=source)
        stamp.write_text(json.dumps(identity, indent=2) + "\n")
    if args.prepare_only:
        print(f"Pinned patched source ready: {source}")
        return
    install = output / "install"
    configure = ["cmake", "-S", source, "-B", output / "cmake",
                 f"-DCMAKE_CXX_COMPILER={args.cxx.resolve()}",
                 f"-DSimGrid_PATH={args.simgrid.resolve()}", f"-DFSMOD_PATH={args.fsmod.resolve()}",
                 f"-DCMAKE_PREFIX_PATH={args.dependency_prefix.resolve()}",
                 f"-DCMAKE_INSTALL_PREFIX={install}"]
    run(configure + args.cmake_arg)
    run(["cmake", "--build", output / "cmake", "--target", "wrench", "wrenchwfcommonsworkflowparser",
         "-j", args.jobs])
    # Stage only the two built libraries and public headers; upstream's global
    # install rule also requires unrelated tools/examples to be built.
    shutil.copytree(source / "include", install / "include", dirs_exist_ok=True)
    (install / "lib").mkdir(parents=True, exist_ok=True)
    for library in ("libwrench.a", "libwrenchwfcommonsworkflowparser.a"):
        candidates = list((output / "cmake").rglob(library))
        if len(candidates) != 1:
            raise SystemExit(f"Expected one built {library}, found {len(candidates)}")
        shutil.copy2(candidates[0], install / "lib" / library)
    provenance = install / "share/swarm-wrench-compat"
    provenance.mkdir(parents=True, exist_ok=True)
    shutil.copy2(patch, provenance / patch.name)
    shutil.copy2(source / "LICENSE.md", provenance / "LICENSE.md")
    (provenance / "upstream.json").write_text(json.dumps(upstream, indent=2) + "\n")
    print(f"Patched WRENCH ready: -DWRENCH_PATH={install}")


if __name__ == "__main__":
    main()
