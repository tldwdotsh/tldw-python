"""Minimal, dependency-free PEP 517 wheel backend for this pure Python package.

Run ``python build_wheel.py dist`` or use ``pip wheel --no-build-isolation .``.
Only the explicitly listed package sources and public metadata enter the wheel.
"""

import base64
import csv
import gzip
import hashlib
import io
import re
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NAME = "tldw_client"
VERSION = "0.2.5"
INFO = f"{NAME}-{VERSION}.dist-info"
METADATA = f"""Metadata-Version: 2.4
Name: tldw-client
Version: {VERSION}
Summary: Dependency-free tldw.sh asynchronous analysis client
Requires-Python: >=3.10
License-Expression: MIT
License-File: LICENSE
Keywords: tldw,video,analysis,agents,mcp,api
Classifier: Development Status :: 4 - Beta
Classifier: Intended Audience :: Developers
Classifier: Operating System :: OS Independent
Classifier: Programming Language :: Python :: 3 :: Only
Classifier: Programming Language :: Python :: 3.10
Classifier: Programming Language :: Python :: 3.11
Classifier: Programming Language :: Python :: 3.12
Classifier: Programming Language :: Python :: 3.13
Classifier: Programming Language :: Python :: 3.14
Classifier: Topic :: Software Development :: Libraries :: Python Modules
Project-URL: Homepage, https://tldw.sh
Project-URL: Documentation, https://tldw.sh/docs
Project-URL: Source, https://github.com/tldwdotsh/tldw-python
Project-URL: Issues, https://github.com/tldwdotsh/tldw-python/issues
Description-Content-Type: text/markdown

"""
WHEEL = "Wheel-Version: 1.0\nGenerator: tldw-stdlib-wheel\nRoot-Is-Purelib: true\nTag: py3-none-any\n"


def _metadata():
    _check_version()
    return {
        f"{INFO}/METADATA": (METADATA + _read_source("README.md").decode()).encode(),
        f"{INFO}/WHEEL": WHEEL.encode(),
        f"{INFO}/licenses/LICENSE": _read_source("LICENSE"),
    }


def _check_version():
    project = _read_source("pyproject.toml").decode()
    package = _read_source(f"{NAME}/__init__.py").decode()
    project_version = re.search(r'^version\s*=\s*"([^\"]+)"', project, re.MULTILINE)
    package_version = re.search(r'^__version__\s*=\s*"([^\"]+)"', package, re.MULTILINE)
    if (
        not project_version
        or not package_version
        or {VERSION, project_version[1], package_version[1]} != {VERSION}
    ):
        raise ValueError(
            "Client version differs between pyproject, runtime and build metadata"
        )


def _read_source(name):
    path = ROOT / name
    parents = (
        parent for parent in path.parents if parent != ROOT and ROOT in parent.parents
    )
    if (
        not path.is_file()
        or path.is_symlink()
        or any(parent.is_symlink() for parent in parents)
    ):
        raise ValueError("Package sources must be regular, non-symlink files")
    return path.read_bytes()


def build_sdist(sdist_directory, config_settings=None):
    """Build a reviewed source archive that a clean PEP 517 frontend can rebuild."""
    destination = Path(sdist_directory)
    destination.mkdir(parents=True, exist_ok=True)
    files = {
        name: _read_source(name)
        for name in (
            "pyproject.toml",
            "build_wheel.py",
            "README.md",
            "LICENSE",
            f"{NAME}/__init__.py",
        )
    }
    files.update(
        {
            path.relative_to(ROOT).as_posix(): _read_source(path.relative_to(ROOT))
            for path in sorted((ROOT / "tests").glob("test_*.py"))
        }
    )
    files["PKG-INFO"] = _metadata()[f"{INFO}/METADATA"]
    filename = f"{NAME}-{VERSION}.tar.gz"
    with (
        (destination / filename).open("wb") as raw,
        # Match the reviewed release format across Python's changing gzip default.
        gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, compresslevel=9) as compressed,
        tarfile.open(
            fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT
        ) as archive,
    ):
        for name, content in sorted(files.items()):
            entry = tarfile.TarInfo(f"{NAME}-{VERSION}/{name}")
            entry.size = len(content)
            entry.mode = 0o644
            entry.mtime = 0
            archive.addfile(entry, io.BytesIO(content))
    return filename


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    destination = Path(wheel_directory)
    destination.mkdir(parents=True, exist_ok=True)
    files = {
        f"{NAME}/__init__.py": _read_source(f"{NAME}/__init__.py"),
        **_metadata(),
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, content in sorted(files.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(content).digest())
            .decode()
            .rstrip("=")
        )
        writer.writerow((path, "sha256=" + digest, str(len(content))))
    writer.writerow((f"{INFO}/RECORD", "", ""))
    files[f"{INFO}/RECORD"] = record.getvalue().encode()
    filename = f"{NAME}-{VERSION}-py3-none-any.whl"
    with zipfile.ZipFile(destination / filename, "w", zipfile.ZIP_DEFLATED) as archive:
        for path, content in sorted(files.items()):
            info = zipfile.ZipInfo(path, (2026, 11, 4, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, content)
    return filename


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    for path, content in _metadata().items():
        destination = Path(metadata_directory) / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    return INFO


def get_requires_for_build_wheel(config_settings=None):
    return []


def get_requires_for_build_sdist(config_settings=None):
    return []


if __name__ == "__main__":
    print(build_wheel(sys.argv[1] if len(sys.argv) > 1 else ROOT / "dist"))
