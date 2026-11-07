import base64
import csv
import hashlib
import io
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_wheel import build_sdist, build_wheel


class PackagingTests(unittest.TestCase):
    def test_sdist_is_deterministic_and_rebuilds_the_same_wheel(self):
        import importlib.util

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / build_sdist(directory)
            first = path.read_bytes()
            build_sdist(directory)
            self.assertEqual(first, path.read_bytes())
            with tarfile.open(path, "r:gz") as archive:
                names = archive.getnames()
                self.assertTrue(
                    all(name.startswith("tldw_client-0.2.0/") for name in names)
                )
                self.assertIn("tldw_client-0.2.0/PKG-INFO", names)
                self.assertFalse(
                    any("__pycache__" in name or ".env" in name for name in names)
                )
                for member in archive.getmembers():
                    self.assertTrue(member.isfile())
                    self.assertNotIn("..", Path(member.name).parts)
                    target = root / member.name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(archive.extractfile(member).read())
            spec = importlib.util.spec_from_file_location(
                "rebuilt", root / "tldw_client-0.2.0/build_wheel.py"
            )
            rebuilt = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(rebuilt)
            original = root / "original"
            recovered = root / "recovered"
            expected = original / build_wheel(original)
            actual = recovered / rebuilt.build_wheel(recovered)
            self.assertEqual(expected.read_bytes(), actual.read_bytes())

    def test_modern_metadata_includes_license_and_project_links(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / build_wheel(directory)
            with zipfile.ZipFile(path) as archive:
                metadata = archive.read("tldw_client-0.2.0.dist-info/METADATA").decode()
                self.assertIn("License-Expression: MIT", metadata)
                self.assertIn(
                    "Project-URL: Source, https://github.com/tldwdotsh/tldw-python",
                    metadata,
                )
                self.assertIn("License-File: LICENSE", metadata)
                self.assertIn(
                    "Project-URL: Documentation, https://tldw.sh/docs", metadata
                )

    def test_offline_wheel_is_deterministic_and_only_contains_public_package_files(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / build_wheel(directory)
            first = path.read_bytes()
            build_wheel(directory)
            self.assertEqual(first, path.read_bytes())
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                self.assertEqual(len(names), 5)
                self.assertIn("tldw_client/__init__.py", names)
                self.assertNotIn(".env", " ".join(names))
                records = list(
                    csv.reader(
                        io.StringIO(
                            archive.read("tldw_client-0.2.0.dist-info/RECORD").decode()
                        )
                    )
                )
                for name, digest, length in records:
                    if not digest:
                        continue
                    content = archive.read(name)
                    expected = "sha256=" + base64.urlsafe_b64encode(
                        hashlib.sha256(content).digest()
                    ).decode().rstrip("=")
                    self.assertEqual(digest, expected)
                    self.assertEqual(int(length), len(content))


if __name__ == "__main__":
    unittest.main()
