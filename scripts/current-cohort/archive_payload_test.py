"""Inert archive/manifest/file DATA only; never installs/imports a fake native package."""
import base64
import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

from archive_payload import InspectionError, inspect


class ArchivePayloadTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="dp-pack-data-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.consumer = self.root / "consumer"
        self.installed = self.consumer / "node_modules/footprintjs-datapipeline"
        self.installed.mkdir(parents=True)
        self.archive = self.root / "own-packed.tgz"
        self.files = {
            "package.json": json.dumps({"name": "footprintjs-datapipeline", "version": "0.10.1"}).encode(),
            "notes/fixture.txt": b"inert fixture data; no module execution\n",
            ".hidden/legal.txt": b"hidden payload data\n",
        }
        for name, data in self.files.items():
            path = self.installed / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(0o644)
        self.pack()
        self.consumer_manifest = {"dependencies": {"footprintjs-datapipeline": "file:" + str(self.archive)}}
        self.lock = {"packages": {
            "": self.consumer_manifest,
            "node_modules/footprintjs-datapipeline": {"version": "0.10.1", "resolved": "file:../own-packed.tgz", "integrity": self.integrity()},
        }}
        (self.consumer / "package.json").write_text(json.dumps(self.consumer_manifest))
        self.save_lock()

    def pack(self, extra=()):
        with tarfile.open(self.archive, "w:gz") as stream:
            for name, data in self.files.items():
                row = tarfile.TarInfo("package/" + name)
                row.size = len(data)
                row.mode = 0o644
                stream.addfile(row, io.BytesIO(data))
            for row, data in extra:
                stream.addfile(row, None if data is None else io.BytesIO(data))
        self.tar_sha = hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def integrity(self):
        return "sha512-" + base64.b64encode(hashlib.sha512(self.archive.read_bytes()).digest()).decode()

    def save_lock(self):
        (self.consumer / "package-lock.json").write_text(json.dumps(self.lock))

    def check(self, lock_sha=None):
        current = hashlib.sha256((self.consumer / "package-lock.json").read_bytes()).hexdigest()
        return inspect(self.archive, self.consumer, "0.10.1", self.tar_sha, current if lock_sha is None else lock_sha)

    def test_complete_payload_and_relative_file_resolution_join(self):
        result = self.check()
        self.assertEqual(result["files"], 3)
        self.assertEqual(result["members"], 3)
        self.assertEqual(result["archiveSha256"], self.tar_sha)
        self.assertEqual(result["archiveIntegrity"], self.integrity())
        self.assertIn(".hidden/legal.txt", result["inventory"])
        self.assertEqual(result["payloadComparison"], "all regular-file bytes and modes equal")

    def test_lock_must_name_this_archive_not_an_identical_copy(self):
        other = self.root / "other.tgz"
        shutil.copyfile(self.archive, other)
        self.lock["packages"]["node_modules/footprintjs-datapipeline"]["resolved"] = "file:../other.tgz"
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "candidate-lock-resolved-drift"):
            self.check()

    def test_integrity_and_root_spec_are_actual_file_bindings(self):
        leaf = self.lock["packages"]["node_modules/footprintjs-datapipeline"]
        leaf["integrity"] = "sha512-AAAA"
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "candidate-lock-integrity-drift"):
            self.check()
        leaf["integrity"] = self.integrity()
        other = self.root / "other.tgz"
        shutil.copyfile(self.archive, other)
        self.consumer_manifest["dependencies"]["footprintjs-datapipeline"] = "file:" + str(other)
        (self.consumer / "package.json").write_text(json.dumps(self.consumer_manifest))
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "candidate-root-spec-drift"):
            self.check()

    def test_original_input_digests_must_still_hold_after_consumer_work(self):
        original_lock = hashlib.sha256((self.consumer / "package-lock.json").read_bytes()).hexdigest()
        self.lock["unrelated"] = True
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "root-lock-input-changed"):
            self.check(original_lock)
        self.archive.write_bytes(self.archive.read_bytes() + b"changed")
        with self.assertRaisesRegex(InspectionError, "archive-input-changed"):
            self.check()

    def test_same_version_changed_payload_bytes_or_mode_refuse(self):
        path = self.installed / "notes/fixture.txt"
        path.write_bytes(b"modified while package version stayed 0.10.1\n")
        with self.assertRaisesRegex(InspectionError, "installed-file-bytes-drift"):
            self.check()
        path.write_bytes(self.files["notes/fixture.txt"])
        path.chmod(0o755)
        with self.assertRaisesRegex(InspectionError, "installed-file-mode-drift"):
            self.check()

    def test_extra_file_and_empty_directory_refuse(self):
        extra = self.installed / "extra.txt"
        extra.write_text("extra")
        with self.assertRaisesRegex(InspectionError, "installed-file-inventory-drift"):
            self.check()
        extra.unlink()
        (self.installed / "extra-directory").mkdir()
        with self.assertRaisesRegex(InspectionError, "installed-directory-inventory-drift"):
            self.check()

    def test_missing_or_linked_installed_file_is_not_an_equal_payload(self):
        path = self.installed / "notes/fixture.txt"
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.check()
        other = self.root / "other-data.txt"
        other.write_bytes(self.files["notes/fixture.txt"])
        path.symlink_to(other)
        with self.assertRaisesRegex(InspectionError, "nonregular-or-linked-input"):
            self.check()

    def test_archive_parent_traversal_refuses_before_any_extraction_or_escape_read(self):
        row = tarfile.TarInfo("package/../escape.txt")
        row.size = 0
        row.mode = 0o644
        self.pack([(row, b"")])
        self.lock["packages"]["node_modules/footprintjs-datapipeline"]["integrity"] = self.integrity()
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "noncanonical-member-path"):
            self.check()

    def test_duplicate_path_and_links_are_not_accepted_archive_members(self):
        duplicate = tarfile.TarInfo("package/notes/fixture.txt")
        duplicate.size = len(self.files["notes/fixture.txt"])
        duplicate.mode = 0o644
        self.pack([(duplicate, self.files["notes/fixture.txt"])])
        self.lock["packages"]["node_modules/footprintjs-datapipeline"]["integrity"] = self.integrity()
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "duplicate-archive-path"):
            self.check()
        link = tarfile.TarInfo("package/link")
        link.type = tarfile.SYMTYPE
        link.linkname = "notes/fixture.txt"
        self.pack([(link, None)])
        self.lock["packages"]["node_modules/footprintjs-datapipeline"]["integrity"] = self.integrity()
        self.save_lock()
        with self.assertRaisesRegex(InspectionError, "nonregular-archive-member"):
            self.check()


if __name__ == "__main__":
    unittest.main()
