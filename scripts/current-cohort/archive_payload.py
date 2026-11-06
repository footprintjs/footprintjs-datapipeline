"""Bounded own npm-pack DATA inspection and exact installed candidate join; never extracts or executes."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tarfile
from urllib.parse import unquote, urlsplit

PACKAGE = "footprintjs-datapipeline"
MAX_ARCHIVE = 64 * 1024 * 1024
MAX_FILE = 16 * 1024 * 1024
MAX_TOTAL = 128 * 1024 * 1024
MAX_MEMBERS = 20_000
MAX_RECEIPT = 8 * 1024 * 1024


class InspectionError(ValueError):
    pass


def require(condition: bool, code: str) -> None:
    if not condition:
        raise InspectionError(code)


def regular(path: Path, limit: int) -> bytes:
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "nonregular-or-linked-input")
    require(0 <= info.st_size <= limit, "input-byte-limit")
    data = path.read_bytes()
    require(len(data) == info.st_size, "input-size-changed")
    return data


def file_target(value: object, consumer: Path) -> Path:
    require(isinstance(value, str), "missing-file-resolution")
    parsed = urlsplit(value)
    require(parsed.scheme == "file" and not parsed.netloc and not parsed.query and not parsed.fragment,
            "nonlocal-file-resolution")
    name = unquote(parsed.path, errors="strict")
    require(bool(name) and "\0" not in name, "bad-file-resolution")
    path = Path(name)
    return (path if path.is_absolute() else consumer / path).resolve(strict=True)


def member_path(name: str, directory: bool) -> str:
    require(len(name.encode("utf-8")) <= 4096 and "\0" not in name and "\\" not in name,
            "bad-member-name")
    clean = name[:-1] if directory and name.endswith("/") else name
    parts = clean.split("/")
    require(parts[0] == "package" and all(part not in ("", ".", "..") for part in parts),
            "noncanonical-member-path")
    require(len(parts) <= 64 and (len(parts) > 1 or directory), "bad-member-depth")
    return "/".join(parts[1:])


def inspect(archive: Path, consumer: Path, version: str, tar_sha: str, lock_sha: str) -> dict:
    compressed = regular(archive, MAX_ARCHIVE)
    archive = archive.resolve(strict=True)
    consumer = consumer.resolve(strict=True)
    actual_tar_sha = hashlib.sha256(compressed).hexdigest()
    require(actual_tar_sha == tar_sha, "archive-input-changed")
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(compressed).digest()).decode("ascii")
    lock_bytes = regular(consumer / "package-lock.json", 16 * 1024 * 1024)
    require(hashlib.sha256(lock_bytes).hexdigest() == lock_sha, "root-lock-input-changed")
    lock = json.loads(lock_bytes)
    leaf = lock["packages"]["node_modules/" + PACKAGE]
    require(file_target(leaf.get("resolved"), consumer) == archive, "candidate-lock-resolved-drift")
    require(leaf.get("integrity") == integrity, "candidate-lock-integrity-drift")
    require(leaf.get("version") == version, "candidate-lock-version-drift")
    root_spec = lock["packages"][""]["dependencies"][PACKAGE]
    manifest = json.loads(regular(consumer / "package.json", 1_048_576))
    require(file_target(root_spec, consumer) == archive and
            file_target(manifest["dependencies"][PACKAGE], consumer) == archive,
            "candidate-root-spec-drift")
    installed = consumer / "node_modules" / PACKAGE
    require(stat.S_ISDIR(installed.lstat().st_mode), "candidate-root-not-regular-directory")
    files: dict[str, dict] = {}
    directories: set[str] = set()
    explicit_modes: dict[str, int] = {}
    seen: set[str] = set()
    total = 0
    count = 0
    # Python's stdlib owns archive parsing. Every byte is read as DATA; nothing is extracted.
    with tarfile.open(fileobj=io.BytesIO(compressed), mode="r:gz") as stream:
        for item in stream:
            count += 1
            require(count <= MAX_MEMBERS, "archive-member-limit")
            require(item.isfile() or item.isdir(), "nonregular-archive-member")
            path = member_path(item.name, item.isdir())
            require(path not in seen, "duplicate-archive-path")
            seen.add(path)
            require(item.mode >= 0 and item.mode == (item.mode & 0o777), "unsupported-archive-mode")
            for parent in PurePosixPath(path).parents:
                if str(parent) != ".":
                    directories.add(str(parent))
            if item.isdir():
                directories.add(path)
                explicit_modes[path] = item.mode
                continue
            require(0 <= item.size <= MAX_FILE, "archive-file-limit")
            total += item.size
            require(total <= MAX_TOTAL, "archive-total-limit")
            handle = stream.extractfile(item)
            require(handle is not None, "unreadable-archive-file")
            with handle:
                data = handle.read(MAX_FILE + 1)
            require(len(data) == item.size, "archive-file-size-drift")
            target = installed / path
            actual = regular(target, MAX_FILE)
            require(actual == data, "installed-file-bytes-drift")
            require(stat.S_IMODE(target.lstat().st_mode) == item.mode, "installed-file-mode-drift")
            if path == "package.json":
                packed = json.loads(data)
                require(packed.get("name") == PACKAGE and packed.get("version") == version,
                        "archive-manifest-identity-drift")
            files[path] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(), "mode": item.mode}
    require("package.json" in files and bool(files), "archive-manifest-missing")
    actual_files: set[str] = set()
    actual_dirs: set[str] = set()
    actual_entries = 0
    for root, dirs, names in os.walk(installed, followlinks=False):
        for name in dirs + names:
            actual_entries += 1
            require(actual_entries <= MAX_MEMBERS * 2, "installed-entry-limit")
            target = Path(root) / name
            info = target.lstat()
            path = target.relative_to(installed).as_posix()
            if stat.S_ISDIR(info.st_mode):
                actual_dirs.add(path)
            else:
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                        "nonregular-or-linked-installed-member")
                actual_files.add(path)
    require(actual_files == set(files), "installed-file-inventory-drift")
    require(actual_dirs == directories - {""}, "installed-directory-inventory-drift")
    for path, mode in explicit_modes.items():
        require(stat.S_IMODE((installed / path).lstat().st_mode) == mode, "installed-directory-mode-drift")
    return {
        "status": "PASSED", "archive": str(archive), "consumer": str(consumer), "version": version,
        "archiveSha256": actual_tar_sha, "archiveIntegrity": integrity, "archiveBytes": len(compressed),
        "rootLockSha256": lock_sha, "resolved": leaf["resolved"], "members": count,
        "files": len(files), "directories": len(directories - {""}), "payloadBytes": total,
        "installedEntries": actual_entries, "payloadComparison": "all regular-file bytes and modes equal",
        "directoryModes": "explicit archive directories only; implicit parents enumerated",
        "inventory": files,
    }


if __name__ == "__main__":
    if len(sys.argv) != 7:
        raise SystemExit("archive_payload.py ARCHIVE CONSUMER RECEIPT VERSION TAR_SHA256 LOCK_SHA256")
    output = Path(sys.argv[3])
    try:
        result = inspect(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[4], sys.argv[5], sys.argv[6])
        encoded = json.dumps(result, indent=2) + "\n"
        require(len(encoded.encode("utf-8")) <= MAX_RECEIPT, "inspection-receipt-byte-limit")
    except Exception as error:
        output.write_text(json.dumps({"status": "FAILED", "error": type(error).__name__, "message": str(error)}, indent=2) + "\n")
        raise
    output.write_text(encoded)
