"""Finite own-pack DATA binder controls; no registry/runtime."""
import base64
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('publish-candidate.py')
BINDER = None
if SCRIPT.exists():
    SPEC = importlib.util.spec_from_file_location('candidate_data_binder', SCRIPT)
    BINDER = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(BINDER)


class CandidateFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='dp-bind-data-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.incoming = self.root / 'incoming'
        self.incoming.mkdir()
        self.source = self.root / 'source.json'
        self.event = self.root / 'event.json'
        self.manifest = {'name': 'footprintjs-datapipeline', 'version': '0.11.0',
                         'type': 'module', 'files': ['dist'],
                         'dependencies': {'footprintjs': '^11.0.0', 'foottrace': '^1.11.1'}}
        self.source.write_text(json.dumps(self.manifest))
        self.event_data = {'action': 'published', 'release': {
            'tag_name': 'v0.11.0', 'draft': False, 'prerelease': False}}
        self.event.write_text(json.dumps(self.event_data))
        self.env = {'GITHUB_EVENT_NAME': 'release',
                    'GITHUB_REPOSITORY': 'footprintjs/footprintjs-datapipeline',
                    'GITHUB_EVENT_PATH': str(self.event), 'GITHUB_SHA': 'a' * 40,
                    'RELEASE_TAG': 'v0.11.0', 'GITHUB_REF': 'refs/tags/v0.11.0',
                    'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2',
                    'INCOMING': str(self.incoming), 'SEALED': str(self.root / 'sealed')}
        self.pack()

    def pack(self, manifest=None, duplicate=False, manifest_kind=None):
        buffer = io.BytesIO()
        payload = json.dumps(self.manifest if manifest is None else manifest).encode()
        with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
            for _ in range(2 if duplicate else 1):
                item = tarfile.TarInfo('package/package.json')
                item.mode = 0o644
                item.size = len(payload)
                if manifest_kind is not None:
                    item.type = manifest_kind
                    item.linkname = 'package/elsewhere'
                    item.size = 0
                archive.addfile(item, io.BytesIO(payload) if manifest_kind is None else None)
            data = b'finite inert payload, never imported'
            item = tarfile.TarInfo('package/dist/index.js')
            item.mode = 0o644
            item.size = len(data)
            archive.addfile(item, io.BytesIO(data))
        self.bytes = buffer.getvalue()
        (self.incoming / 'candidate.tgz').write_bytes(self.bytes)
        self.metadata = {'name': self.manifest['name'], 'version': self.manifest['version'],
                         'bytes': len(self.bytes), 'sha256': hashlib.sha256(self.bytes).hexdigest()}
        self.save_metadata()

    def save_metadata(self):
        (self.incoming / 'candidate.json').write_text(json.dumps(self.metadata))

    def refuse(self, code):
        self.assertIsNotNone(BINDER, 'The candidate binder must be present before admission can pass')
        with self.assertRaisesRegex(ValueError, code):
            BINDER.bind(self.source, self.env)
        self.assertFalse((self.root / 'sealed' / 'package.tgz').exists())

    def test_positive_seals_exact_original_bytes_and_closed_origin_fields(self):
        self.assertIsNotNone(BINDER, 'The candidate binder must be present before admission can pass')
        result = BINDER.bind(self.source, self.env)
        self.assertEqual((self.root / 'sealed' / 'package.tgz').read_bytes(), self.bytes)
        self.assertEqual(result, {'sha256': hashlib.sha256(self.bytes).hexdigest(),
            'integrity': 'sha512-' + base64.b64encode(hashlib.sha512(self.bytes).digest()).decode(),
            'version': '0.11.0', 'source': 'a' * 40, 'tag': 'v0.11.0', 'run': '123', 'attempt': '2'})

    def test_forged_pack_with_same_name_version_and_new_matching_digest_is_not_source_manifest(self):
        self.pack({**self.manifest, 'description': 'not in immutable source'})
        self.refuse('source-manifest-drift')

    def test_wrong_package_manifest(self):
        self.pack({**self.manifest, 'name': 'other-package'})
        self.refuse('source-manifest-drift')

    def test_wrong_version_manifest(self):
        self.pack({**self.manifest, 'version': '0.11.1'})
        self.refuse('source-manifest-drift')

    def test_stale_digest_refuses_changed_compressed_bytes(self):
        (self.incoming / 'candidate.tgz').write_bytes(self.bytes + b'changed')
        self.refuse('tested-candidate-drift')

    def test_declared_byte_length_is_checked(self):
        self.metadata['bytes'] += 1
        self.save_metadata()
        self.refuse('tested-candidate-drift')

    def test_unknown_candidate_metadata_key_is_not_accepted(self):
        self.metadata['source'] = 'b' * 40
        self.save_metadata()
        self.refuse('tested-candidate-drift')

    def test_duplicate_metadata_keys_refuse(self):
        (self.incoming / 'candidate.json').write_text('{"name":"first","name":"second"}')
        self.refuse('duplicate-json-key')

    def test_duplicate_source_keys_refuse(self):
        self.source.write_text('{"name":"one","name":"two"}')
        self.refuse('duplicate-json-key')

    def test_duplicate_archive_manifest_refuses(self):
        self.pack(duplicate=True)
        self.refuse('source-manifest-drift')

    def test_nonregular_manifest_refuses(self):
        self.pack(manifest_kind=tarfile.SYMTYPE)
        self.refuse('archive-manifest-kind')

    def test_extra_intake_file_refuses(self):
        (self.incoming / '.npmrc').write_text('inert-data')
        self.refuse('candidate-intake')

    def test_archive_symlink_refuses_before_read(self):
        archive = self.incoming / 'candidate.tgz'
        archive.rename(self.root / 'other.tgz')
        archive.symlink_to(self.root / 'other.tgz')
        self.refuse('candidate-input-kind')

    def test_archive_hardlink_refuses_before_read(self):
        os.link(self.incoming / 'candidate.tgz', self.root / 'second-link.tgz')
        self.refuse('candidate-input-kind')

    def test_symlink_intake_root_refuses(self):
        self.incoming.rename(self.root / 'real-incoming')
        self.incoming.symlink_to(self.root / 'real-incoming', target_is_directory=True)
        self.refuse('candidate-intake')

    def test_sparse_oversized_archive_refuses_without_reading_payload(self):
        with (self.incoming / 'candidate.tgz').open('wb') as output:
            output.truncate(64 * 1024 * 1024 + 1)
        self.refuse('candidate-input-kind')

    def test_origin_fields_refuse_independently(self):
        for key, value, code in [
            ('GITHUB_REPOSITORY', 'other/repository', 'release-event'),
            ('GITHUB_EVENT_NAME', 'push', 'release-event'),
            ('GITHUB_SHA', 'short', 'release-origin'),
            ('GITHUB_RUN_ID', '0', 'release-origin'),
            ('GITHUB_RUN_ATTEMPT', 'bad', 'release-origin'),
            ('RELEASE_TAG', 'v0.11.1', 'release-tag'),
            ('GITHUB_REF', 'refs/heads/main', 'release-tag'),
        ]:
            with self.subTest(key=key):
                old = self.env[key]
                self.env[key] = value
                self.refuse(code)
                self.env[key] = old

    def test_release_states_refuse(self):
        for field, value in [('action', 'created'), ('draft', True), ('prerelease', True)]:
            with self.subTest(field=field):
                event = json.loads(json.dumps(self.event_data))
                target = event if field == 'action' else event['release']
                target[field] = value
                self.event.write_text(json.dumps(event))
                self.refuse('release-state')

    def test_event_tag_must_match_the_source_version(self):
        self.event_data['release']['tag_name'] = 'v0.11.9'
        self.event.write_text(json.dumps(self.event_data))
        self.refuse('release-tag')

    def test_python_optimization_cannot_remove_the_manifest_guard(self):
        self.pack({**self.manifest, 'description': 'different source'})
        result = subprocess.run([sys.executable, '-O', str(SCRIPT), str(self.source)],
            env={**self.env, 'GITHUB_OUTPUT': str(self.root / 'outputs')},
            capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('source-manifest-drift', result.stderr)
        self.assertFalse((self.root / 'sealed' / 'package.tgz').exists())


if __name__ == '__main__':
    unittest.main()
