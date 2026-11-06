"""Bind already-tested pack data; no publish or lifecycle."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
import tarfile


def closed_json(data):
    def fields(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError('duplicate-json-key')
            out[key] = value
        return out
    return json.loads(data, object_pairs_hook=fields)


def regular(path, limit):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or not 0 < s.st_size <= limit:
        raise ValueError('candidate-input-kind')
    b = path.read_bytes()
    if len(b) != s.st_size:
        raise ValueError('candidate-input-changed')
    return b


def bind(source_path, env):
    if env['GITHUB_EVENT_NAME'] != 'release' or env['GITHUB_REPOSITORY'] != 'footprintjs/footprintjs-datapipeline':
        raise ValueError('release-event')
    event = closed_json(regular(Path(env['GITHUB_EVENT_PATH']), 1024 * 1024))
    release = event['release']
    if event['action'] != 'published' or release['draft'] is not False or release['prerelease'] is not False:
        raise ValueError('release-state')
    sha, tag = env['GITHUB_SHA'], env['RELEASE_TAG']
    run, attempt = env['GITHUB_RUN_ID'], env['GITHUB_RUN_ATTEMPT']
    if not re.fullmatch(r'[a-f0-9]{40}', sha) or any(not re.fullmatch(r'[1-9][0-9]{0,19}', x) for x in (run, attempt)):
        raise ValueError('release-origin')
    source = closed_json(regular(Path(source_path), 1024 * 1024))
    version = source['version']
    if source['name'] != 'footprintjs-datapipeline' or not isinstance(version, str) or len(version) > 64 or not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError('source-identity')
    if tag != 'v' + version or release['tag_name'] != tag or env['GITHUB_REF'] != 'refs/tags/' + tag:
        raise ValueError('release-tag')
    incoming = Path(env['INCOMING'])
    if incoming.is_symlink() or set(x.name for x in incoming.iterdir()) != {'candidate.tgz', 'candidate.json'}:
        raise ValueError('candidate-intake')
    metadata = closed_json(regular(incoming / 'candidate.json', 8192))
    data = regular(incoming / 'candidate.tgz', 64 * 1024 * 1024)
    digest = hashlib.sha256(data).hexdigest()
    if metadata != {'name': source['name'], 'version': version, 'bytes': len(data), 'sha256': digest}:
        raise ValueError('tested-candidate-drift')
    # The existing packed check already proves complete payload bytes/modes. This
    # narrower stdlib DATA read joins its manifest to the immutable source blob.
    manifests = []
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for index, item in enumerate(archive):
            if index >= 20_000:
                raise ValueError('archive-member-limit')
            if item.name == 'package/package.json':
                if not item.isfile() or not 0 < item.size <= 1024 * 1024:
                    raise ValueError('archive-manifest-kind')
                stream = archive.extractfile(item)
                if stream is None:
                    raise ValueError('archive-manifest-missing')
                with stream:
                    payload = stream.read(1024 * 1024 + 1)
                if len(payload) != item.size:
                    raise ValueError('archive-manifest-size')
                manifests.append(closed_json(payload))
    if manifests != [source]:
        raise ValueError('source-manifest-drift')
    output = Path(env['SEALED'])
    output.mkdir()  # Fresh path, never mix a previous attempt.
    (output / 'package.tgz').write_bytes(data)
    integrity = 'sha512-' + base64.b64encode(hashlib.sha512(data).digest()).decode('ascii')
    return {'sha256': digest, 'integrity': integrity, 'version': version,
            'source': sha, 'tag': tag, 'run': run, 'attempt': attempt}


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('publish-candidate.py SOURCE_MANIFEST')
    result = bind(sys.argv[1], os.environ)
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf8') as output:
        for name, value in result.items():
            output.write(name + '=' + value + '\n')
