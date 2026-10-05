"""Build portable 120000-129999 icon releases; no game writes or network operations."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import shutil
import tempfile
import zipfile
import urllib.request
import time


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def package_release(package, output, text_version=None):
    package, output = Path(package).resolve(), Path(output).resolve()
    manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    for field in ('koreanVersion', 'globalVersion'):
        if not re.fullmatch(r'\d{4}\.\d{2}\.\d{2}\.\d{4}\.\d{4}', manifest[field]):
            raise ValueError('Invalid resource client version')
    entries = manifest['entries']
    if manifest.get('kind') != 'icons-12' or not re.fullmatch(r'[0-9a-f]{64}', manifest.get('globalExecutableSha256', '')):
        raise ValueError('Expected icons-12 package with executable fingerprint')
    if not entries:
        raise ValueError('Empty resource package')
    files, targets = {}, set()
    for entry in entries:
        target = entry['target']
        match = re.fullmatch(r'ui/icon/(12\d)000/(?:(en|ja|de|fr)/)?\1\d{3}(_hr1)?\.tex', target)
        if entry['status'] != 'prepared' or not match or target in targets:
            raise ValueError('Invalid/duplicate resource target: ' + target)
        source = target.replace('/' + match[2] + '/', '/ko/') if match[2] else target
        if entry['source'] != source:
            raise ValueError('Korean source mismatch')
        targets.add(target)
        relative, digest = entry['cacheFile'], entry['sha256']
        if not re.fullmatch(r'[0-9a-f]{64}', digest) or relative != 'assets/' + digest + '.tex':
            raise ValueError('Invalid content-addressed resource path')
        path = package / relative
        if not path.resolve().is_relative_to(package):
            raise ValueError('Resource outside package')
        if relative not in files:
            if sha256(path) != digest:
                raise ValueError('Resource checksum mismatch: ' + relative)
            with path.open('rb') as stream:
                header = stream.read(80)
            if len(header) != 80 or header[14] & 63 != 1:
                raise ValueError('Unsupported icon mip count')
            files[relative] = path
    output.mkdir(parents=True, exist_ok=True)
    channel = os.environ.get('RESOURCE_RELEASE_CHANNEL', 'stable')
    if channel not in ('stable', 'staging'):
        raise ValueError('RESOURCE_RELEASE_CHANNEL must be stable or staging')
    text_version = text_version or os.environ.get('RESOURCE_TEXT_VERSION') or manifest['koreanVersion']
    if not re.fullmatch(r'\d+(?:\.\d+){4,6}', text_version):
        raise ValueError('Invalid shared text/resource version')
    temporary = output / 'resources.zip.tmp'
    # Include only the portable runtime manifest; never publish local settings or credentials.
    portable = {field: manifest[field] for field in ('schemaVersion', 'kind', 'koreanVersion', 'globalVersion', 'globalExecutableSha256')}
    portable['entries'] = [{key: entry[key] for key in ('target', 'source', 'status', 'cacheFile', 'sha256')}
                           for entry in entries]
    with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as bundle:
        metadata = zipfile.ZipInfo('manifest.json')
        metadata.compress_type = zipfile.ZIP_DEFLATED
        bundle.writestr(metadata, json.dumps(portable, ensure_ascii=False, indent=2))
        for relative, path in sorted(files.items()):
            item = zipfile.ZipInfo(relative)
            item.compress_type = zipfile.ZIP_DEFLATED
            with path.open('rb') as source, bundle.open(item, 'w') as destination:
                shutil.copyfileobj(source, destination, 1024 * 1024)
    archive_hash = sha256(temporary)
    archive = output / 'resources.zip'
    os.replace(temporary, archive)
    release = output / 'release.json'
    release.write_text(json.dumps({
        'schemaVersion': 1, 'kind': 'icons-12', 'channel': channel, 'textVersion': text_version,
        'koreanVersion': manifest['koreanVersion'], 'globalVersion': manifest['globalVersion'],
        'globalExecutableSha256': manifest['globalExecutableSha256'],
        'archiveKey': archive.name, 'sha256': archive_hash,
        'bytes': archive.stat().st_size, 'mappings': len(entries), 'uniqueFiles': len(files),
        'unpackedBytes': sum(path.stat().st_size for path in files.values()),
        'languages': ['en', 'ja', 'de', 'fr'], 'includesShared': True,
    }, indent=2), encoding='utf-8')
    return archive, release


def publish_release(uploader, archive, release, verify_shared_version=False, data_path=None):
    # The CSV pipeline publishes its shared version.txt only after these objects exist.
    data = json.loads(Path(release).read_text(encoding='utf-8'))
    if verify_shared_version:
        key = 'version.txt' if data['channel'] == 'stable' else 'dev-version.txt'
        public_url = os.environ.get('RESOURCE_PUBLIC_BASE_URL')
        if public_url:
            url = public_url.rstrip('/') + '/' + key + '?t=' + str(time.time_ns())
            with urllib.request.urlopen(url, timeout=30) as response:
                raw = response.read(4096).decode('utf-8').strip()
        else:
            response = uploader.s3.get_object(Bucket=uploader.bucket_name, Key=key)
            raw = response['Body'].read(4096).decode('utf-8').strip()
        lines = [line for line in raw.splitlines() if data['channel'] in line.lower()]
        match = re.search(r'[\d.]+', lines[0] if lines else raw)
        if not match or match[0].strip('.') != data['textVersion']:
            raise ValueError('Standalone image release must match the published shared CSV version')
    if Path(archive).name != 'resources.zip' or data['archiveKey'] != 'resources.zip':
        raise ValueError('Only the root resources.zip publication layout is supported')
    # Preserve CSV presets in the existing root data.json; no separate S3 resource catalog.
    if data_path is None:
        public_url = os.environ.get('RESOURCE_PUBLIC_BASE_URL')
        if public_url:
            with urllib.request.urlopen(public_url.rstrip('/') + '/data.json?t=' + str(time.time_ns()), timeout=30) as response:
                combined = json.load(response)
        else:
            response = uploader.s3.get_object(Bucket=uploader.bucket_name, Key='data.json')
            combined = json.loads(response['Body'].read())
        data_path = Path(release).parent / 'data.json'
    else:
        data_path = Path(data_path)
        combined = json.loads(data_path.read_text(encoding='utf-8'))
    combined['imageResources'] = data
    data_path.write_text(json.dumps(combined, ensure_ascii=False, indent=2), encoding='utf-8')
    if not uploader.upload_files([str(archive)]):
        raise RuntimeError('Resource archive upload failed; version not published')
    if not uploader.upload_files([str(data_path)]):
        raise RuntimeError('Shared data.json upload failed; version not published')


def build_release(output, expected_korean_version=None, text_version=None):
    project = Path(os.environ['RESOURCE_SWAP_PROJECT']).resolve()
    korean, global_client = os.environ['RESOURCE_KR_CLIENT'], os.environ['RESOURCE_GLOBAL_CLIENT']
    if not (project / 'ResourceSwap.csproj').is_file():
        raise ValueError('RESOURCE_SWAP_PROJECT must point to ffxiv-resource-swap')
    output = Path(output).resolve()
    # Every release is extracted afresh; existing development settings/cache are untouched.
    with tempfile.TemporaryDirectory(prefix='icon-release-') as directory:
        work = Path(directory)
        config, package = work / 'swap.json', work / 'package'
        command = ['dotnet', 'run', '--project', str(project / 'ResourceSwap.csproj'), '--']
        for args in [('init', korean, global_client, str(config)),
                     ('export-icons-12', str(config), str(package))]:
            subprocess.run(command + list(args), cwd=project, check=True, stdout=subprocess.DEVNULL)
        manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
        if expected_korean_version and manifest['koreanVersion'] != expected_korean_version:
            raise ValueError('CSV release and Korean client versions differ')
        return package_release(package, output, text_version)
