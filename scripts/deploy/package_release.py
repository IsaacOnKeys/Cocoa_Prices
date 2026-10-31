"""Build/verify a Git-pinned application candidate; never contact or change GCP."""

import argparse
import hashlib
import json
import re
import runpy
import subprocess
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[2]
PATHS = ('functions', 'schemas', 'airflow', 'pipelines', 'sql', 'scripts/deploy',
         'scripts/operations', 'scripts/diagnostics', 'infra/terraform', 'requirements-ops.txt',
         'docs/operations', '.dockerignore')
SUFFIXES = {'.py', '.sql', '.avsc', '.tf', '.txt', '.yaml', '.md', '.sh'}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def relative(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or '\\' in name or ':' in name or '\0' in name or not path.parts:
        raise ValueError('Unsafe release path')
    return path


def allowed(name):
    path = relative(name)
    return (path.suffix in SUFFIXES or path.name in {'Dockerfile', '.dockerignore'}) and not any(
        part.startswith('.') and part != '.dockerignore' for part in path.parts)


def verify(folder):
    folder = Path(folder)
    manifest = json.loads((folder / 'manifest.json').read_text())
    expected = manifest['files']
    for name, checksum in expected.items():
        path = folder.joinpath(*relative(name).parts)
        if path.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent != folder.parent):
            raise ValueError('Release links are not allowed')
        if not re.fullmatch('[a-f0-9]{64}', checksum) or digest(path.read_bytes()) != checksum:
            raise ValueError('Release checksum mismatch: ' + name)
    actual = {path.relative_to(folder).as_posix() for path in folder.rglob('*') if path.is_file()}
    if actual != set(expected) | {'manifest.json'}:
        raise ValueError('Unexpected release files')
    return manifest


def build(output, revision, namespace):
    output = Path(output)
    if not re.fullmatch(r'application_validation_20[0-9]{6}_[a-f0-9]{8}', namespace):
        raise ValueError('An explicit isolated application_validation_YYYYMMDD_8hex namespace is required')
    if output.exists():
        raise ValueError('Release output must not exist')
    commit = subprocess.check_output(['git', 'rev-parse', '--verify', '--end-of-options', revision + '^{commit}'], cwd=ROOT, text=True).strip()
    listing = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', commit, '--', *PATHS], cwd=ROOT, text=True)
    names = [name for name in listing.splitlines() if allowed(name)]
    archive = subprocess.check_output(['git', 'archive', '--format=zip', commit, '--', *names], cwd=ROOT)
    output.mkdir(parents=True)
    (output / 'source.zip').write_bytes(archive)
    with TemporaryDirectory(prefix='.cocoa-release-', dir=output.parent) as temporary:
        checkout = Path(temporary)
        with ZipFile(output / 'source.zip') as bundle:
            for item in bundle.infolist():
                relative(item.filename)
                if item.is_dir():
                    continue
                if (item.external_attr >> 16) & 0o170000 == 0o120000:
                    raise ValueError('Git release links are not allowed')
                target = checkout.joinpath(*PurePosixPath(item.filename).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(bundle.read(item))
        packager = runpy.run_path(str(checkout / 'scripts/deploy/package_functions.py'))
        for feed in packager['FUNCTIONS']:
            packager['package_function'](feed, repo_root=checkout, output_dir=output / 'functions')
        renderer = runpy.run_path(str(checkout / 'scripts/deploy/render_delivery_sql.py'))
        identifiers = {'delivery': namespace, 'curated': namespace + '_serving', 'staging': namespace + '_staging',
                       'models': namespace + '_models', 'studio': namespace + '_studio'}
        sql = output / 'isolated-sql'
        sql.mkdir()
        for kind in renderer['FILES']:
            if kind not in renderer['FRAGMENTS']:
                (sql / (kind + '.sql')).write_text(renderer['render'](kind, **identifiers), encoding='utf-8', newline='\n')
    files = {path.relative_to(output).as_posix(): digest(path.read_bytes())
             for path in sorted(output.rglob('*')) if path.is_file()}
    manifest = {'version': 1, 'git_revision': commit, 'builder_sha256': digest(Path(__file__).read_bytes()),
                'status': 'candidate_not_deployed', 'python': '3.11', 'project': 'cocoa-prices-430315',
                'region': 'europe-west3', 'isolated_namespaces': identifiers,
                'source_paths': names, 'files': files}
    (output / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n', encoding='utf-8')
    return verify(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--revision', default='HEAD')
    parser.add_argument('--namespace')
    parser.add_argument('--verify', type=Path)
    args = parser.parse_args()
    if args.verify:
        if args.output or args.namespace or args.revision != 'HEAD':
            parser.error('--verify cannot be combined with build arguments')
        manifest = verify(args.verify)
        folder = args.verify
    else:
        if not args.output or not args.namespace:
            parser.error('Building requires --output and --namespace')
        manifest = build(args.output, args.revision, args.namespace)
        folder = args.output
    print(json.dumps({'git_revision': manifest['git_revision'], 'files_verified': len(manifest['files']),
                      'manifest_sha256': digest((folder / 'manifest.json').read_bytes()),
                      'status': manifest['status']}))


if __name__ == '__main__':
    main()
