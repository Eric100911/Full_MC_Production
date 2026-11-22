#!/usr/bin/env python3
"""Migrate a stopped, rescue-prepared shard run; never submits jobs."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from full_mc.application.dagman_recovery import quiet, read
from full_mc.io import atomic_json


def convert(text):
    lines = []
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith('PRE_SKIP '):
            continue
        if (line.lstrip().startswith('SCRIPT ') and ' POST ' in line
                and any(' ' + phase + ' ' in line for phase in
                        ('accept-result', 'complete-source', 'complete-shard'))
                and '--pre-return-code' not in line):
            line = line.rstrip('\r\n') + ' --pre-return-code $PRE_SCRIPT_RETURN\n'
        lines.append(line)
    return ''.join(lines)


def execute(command, runner):
    result = runner(command, check=False, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return json.loads(result.stdout)


def migrate(run_dir, build_dir, apply=False, runner=subprocess.run):
    root, build = Path(run_dir).resolve(), Path(build_dir).resolve()
    prepared = read(root / 'recovery/current.json')
    submission = read(root / 'submission.json')
    if (prepared['state'] != 'prepared'
            or str(submission['root_cluster_id']) != prepared['root_cluster_id']):
        raise ValueError('requires rescue prepared for the current stopped root')
    directory = Path(prepared['directory']) / 'post-completion'
    manifest = directory / 'migration.json'
    if manifest.exists() and read(manifest)['state'] == 'complete':
        previous = read(manifest)
        if previous['build_dir'] != str(build):
            raise ValueError('already migrated with another build')
        return previous
    entry = build / 'controller/core/mcprod'
    descriptor = read(build / 'controller.json')
    if descriptor['source_dirty']:
        raise ValueError('requires a clean controller build')
    for file, needle in [('src/full_mc/application/shards.py', 'def _post_pre_status('),
                         ('src/full_mc/planning/shards.py', '"DAGMAN_ALWAYS_RUN_POST": True'),
                         ('src/full_mc/cli.py', '--pre-return-code')]:
        if needle not in (build / 'controller/core' / file).read_text():
            raise ValueError('controller lacks POST protocol: ' + file)
    quiet(root, prepared['schedd'], set(prepared['clusters']), runner)
    active = execute([str(entry), 'controller', 'status', '--run-dir', str(root)], runner)
    changes = {}
    for group in prepared['groups']:
        path = Path(group['dag'])
        before = path.read_text()
        after = convert(before)
        if before != after:
            changes[path] = after
    config = root / 'dagman.config'
    text = config.read_text()
    lines = text.splitlines(keepends=True)
    found = [i for i, line in enumerate(lines) if line.strip().startswith('DAGMAN_ALWAYS_RUN_POST')]
    if len(found) != 1:
        raise ValueError('expected exactly one ALWAYS_RUN_POST setting')
    lines[found[0]] = 'DAGMAN_ALWAYS_RUN_POST = TRUE\n'
    changes[config] = ''.join(lines)
    record = dict(state='preview', root_cluster_id=prepared['root_cluster_id'],
                  build_dir=str(build), build_id=descriptor['build_id'],
                  controller_revision=descriptor['git_revision'],
                  files=[str(p) for p in changes], worker_runtime_unchanged=True)
    if not apply:
        return record
    directory.mkdir(exist_ok=True)
    with (directory / 'migration.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record['state'] = 'applying'
        atomic_json(manifest, record)
        for path, content in changes.items():
            backup = directory / 'original' / path.relative_to(root)
            backup.parent.mkdir(parents=True, exist_ok=True)
            if not backup.exists():
                backup.write_bytes(path.read_bytes())
            elif path.read_bytes() not in (backup.read_bytes(), content.encode()):
                raise ValueError('file changed outside migration: ' + str(path))
            mode = path.stat().st_mode & 0o777
            with tempfile.NamedTemporaryFile('w', dir=path.parent, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(content); stream.flush(); os.fsync(stream.fileno())
            temporary.chmod(mode)
            os.replace(temporary, path)
        command = [str(entry), 'controller', 'activate', '--run-dir', str(root), '--build-dir', str(build)]
        for key in ('control_events', 'admission_hints', 'producer_index'):
            if active.get(key):
                command.append('--' + key.replace('_', '-'))
        if active.get('plan_accept_post_seconds') is not None:
            command += ['--plan-accept-post-seconds', str(active['plan_accept_post_seconds'])]
        record['activation'] = execute(command, runner)
        record['state'] = 'complete'
        atomic_json(manifest, record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True)
    parser.add_argument('--build-dir', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(migrate(args.run_dir, args.build_dir, args.apply), indent=2))


if __name__ == '__main__':
    main()
