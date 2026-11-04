"""Explicit preparation of native rescues; never submits or removes jobs."""
from copy import deepcopy
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

from full_mc.io import atomic_json
from full_mc.local_paths import record_path
from full_mc.storage.transfer import TransferClient
from .recovery_records import reconstruct

SUFFIXES = ('.nodes.log', '.lock', '.dagman.out', '.dagman.log', '.condor.sub',
            '.lib.out', '.lib.err', '.metrics')


def read(path):
    return json.loads(Path(path).read_text())


def events(path):
    if not Path(path).exists():
        return []
    records = []
    for record in Path(path).read_text().split('\n...'):
        match = re.search(r'(?m)^(\d{3}) \((\d+)\.(\d+)\.(\d+)\)', record)
        if match:
            name = re.search(r'DAG Node:\s*(\S+)', record)
            exitcode = re.search(r'Normal termination \(return value (-?\d+)\)', record)
            records.append(dict(code=match[1], cluster=int(match[2]),
                                node=name[1] if name else None,
                                exit=int(exitcode[1]) if exitcode else None))
    return records


def failed_root(path, cluster):
    history = [e for e in events(path) if e['cluster'] == int(cluster)]
    if (not history or history[0]['code'] != '000' or history[-1]['code'] != '005'
            or history[-1]['exit'] in (None, 0)):
        raise ValueError('no exact failed root termination: ' + str(path))
    return history[-1]


def graph(path):
    nodes, dependencies = {}, []
    for line in Path(path).read_text().splitlines():
        fields = shlex.split(line, comments=True)
        if not fields:
            continue
        if fields[0] == 'JOB':
            nodes[fields[1]] = None
        elif fields[:2] == ['SUBDAG', 'EXTERNAL']:
            nodes[fields[2]] = Path(fields[3])
        elif fields[0] == 'PARENT':
            at = fields.index('CHILD')
            dependencies.extend((p, c) for p in fields[1:at] for c in fields[at+1:])
    return nodes, dependencies


def quiet(root, schedd, clusters, runner):
    ids = ','.join(str(int(x)) for x in sorted(clusters))
    constraint = f'member(ClusterId,{{{ids}}}) || member(DAGManJobId,{{{ids}}}) || Iwd == {json.dumps(str(root))}'
    result = runner(['condor_q', '-name', schedd, '-constraint', constraint,
                     '-autoformat', 'ClusterId', 'ProcId', 'JobStatus'],
                    check=False, capture_output=True, text=True, timeout=60)
    if result.returncode or result.stdout.strip():
        raise RuntimeError('queue query failed or related jobs still present: '
                           + (result.stdout + result.stderr)[-2000:])


def envelope(task, outcome):
    if any(outcome.get(k) != task.get(k) for k in ('task_id', 'stage', 'runtime_release')) or outcome.get('status') != 'ok':
        raise ValueError('result identity/status mismatch: ' + task['task_id'])


def verify_record(task, outcome, client):
    envelope(task, outcome)
    if task['stage'] != 'plan':
        expected = {(r['artifact_id'], r['url'], r['kind']) for r in task['outputs']}
        actual = {(r['artifact_id'], r['url'], r['kind']) for r in outcome['outputs']}
        if actual != expected:
            raise ValueError('result output membership mismatch')
    for ref in outcome.get('outputs', []) + outcome.get('originals', []):
        if client.size(ref['url']) != ref['size_bytes'] or ref['size_bytes'] <= 0:
            raise ValueError('published size mismatch: ' + ref['url'])
        if 'sidecar_url' in ref:
            record = client.read_json(ref['sidecar_url'])
            if (record.get('task_id') != task['task_id'] and task['stage'] != 'plan') or any(
                    record.get(k) != ref.get(k) for k in ('artifact_id', 'url', 'size_bytes', 'events', 'kind') if k in ref):
                raise ValueError('published sidecar mismatch')
            if not record.get('complete') or record.get('runtime_release') != task['runtime_release']:
                raise ValueError('incomplete or wrong-runtime sidecar')
    if task['stage'] == 'reclaim':
        for entry in outcome['reclaimed']:
            for obj in entry['objects']:
                try:
                    client.size(obj['url'])
                except FileNotFoundError:
                    continue
                raise ValueError('reclaimed object is still present')


def prepare_rescue(run_dir, *, schedd, root_cluster_id, apply=False,
                   runner=subprocess.run, client=None):
    root = Path(run_dir).resolve()
    if not str(root_cluster_id).isdigit():
        raise ValueError('root cluster ID must be numeric')
    client = client or TransferClient()
    active = root / 'recovery' / 'current.json'
    if active.exists():
        previous = read(active)
        if previous['root_cluster_id'] != str(root_cluster_id):
            # A different, subsequently failed root starts a new recovery attempt.
            if previous['state'] != 'prepared':
                raise RuntimeError('unfinished recovery for a different root')
        elif previous['state'] == 'prepared':
            return previous
        elif not apply:
            return previous
    spec, state = read(root / 'run.json'), read(root / 'state.json')
    submission = read(root / 'submission.json')
    if submission.get('scheduler') != schedd:
        raise ValueError('schedd differs from recorded submission')
    if submission.get('root_cluster_id') not in (None, str(root_cluster_id), int(root_cluster_id)):
        raise ValueError('root differs from recorded submission')
    if submission.get('state') in {'unknown', 'submitting'} and not (
            submission.get('attempts') and all(
                'already exists' in a.get('submit_output', '')
                and a.get('root_cluster_id') is None for a in submission['attempts'])):
        raise RuntimeError('unresolved ambiguous submission')
    base = root / 'recovery' / (str(root_cluster_id) + '-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    journal = dict(state='preparing', run_dir=str(root), root_cluster_id=str(root_cluster_id),
                   schedd=schedd, directory=str(base), groups=[], accepted=[], blocked=[], moved=[])
    resuming = active.exists() and read(active)['root_cluster_id'] == str(root_cluster_id)
    if resuming:
        journal = read(active)
        base = Path(journal['directory'])
    prior_groups = {g['dag']: g for g in journal['groups']}
    journal['groups'] = []
    journal['blocked'] = []
    root_log = root / 'run.dag.dagman.log'
    if not root_log.exists():
        root_log = base / 'control-files/run.dag.dagman.log'
    failed_root(root_log, root_cluster_id)
    root_nodes, root_edges = graph(root / 'run.dag')
    dags = [root / 'run.dag'] + [p if p.is_absolute() else root / p
                               for p in root_nodes.values() if p is not None]
    clusters = {int(root_cluster_id)}
    submitted, successful = set(), set()
    tasks, edges = {}, list(root_edges)
    for dag in dags:
        if not dag.exists():
            continue  # Children not yet rendered remain under their normal PRE.
        log = Path(str(dag) + '.nodes.log')
        if not log.exists():
            log = base / 'control-files' / log.relative_to(root)
        mapping = {}
        for e in events(log):
            if e['cluster'] > 0:
                clusters.add(e['cluster'])
            if e['node'] and e['code'] == '000':
                mapping[e['cluster']] = e['node']
                submitted.add(e['node'])
            if e['code'] == '005' and e['exit'] == 0 and e['cluster'] in mapping:
                successful.add(mapping[e['cluster']])
        nodes, deps = graph(dag)
        if dag != root / 'run.dag':
            edges.extend(deps)
        for identity, child in nodes.items():
            if child is None and identity != 'FINALIZE':
                path = record_path(root, 'tasks', identity)
                if path.exists():
                    tasks[identity] = read(path)
    journal["clusters"] = sorted(clusters)
    quiet(root, schedd, clusters, runner)
    state = read(root / 'state.json')
    lock = None
    if apply:
        (root / 'recovery').mkdir(exist_ok=True)
        lock = (root / 'recovery/prepare.lock').open('a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if active.exists() and read(active)['state'] == 'prepared' and not resuming:
            # Re-read after locking; another preparer may have finished.
            if read(active)['root_cluster_id'] == str(root_cluster_id):
                lock.close()
                return read(active)
        base.mkdir(exist_ok=resuming)
        atomic_json(active, journal)
    try:
        def save():
            if apply:
                atomic_json(base / 'manifest.json', journal)
                atomic_json(active, journal)

        accepted_refs = {}
        done = set(state['accepted'])
        for identity in done:
            receipt = read(record_path(root, 'ledger/accepted', identity))
            if receipt.get('task_id') != identity or receipt.get('runtime_release') != spec['runtime_release'] or receipt.get('status') != 'ok' or receipt.get('stage') != state['accepted'][identity]['stage']:
                raise ValueError('accepted receipt mismatch: ' + identity)
            for ref in receipt.get('outputs', []) + receipt.get('originals', []):
                accepted_refs[ref['artifact_id']] = ref
        for identity in root_nodes:
            if identity.startswith('SOURCE_') and state['planner']['sources'].get(str(int(identity[7:]))) in {'completed', 'skipped'}:
                done.add(identity)
            if identity.startswith('SHARD_') and state['allocations'].get(identity, {}).get('completed'):
                done.add(identity)
        consumed_artifacts = None
        remaining = {i for i in tasks if i not in done}
        with tempfile.TemporaryDirectory(dir='/tmp/chiw', prefix='mcprod-rescue-') as scratch:
            while remaining:
                ready = sorted(i for i in remaining if not any(c == i and p not in done for p, c in edges))
                if not ready:
                    break
                progressed = False
                for identity in ready:
                    task = tasks[identity]
                    path = record_path(root, 'results', identity)
                    receipt_path = record_path(root, 'ledger/accepted', identity)
                    if not (path.exists() or receipt_path.exists() or identity in submitted):
                        continue
                    try:
                        candidates = [read(p) for p in (receipt_path, path) if p.exists()]
                        outcome = None
                        for candidate in candidates:
                            try:
                                verify_record(task, candidate, client)
                            except (ValueError, KeyError, FileNotFoundError):
                                continue
                            outcome = candidate
                            break
                        if outcome is None:
                            outcome, proof = reconstruct(task, accepted_refs, Path(scratch) / identity, client)
                        else:
                            proof = ['existing local result/receipt']
                        if receipt_path.exists():
                            old = read(receipt_path)
                            def comparable(v):
                                v = deepcopy(v)
                                if task['stage'] == 'archive':
                                    for ref in v['outputs']:
                                        ref.pop('local_path', None)
                                return v
                            if comparable(old) != comparable(outcome):
                                # A recovery retry may have published a new file at the
                                # same URL. Preserve the orphan, then let the deployed
                                # controller validate and publish the replacement receipt.
                                current = read(root / 'state.json')
                                if (identity in current['accepted']
                                        or identity in current['staging'].get('settled_publications', {})
                                        or identity in current['staging'].get('settled_reclaims', {})):
                                    raise ValueError('replacement receipt already committed: ' + identity)
                                if consumed_artifacts is None:
                                    consumed_artifacts = set()
                                    for consumer in current['accepted']:
                                        frozen = tasks.get(consumer)
                                        if frozen is None:
                                            frozen = read(record_path(root, 'tasks', consumer))
                                        consumed_artifacts.update(ref['artifact_id'] for ref in frozen['inputs'])
                                old_ids = {ref['artifact_id'] for ref in old.get('outputs', []) + old.get('originals', [])}
                                if old_ids & consumed_artifacts:
                                    raise ValueError('replacement has an accepted downstream consumer: ' + identity)
                                proof.append('recovery replacement of uncommitted receipt')
                                if apply:
                                    backup = base / 'superseded-receipts' / receipt_path.relative_to(root / 'ledger/accepted')
                                    backup.parent.mkdir(parents=True, exist_ok=True)
                                    replacements = journal.setdefault('replaced_receipts', {})
                                    replacements[identity] = dict(backup=str(backup),
                                        old_outputs=old.get('outputs', []), new_outputs=outcome.get('outputs', []))
                                    save()
                                    if backup.exists():
                                        raise ValueError('replacement backup already exists: ' + identity)
                                    receipt_path.rename(backup)
                            else:
                                outcome = old
                        if apply:
                            recovered = base / 'recovered-results' / (identity + '.json')
                            recovered.parent.mkdir(exist_ok=True)
                            atomic_json(recovered, outcome)
                            if path.exists():
                                backup = base / 'original-results' / path.relative_to(root / 'results')
                                backup.parent.mkdir(parents=True, exist_ok=True)
                                if not backup.exists():
                                    backup.write_bytes(path.read_bytes())
                            atomic_json(path, outcome)
                            result = runner([str(root / 'controller.sh'), 'accept-result',
                                             '--task-id', identity, '--return-code', '0'],
                                            check=False, capture_output=True, text=True, cwd=str(root))
                            if result.returncode:
                                raise RuntimeError('controller acceptance failed/deferred: ' + identity
                                                   + ': ' + result.stderr[-1000:])
                            updated = read(root / 'state.json')
                            if identity not in updated['accepted']:
                                raise RuntimeError('controller did not publish accepted marker')
                        journal['accepted'].append(dict(task_id=identity, proof=proof))
                        save()
                        done.add(identity)
                        remaining.remove(identity)
                        progressed = True
                        for ref in outcome.get('outputs', []) + outcome.get('originals', []):
                            accepted_refs[ref['artifact_id']] = ref
                    except FileNotFoundError as exc:
                        if identity in successful or receipt_path.exists() or any(c.get('status') == 'ok' for c in candidates):
                            journal['blocked'].append(dict(task_id=identity, reason=str(exc)))
                    except (ValueError, KeyError) as exc:
                        if task['stage'] == 'reclaim' and 'still needs execution' in str(exc) and identity not in successful:
                            continue
                        journal['blocked'].append(dict(task_id=identity, reason=str(exc)))
                if not progressed:
                    break
        if apply:
            state = read(root / 'state.json')
        for dag in dags:
            if not dag.exists():
                continue
            if dag != root / 'run.dag' and dag.stem in journal['groups'][0]['done']:
                continue
            nodes, _ = graph(dag)
            selected = []
            if dag == root / 'run.dag':
                for identity in nodes:
                    if identity.startswith('SOURCE_'):
                        complete = state['planner']['sources'].get(str(int(identity[7:]))) in {'completed', 'skipped'}
                    elif identity.startswith('SHARD_'):
                        complete = state['allocations'].get(identity, {}).get('completed', False)
                    elif identity.startswith('CLEANUP_'):
                        complete = state['cleanup_claims'].get(identity, {}).get('completed', False)
                    else:
                        complete = False  # FINALIZE still repairs its completion marker.
                    if complete:
                        selected.append(identity)
            else:
                selected = sorted(set(nodes) & done)
            numbers = [int(p.name.rsplit('rescue', 1)[1]) for p in dag.parent.glob(dag.name + '.rescue[0-9][0-9][0-9]')]
            number = max(numbers, default=0) + 1
            if number > 999:
                raise ValueError('rescue numbering exhausted')
            rescue = Path(prior_groups[str(dag)]['rescue']) if str(dag) in prior_groups else Path(str(dag) + f'.rescue{number:03d}')
            retries = []
            if numbers:
                for line in Path(str(dag) + f'.rescue{max(numbers):03d}').read_text().splitlines():
                    if line.startswith('RETRY ') and line.split()[1] not in selected:
                        retries.append(line)
            journal['groups'].append(dict(dag=str(dag), rescue=str(rescue), done=selected,
                                           remaining=sorted(set(nodes) - set(selected))))
            if apply and not journal['blocked']:
                # Persist the chosen filenames before any control files are moved.
                save()
                content = '# Prepared by mcprod recovery prepare-rescue\n'
                content += ''.join('DONE ' + i + '\n' for i in sorted(selected))
                content += ''.join(line + '\n' for line in retries)
                if rescue.exists():
                    if rescue.read_text() != content:
                        raise RuntimeError('prepared rescue changed: ' + str(rescue))
                else:
                    with tempfile.NamedTemporaryFile('w', dir=rescue.parent, delete=False) as stream:
                        temporary = Path(stream.name)
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                    try:
                        os.link(temporary, rescue)
                    finally:
                        temporary.unlink()
        if journal['blocked']:
            journal['state'] = 'blocked'
            save()
            return journal
        if apply:
            quiet(root, schedd, clusters, runner)
            for group in journal['groups']:
                # Completed root children will not be restarted.
                if group['dag'] != str(root / 'run.dag') and Path(group['dag']).stem in journal['groups'][0]['done']:
                    continue
                for suffix in SUFFIXES:
                    source = Path(group['dag'] + suffix)
                    destination = base / 'control-files' / source.relative_to(root)
                    if source.exists():
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        if destination.exists():
                            raise RuntimeError('control backup already exists: ' + str(destination))
                        source.rename(destination)
                    if destination.exists() and str(source) not in journal['moved']:
                        journal['moved'].append(str(source))
                        save()
            imported = dict(number=len(submission['attempts']) + 1, kind='reconciled-existing-root',
                            state='terminal', scheduler=schedd, root_cluster_id=str(root_cluster_id),
                            reconciliation_evidence=str(base / 'control-files/run.dag.dagman.log'))
            if submission['attempts'] and submission['attempts'][-1].get('kind') == 'reconciled-existing-root' and submission['attempts'][-1].get('root_cluster_id') == str(root_cluster_id):
                imported = submission['attempts'][-1]
            else:
                submission['attempts'].append(imported)
            submission.update(state='terminal', scheduler=schedd, root_cluster_id=str(root_cluster_id),
                              current_attempt=imported['number'], reconciliation_required=False)
            atomic_json(root / 'submission.json', submission)
            journal['state'] = 'prepared'
            save()
        else:
            journal['state'] = 'preview'
        return journal
    except Exception as exc:
        if apply:
            journal['error'] = str(exc)
            atomic_json(active, journal)
            atomic_json(base / 'manifest.json', journal)
        raise
    finally:
        if lock:
            lock.close()


def prepared_failure(root, cluster):
    path = Path(root) / 'recovery/current.json'
    if not path.exists():
        return None
    record = read(path)
    if record['root_cluster_id'] != str(cluster):
        return None
    if record['state'] != 'prepared':
        raise RuntimeError('recovery preparation is incomplete')
    failed_root(Path(record['directory']) / 'control-files/run.dag.dagman.log', cluster)
    return dict(state='terminal', terminal=True, successful=False, query='prepared-rescue-evidence')
