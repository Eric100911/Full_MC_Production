"""Recovery regressions using isolated records, storage and controller doubles."""
import gzip
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from full_mc.application.dagman_recovery import events, prepare_rescue, prepared_failure
from full_mc.application.recovery_records import reconstruct
from full_mc.io import atomic_json


def response(stdout='', code=0):
    return SimpleNamespace(returncode=code, stdout=stdout, stderr='')


class Store:
    def __init__(self, objects):
        self.objects = objects

    def size(self, url):
        if url not in self.objects:
            raise FileNotFoundError(url)
        return len(self.objects[url])

    def read_json(self, url):
        self.size(url)
        return json.loads(self.objects[url])

    def copy_in(self, url, path):
        self.size(url)
        Path(path).write_bytes(self.objects[url])
        return Path(path)


def task(stage, identity='A'):
    return dict(task_id=identity, stage=stage, runtime_release='r1', parameters={},
                inputs=[], outputs=[dict(artifact_id='out', url='root://host/out', kind='lhe')])


class Records(unittest.TestCase):
    def test_sidecar_stages_and_runtime_mismatch(self):
        for stage in ('lhe', 'mix', 'miniaod'):
            t = task(stage)
            t['parameters']['staging_claim_bytes'] = 1000
            metadata = dict(t['outputs'][0], task_id='A', size_bytes=3, events=2,
                            status='ok', complete=True, runtime_release='r1', partial=False)
            store = Store({'root://host/out': b'abc',
                           'root://host/out.json': json.dumps(metadata).encode()})
            with tempfile.TemporaryDirectory() as tmp:
                result, _ = reconstruct(t, {}, tmp, store)
                self.assertEqual(result['outputs'][0]['events'], 2)
                self.assertEqual(len(result['staged_objects']), 2)
                metadata['runtime_release'] = 'wrong'
                store.objects['root://host/out.json'] = json.dumps(metadata).encode()
                with self.assertRaises(ValueError):
                    reconstruct(t, {}, tmp, store)

    def test_plan_counts_actual_gzip_and_rejects_partial_upload(self):
        t = task('plan'); t['outputs'] = []
        t['inputs'] = [dict(artifact_id='input', url='root://host/input', events=3)]
        t['parameters'].update(events_per_block=2, pool_name='gg', group_id='g',
                               block_output_dir='root://host/blocks',
                               original_output=dict(artifact_id='original', kind='lhe', url='root://host/original'))
        objects = {'root://host/original': b'original'}
        for i, count in enumerate((2, 1)):
            objects[f'root://host/blocks/block_g_{i:06d}.lhe.gz'] = gzip.compress(b'<event>\n' * count)
        with tempfile.TemporaryDirectory() as tmp:
            result, _ = reconstruct(t, {}, tmp, Store(objects))
            self.assertEqual(result['planned_events'], 3)
            self.assertEqual([r['events'] for r in result['outputs']], [2, 1])
            objects['root://host/blocks/block_g_000001.lhe.gz'] = gzip.compress(b'')
            with self.assertRaises(ValueError):
                reconstruct(t, {}, tmp, Store(objects))

    def test_archive_members_are_verified(self):
        t = task('archive')
        t['inputs'] = [dict(artifact_id='in', url='root://host/a.lhe.gz')]
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w') as archive:
            member = tarfile.TarInfo('000000_a.lhe.gz'); member.size = 3
            archive.addfile(member, io.BytesIO(b'abc'))
        blob = data.getvalue()
        metadata = dict(t['outputs'][0], task_id='A', status='ok', complete=True,
                        runtime_release='r1', size_bytes=len(blob))
        store = Store({'root://host/out': blob, 'root://host/out.json': json.dumps(metadata).encode()})
        producer = dict(t['inputs'][0], kind='lhe', size_bytes=3, events=2)
        with tempfile.TemporaryDirectory() as tmp:
            result, _ = reconstruct(t, {'in': producer}, tmp, store)
            self.assertEqual(result['members'][0]['events'], 2)
            producer['size_bytes'] = 4
            with self.assertRaises(ValueError):
                reconstruct(t, {'in': producer}, tmp, store)

    def test_reclaim_never_deletes_and_network_error_is_not_absence(self):
        t = task('reclaim'); t['outputs'] = []; t['inputs'] = [dict(artifact_id='in', url='root://host/in')]
        with tempfile.TemporaryDirectory() as tmp:
            result, _ = reconstruct(t, {}, tmp, Store({}))
            self.assertEqual(result['reclaimed'][0]['objects'][0]['disposition'], 'already-missing')
            with self.assertRaises(ValueError):
                reconstruct(t, {}, tmp, Store({'root://host/in': b'abc'}))
            class Broken(Store):
                def size(self, url):
                    raise RuntimeError('authentication failed')
            with self.assertRaises(RuntimeError):
                reconstruct(t, {}, tmp, Broken({}))


class Preparation(unittest.TestCase):
    def fixture(self, tmp):
        root = Path(tmp)
        atomic_json(root / 'run.json', dict(runtime_release='r1'))
        atomic_json(root / 'state.json', dict(accepted={}, staging={}, planner={'sources': {}}, allocations={}, cleanup_claims={}))
        atomic_json(root / 'submission.json', dict(schema='full-mc-condor-submission-v1', scheduler='schedd', state='submitted', root_cluster_id='42', attempts=[]))
        (root / 'run.dag').write_text('SUBDAG EXTERNAL SOURCE_000000 child.dag\n')
        (root / 'child.dag').write_text('JOB A worker.sub\nSCRIPT POST A controller accept-result\n')
        (root / 'run.dag.dagman.log').write_text('000 (42.000.000) submitted\n...\n005 (42.000.000) terminated\n (1) Normal termination (return value 4)\n...\n')
        (root / 'run.dag.nodes.log').write_text('000 (43.000.000) submitted\n DAG Node: SOURCE_000000\n...\n')
        (root / 'run.dag.lock').touch()
        (root / 'tasks').mkdir(); (root / 'results').mkdir()
        atomic_json(root / 'tasks/A.json', task('lhe'))
        (root / 'child.dag.nodes.log').write_text('000 (44.000.000) submitted\n DAG Node: A\n...\n005 (44.000.000) done\n (1) Normal termination (return value 0)\n...\n')
        return root

    def test_preskip_collision_keeps_both_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'events'
            p.write_text('034 (000.2147483647.001) skip\n DAG Node: A\n...\n034 (000.2147483647.001) skip\n DAG Node: B\n...\n')
            self.assertEqual([e['node'] for e in events(p)], ['A', 'B'])

    def test_preview_apply_and_root_post_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp)
            metadata = dict(task('lhe')['outputs'][0], task_id='A', status='ok', complete=True,
                            runtime_release='r1', size_bytes=3, events=2)
            store = Store({'root://host/out': b'abc', 'root://host/out.json': json.dumps(metadata).encode()})
            calls = []
            def runner(command, **kwargs):
                calls.append(command)
                if command[0] == 'condor_q':
                    return response()
                self.assertEqual(command[1], 'accept-result')
                outcome = json.loads((root / 'results/A.json').read_text())
                atomic_json(root / 'ledger/accepted/A.json', outcome)
                state = json.loads((root / 'state.json').read_text()); state['accepted']['A'] = {'stage': 'lhe'}
                atomic_json(root / 'state.json', state)
                return response()
            preview = prepare_rescue(root, schedd='schedd', root_cluster_id='42', runner=runner, client=store)
            self.assertEqual(preview['state'], 'preview')
            self.assertFalse((root / 'recovery').exists())
            result = prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True, runner=runner, client=store)
            self.assertEqual(result['state'], 'prepared', result.get('blocked'))
            self.assertFalse((root / 'run.dag.lock').exists())
            self.assertIn('DONE A', Path(result['groups'][1]['rescue']).read_text())
            self.assertNotIn('DONE SOURCE_000000', Path(result['groups'][0]['rescue']).read_text())
            self.assertTrue(prepared_failure(root, '42')['terminal'])
            self.assertFalse(any(c[0] == 'condor_submit_dag' for c in calls))
            self.assertEqual(prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True, runner=runner)['state'], 'prepared')
            from full_mc.backends.condor import resume_run
            def submit_runner(command, **kwargs):
                if command[0] in {'condor_q', 'condor_history'}:
                    return response()
                self.assertEqual(command, ['condor_submit_dag', '-no_recurse', '-AutoRescue', '1', 'run.dag'])
                self.assertEqual(kwargs['env']['_CONDOR_SCHEDD_HOST'], 'schedd')
                return response('submitted to cluster 99')
            resumed = resume_run(root, runner=submit_runner)
            self.assertEqual(resumed['root_cluster_id'], '99')

    def test_recovery_replaces_orphan_with_new_verified_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp)
            old = dict(task('lhe'), status='ok')
            old['outputs'][0].update(size_bytes=2, events=2)
            atomic_json(root / 'ledger/accepted/A.json', old)
            new = json.loads(json.dumps(old)); new['outputs'][0]['size_bytes'] = 3
            atomic_json(root / 'results/A.json', new)
            metadata = dict(new['outputs'][0], task_id='A', complete=True, runtime_release='r1')
            store = Store({'root://host/out': b'abc', 'root://host/out.json': json.dumps(metadata).encode()})
            def runner(command, **kwargs):
                if command[0] == 'condor_q':
                    return response()
                self.assertFalse((root / 'ledger/accepted/A.json').exists())
                atomic_json(root / 'ledger/accepted/A.json', new)
                state = json.loads((root / 'state.json').read_text())
                state['accepted']['A'] = {'stage': 'lhe'}
                atomic_json(root / 'state.json', state)
                return response()
            result = prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True,
                                    runner=runner, client=store)
            self.assertEqual(result['state'], 'prepared', result.get('blocked'))
            backup = Path(result['replaced_receipts']['A']['backup'])
            self.assertEqual(json.loads(backup.read_text())['outputs'][0]['size_bytes'], 2)
            self.assertEqual(json.loads((root / 'ledger/accepted/A.json').read_text())['outputs'][0]['size_bytes'], 3)

    def test_interrupted_archive_resumes_without_new_rescue_number(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp)
            outcome = dict(task('lhe'), status='ok')
            outcome['outputs'][0].update(size_bytes=3, events=2)
            state = json.loads((root / 'state.json').read_text())
            state['accepted']['A'] = {'stage': 'lhe'}
            atomic_json(root / 'state.json', state)
            atomic_json(root / 'ledger/accepted/A.json', outcome)
            original = Path.rename
            interrupted = False
            def rename(path, target):
                nonlocal interrupted
                if path.name == 'run.dag.lock' and not interrupted:
                    interrupted = True
                    raise OSError('simulated interruption')
                return original(path, target)
            with patch.object(Path, 'rename', rename):
                with self.assertRaises(OSError):
                    prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True,
                                   runner=lambda *a, **k: response())
            with self.assertRaises(RuntimeError):
                prepared_failure(root, '42')
            result = prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True,
                                    runner=lambda *a, **k: response())
            self.assertEqual(result['state'], 'prepared', result.get('blocked'))
            self.assertEqual(len(list(root.glob('run.dag.rescue*'))), 1)

    def test_missing_successful_product_blocks_preparation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp)
            result = prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True,
                                    runner=lambda *a, **k: response(), client=Store({}))
            self.assertEqual(result['state'], 'blocked')
            self.assertTrue((root / 'run.dag.lock').exists())
            self.assertFalse(list(root.glob('run.dag.rescue*')))

    def test_failed_worker_without_published_metadata_remains_for_rerun(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.fixture(tmp)
            atomic_json(root / 'results/A.json', dict(task_id='A', stage='lhe', status='failed', outputs=[]))
            log = root / 'child.dag.nodes.log'
            log.write_text(log.read_text().replace('return value 0', 'return value 1'))
            result = prepare_rescue(root, schedd='schedd', root_cluster_id='42',
                                    runner=lambda *a, **k: response(), client=Store({}))
            self.assertEqual(result['state'], 'preview')
            self.assertEqual(result['groups'][1]['remaining'], ['A'])

    def test_held_or_failed_query_prevents_all_changes(self):
        for reply in (response('44 0 5'), response(code=1)):
            with tempfile.TemporaryDirectory() as tmp:
                root = self.fixture(tmp)
                with self.assertRaises(RuntimeError):
                    prepare_rescue(root, schedd='schedd', root_cluster_id='42', apply=True, runner=lambda *a, **k: reply)
                self.assertTrue((root / 'run.dag.lock').exists())
                self.assertFalse((root / 'recovery').exists())


if __name__ == '__main__':
    unittest.main()
