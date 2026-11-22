"""Offline new-worker publication ceilings, physical outputs and source archives."""
import gzip
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from full_mc.io import atomic_json
from full_mc.workers._common import publish, result
from full_mc.workers.archive import execute as archive
from full_mc.workers.plan import execute as plan

SCRATCH = Path('/tmp/chiw')
SCRATCH.mkdir(exist_ok=True)


def task(stage='mix', ceiling=100000):
    return {'task_id': stage, 'stage': stage, 'runtime': 'utilities-el9',
            'runtime_release': 'test', 'inputs': [],
            'outputs': [{'artifact_id': stage, 'url': '/stage/' + stage,
                         'kind': 'hepmc' if stage == 'mix' else 'upstream-archive'}],
            'parameters': {'staging_claim_bytes': ceiling}}


class StagedPublication(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_sidecar_is_counted_before_any_upload(self):
        payload = self.root / 'mixed.hepmc.gz'
        payload.write_bytes(b'x' * 10)
        with patch('full_mc.workers._common.TransferClient') as transfer:
            with self.assertRaisesRegex(RuntimeError, 'sidecar exceed'):
                publish(payload, task(ceiling=10))
            transfer.return_value.copy_out.assert_not_called()
        self.assertTrue(payload.with_name(payload.name + '.json').is_file())

    def test_mixed_result_has_exact_payload_and_actual_sidecar(self):
        payload = self.root / 'mixed.hepmc.gz'
        payload.write_bytes(b'compressed mixed data')
        item = task()
        with patch('full_mc.workers._common.TransferClient') as transfer:
            output = publish(payload, item, events=4)
            outcome = result(item, [output])
        objects = outcome['staged_objects']
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0], {'artifact_id': 'mix', 'kind': 'hepmc',
                                     'url': '/stage/mix', 'size_bytes': payload.stat().st_size})
        self.assertEqual(objects[1], {'artifact_id': 'mix:sidecar', 'kind': 'sidecar',
                                     'url': '/stage/mix.json',
                                     'size_bytes': payload.with_name(payload.name + '.json').stat().st_size})
        self.assertEqual(transfer.return_value.copy_out.call_count, 2)

    def test_partial_sidecar_upload_cannot_return_result_and_retry_uses_same_refs(self):
        payload = self.root / 'mixed.hepmc.gz'
        payload.write_bytes(b'mixed payload')
        item = task()
        item['outputs'][0]['url'] = str(self.root / 'staging/mixed.hepmc.gz')
        from full_mc.storage.transfer import TransferClient
        client = TransferClient()
        real_copy = client.copy_out
        failed = False
        def copy(source, destination):
            nonlocal failed
            if str(destination).endswith('.json') and not failed:
                failed = True
                raise RuntimeError('sidecar transfer interrupted')
            return real_copy(source, destination)
        with patch('full_mc.workers._common.TransferClient', return_value=client), \
                patch.object(client, 'copy_out', side_effect=copy):
            with self.assertRaisesRegex(RuntimeError, 'transfer interrupted'):
                publish(payload, item, events=4)
            self.assertTrue(Path(item['outputs'][0]['url']).is_file())
            self.assertFalse(Path(item['outputs'][0]['url'] + '.json').exists())
            output = publish(payload, item, events=4)
            outcome = result(item, [output])
        for physical in outcome['staged_objects']:
            self.assertEqual(physical['size_bytes'], Path(physical['url']).stat().st_size)
        self.assertEqual(len(outcome['staged_objects']), 2)

    def test_legacy_publish_result_preserves_shape(self):
        payload = self.root / 'mixed.hepmc.gz'
        payload.write_bytes(b'data')
        item = task()
        item['parameters'].pop('staging_claim_bytes')
        with patch('full_mc.workers._common.TransferClient'):
            outcome = result(item, [publish(payload, item)])
        self.assertNotIn('staged_objects', outcome)
        self.assertNotIn('sidecar_size_bytes', outcome['outputs'][0])

    def planner(self, retained=False, ceiling=10000):
        source = self.root / 'source.lhe.gz'
        with gzip.open(source, 'wb') as stream:
            stream.write(b'<LesHouchesEvents>\n<event>\n</event>\n</LesHouchesEvents>\n')
        item = task('plan', ceiling)
        item['outputs'] = []
        item['inputs'] = [{'artifact_id': 'original', 'url': str(source), 'kind': 'lhe',
                           'size_bytes': source.stat().st_size, 'events': 1,
                           'sidecar_url': str(source) + '.json'}]
        item['parameters'].update(pool_name='p', primary_seed=1, group_id='s',
                                  events_per_block=1, shuffle_seed=7,
                                  block_output_dir='/stage/blocks',
                                  original_output={'artifact_id': 'original', 'kind': 'lhe',
                                                   'url': str(source) if retained else '/stage/original.lhe.gz'})
        def run(_task, _runtime, command, work):
            if command[0] != 'python3':
                return
            block = work / 'block.lhe.gz'
            block.write_bytes(b'block data')
            atomic_json(work / 'plan.json', {'total_input_events': 1, 'blocks': [
                {'path': str(block), 'filename': block.name, 'index': 0, 'n_events': 1}]})
        return item, run

    def test_plan_checks_all_new_original_and_blocks_before_upload(self):
        item, run = self.planner(ceiling=10)
        with patch('full_mc.workers.plan.run', side_effect=run), \
                patch('full_mc.workers.plan.TransferClient') as transfer:
            with self.assertRaisesRegex(RuntimeError, 'planner outputs exceed'):
                plan(item, self.root / 'work')
            transfer.return_value.copy_out.assert_not_called()

    def test_plan_counts_new_original_and_blocks_without_fictitious_sidecars(self):
        item, run = self.planner()
        with patch('full_mc.workers.plan.run', side_effect=run), \
                patch('full_mc.workers.plan.TransferClient') as transfer:
            outcome = plan(item, self.root / 'work')
        self.assertEqual(transfer.return_value.copy_out.call_count, 2)
        self.assertEqual(len(outcome['staged_objects']), 2)
        self.assertEqual({ref['kind'] for ref in outcome['staged_objects']}, {'lhe', 'lhe-block'})
        self.assertFalse(any(ref['url'].endswith('.json') for ref in outcome['staged_objects']))

    def test_plan_reused_original_remains_reference_without_second_claim(self):
        item, run = self.planner(retained=True, ceiling=10)
        with patch('full_mc.workers.plan.run', side_effect=run), \
                patch('full_mc.workers.plan.TransferClient') as transfer:
            outcome = plan(item, self.root / 'work')
        self.assertEqual(transfer.return_value.copy_out.call_count, 1)
        self.assertEqual(len(outcome['staged_objects']), 1)
        self.assertEqual(outcome['originals'][0]['sidecar_url'], item['inputs'][0]['sidecar_url'])

    def test_source_archive_accepts_frozen_lhe_without_miniaod_gate(self):
        item, _run = self.planner(retained=True)
        item.update(task_id='source-archive', stage='archive')
        item['parameters'] = {'archive_role': 'source', 'source_plan_task': 'PLAN-source'}
        item['outputs'] = task('archive')['outputs']
        with patch('full_mc.workers._common.TransferClient') as transfer:
            outcome = archive(item, self.root / 'archive')
        self.assertEqual(outcome['members'][0]['artifact_id'], 'original')
        self.assertNotIn('staged_objects', outcome)
        self.assertEqual(transfer.return_value.copy_out.call_count, 2)

    def test_source_archive_rejects_non_original_or_unfrozen_input(self):
        for change in ({'kind': 'hepmc'}, {'size_bytes': None}, {'events': 0}):
            item, _run = self.planner(retained=True)
            item.update(stage='archive')
            item['parameters'] = {'archive_role': 'source', 'source_plan_task': 'PLAN-source'}
            item['inputs'][0].update(change)
            with self.assertRaisesRegex(ValueError, 'frozen original'):
                archive(item, self.root / 'archive')

    def test_source_archive_requires_frozen_plan_identity(self):
        item, _run = self.planner(retained=True)
        item.update(stage='archive')
        item['parameters'] = {'archive_role': 'source'}
        with self.assertRaisesRegex(ValueError, 'frozen source PLAN identity'):
            archive(item, self.root / 'archive')

    def test_legacy_archive_gate_still_applies(self):
        item = task('archive')
        with self.assertRaisesRegex(RuntimeError, 'component MiniAODs'):
            archive(item, self.root / 'archive')


if __name__ == '__main__':
    unittest.main()
