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


def task(stage='mix'):
    return {'task_id': stage, 'stage': stage, 'runtime': 'utilities-el9',
            'runtime_release': 'test', 'inputs': [],
            'outputs': [{'artifact_id': stage, 'url': '/stage/' + stage,
                         'kind': 'hepmc' if stage == 'mix' else 'upstream-archive'}],
            'parameters': {}}


class StagedPublication(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_publish_result_preserves_shape(self):
        payload = self.root / 'mixed.hepmc.gz'
        payload.write_bytes(b'data')
        item = task()
        with patch('full_mc.workers._common.TransferClient'):
            outcome = result(item, [publish(payload, item)])
        self.assertNotIn('staged_objects', outcome)
        self.assertNotIn('sidecar_size_bytes', outcome['outputs'][0])

    def planner(self, retained=False):
        source = self.root / 'source.lhe.gz'
        with gzip.open(source, 'wb') as stream:
            stream.write(b'<LesHouchesEvents>\n<event>\n</event>\n</LesHouchesEvents>\n')
        item = task('plan')
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

    def test_plan_publishes_original_and_blocks_without_fictitious_sidecars(self):
        item, run = self.planner()
        with patch('full_mc.workers.plan.run', side_effect=run), \
                patch('full_mc.workers.plan.TransferClient') as transfer:
            outcome = plan(item, self.root / 'work')
        self.assertEqual(transfer.return_value.copy_out.call_count, 2)
        self.assertEqual(outcome['outputs'][0]['kind'], 'lhe-block')
        self.assertEqual(outcome['originals'][0]['kind'], 'lhe')
        self.assertNotIn('sidecar_url', outcome['outputs'][0])

    def test_plan_reused_original_remains_reference_without_second_upload(self):
        item, run = self.planner(retained=True)
        with patch('full_mc.workers.plan.run', side_effect=run), \
                patch('full_mc.workers.plan.TransferClient') as transfer:
            outcome = plan(item, self.root / 'work')
        self.assertEqual(transfer.return_value.copy_out.call_count, 1)
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


if __name__ == '__main__':
    unittest.main()
