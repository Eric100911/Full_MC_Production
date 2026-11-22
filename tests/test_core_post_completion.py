"""POST completion protocol without PRE_SKIP virtual job identifiers."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from full_mc.application import shards
from full_mc.cli import build_parser, dispatch
from full_mc.io import atomic_json


class PostCompletion(unittest.TestCase):
    def fixture(self, root):
        atomic_json(root / 'run.json', {'sources': [{}], 'runtime_release': 'test'})
        atomic_json(root / 'state.json', {'planner': {'sources': {'0': 'skipped'}},
                    'cleanup_claims': {'CLEANUP_p_000000': {
                        'skipped': True, 'completed': True, 'urls': []}}, 'accepted': {}})

    def test_skip_requires_pre_code_worker_sentinel_and_committed_state(self):
        with tempfile.TemporaryDirectory(dir='/tmp/chiw') as tmp:
            root = Path(tmp); self.fixture(root)
            before = (root / 'state.json').read_bytes()
            for _ in range(2):  # A repeated POST after restart has identical semantics.
                self.assertEqual(shards.complete_source(root, 0, -1004, pre_return_code=99)['exit_code'], 0)
                self.assertEqual(shards.accept_result(root, 'CLEANUP_p_000000', -1004,
                                                     pre_return_code=99)['exit_code'], 0)
            self.assertEqual((root / 'state.json').read_bytes(), before)
            self.assertFalse((root / 'ledger').exists())
            for pre in (1, 75, 98):
                self.assertEqual(shards.complete_source(root, 0, -1004, pre_return_code=pre)['exit_code'], 1)
                self.assertEqual(shards.accept_result(root, 'CLEANUP_p_000000', -1004,
                                                     pre_return_code=pre)['exit_code'], 1)
            self.assertEqual(shards.complete_source(root, 0, 0, pre_return_code=99)['exit_code'], 1)
            self.assertEqual(shards.complete_shard(root, 'C', 0, -1004, pre_return_code=99)['exit_code'], 1)
            self.assertEqual(shards.accept_result(root, 'MIX_missing', -1004,
                                                 pre_return_code=99)['exit_code'], 1)
            state = json.loads(before); state['planner']['sources']['0'] = 'admitted'
            state['cleanup_claims']['CLEANUP_p_000000']['urls'] = ['still-needed']
            atomic_json(root / 'state.json', state)
            self.assertEqual(shards.complete_source(root, 0, -1004, pre_return_code=99)['exit_code'], 1)
            self.assertEqual(shards.accept_result(root, 'CLEANUP_p_000000', -1004,
                                                 pre_return_code=99)['exit_code'], 1)

    def test_generated_root_and_children_use_post_protocol(self):
        from full_mc.planning.shards import write_root_dag, write_child_dag
        with tempfile.TemporaryDirectory(dir='/tmp/chiw') as tmp:
            root = Path(tmp); (root / 'submit').mkdir()
            spec = dict(core_root=tmp, sources=[{'pool': 'p'}], limits={}, scheduler={},
                        campaigns=[dict(name='C', max_processing_nodes=1, sources=[{'pool': 'p'}])])
            root_text = write_root_dag(root, spec).read_text()
            child_text = write_child_dag(root, 'SOURCE_000000', [
                dict(task_id='PLAN_000000', stage='plan', parameters={}),
                dict(task_id='MIX_000000', stage='mix', parameters={})], [], spec,
                claims=['MIX_000000']).read_text()
            self.assertIn('DAGMAN_ALWAYS_RUN_POST = TRUE', (root / 'dagman.config').read_text())
            for text in (root_text, child_text):
                self.assertNotIn('PRE_SKIP ', text)
                for line in text.splitlines():
                    if line.startswith('SCRIPT') and ' POST ' in line:
                        self.assertIn('--pre-return-code $PRE_SCRIPT_RETURN', line)
                        self.assertIn('--return-code $RETURN', line)
            self.assertIn('SCRIPT DEFER 75 60 PRE SOURCE_000000', root_text)
            self.assertIn('CATEGORY SHARD_C_000000 block_processing', root_text)

    def test_no_pre_and_normal_worker_failure_preserve_existing_behavior(self):
        for pre in (-1, 0):
            self.assertEqual(shards.accept_result('/unused', 'A', 42, pre_return_code=pre)['exit_code'], 42)
            self.assertEqual(shards.accept_result('/unused', 'A', 75, pre_return_code=pre)['exit_code'], 1)
        parser = build_parser()
        args = parser.parse_args(['internal', 'complete-source', '--run-dir', '/unused',
                                  '--source-index', '0', '--return-code', '-1004', '--pre-return-code', '99'])
        with patch('full_mc.application.shards.complete_source', return_value={'exit_code': 0}) as complete:
            dispatch(args)
        complete.assert_called_once_with(Path('/unused'), 0, -1004, pre_return_code=99)


@unittest.skipUnless(os.environ.get('MCPROD_NATIVE_DAGMAN_TEST') == '1' and shutil.which('condor_dagman'),
                     'explicit offline native DAGMan test')
class NativePostRecovery(unittest.TestCase):
    def test_defer_skip_chain_restart_and_hard_error(self):
        with tempfile.TemporaryDirectory(dir='/tmp/chiw') as tmp:
            root = Path(tmp)
            (root / 'cfg').write_text('DAGMAN_ALWAYS_RUN_POST = TRUE\nDAGMAN_USER_LOG_SCAN_INTERVAL = 1\n')
            pre = root / 'pre.py'
            pre.write_text("""#!/usr/bin/python3
from pathlib import Path
import sys
name = sys.argv[1]
if name == 'A' and not Path('deferred').exists():
    Path('deferred').touch()
    sys.exit(75)
sys.exit(42 if name == 'ERROR' else 99)
""")
            post = root / 'post.py'
            post.write_text("""#!/usr/bin/python3
from pathlib import Path
import sys,time
name,pre,worker = sys.argv[1:]
if name == 'C':
    Path('started').touch()
    while not Path('release').exists(): time.sleep(.02)
with open('completed', 'a') as f: f.write(name+' '+pre+' '+worker+'\\n')
sys.exit(0 if pre == '99' and worker == '-1004' else 1)
""")
            pre.chmod(0o755); post.chmod(0o755)
            def dag(names):
                lines = ['CONFIG ' + str(root / 'cfg')]
                for name in names:
                    lines += [f'JOB {name} NEVER_SUBMIT_missing.sub',
                              f'SCRIPT DEFER 75 1 PRE {name} {pre} {name}',
                              f'SCRIPT POST {name} {post} {name} $PRE_SCRIPT_RETURN $RETURN']
                lines += [f'PARENT {a} CHILD {b}' for a,b in zip(names, names[1:])]
                (root / 'run.dag').write_text('\n'.join(lines)+'\n')
            env = dict(os.environ, CONDOR_CONFIG='/dev/null', _CONDOR_LOG=tmp,
                       _CONDOR_LOCAL_DIR=tmp, _CONDOR_NETWORK_INTERFACE='127.0.0.1',
                       _CONDOR_USE_SHARED_PORT='FALSE', _CONDOR_DAGMAN_USE_STRICT='0',
                       _CONDOR_DAGMAN_CONFIG_FILE=str(root/'cfg'),
                       _CONDOR_DAGMAN_LOG=str(root/'native.out'),
                       _CONDOR_DAGMAN_REMOVE_NODE_JOBS='FALSE')
            command = ['condor_dagman', '-f', '-l', '.', '-Lockfile', 'run.lock', '-Dag', 'run.dag']
            dag(['A', 'B', 'C'])
            proc = subprocess.Popen(command, cwd=root, env=env, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                deadline = time.monotonic() + 15
                while not (root / 'started').exists() and proc.poll() is None and time.monotonic() < deadline:
                    time.sleep(.05)
                self.assertTrue((root / 'started').exists(), (root/'native.out').read_text()[-3000:])
            finally:
                if proc.poll() is None: os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            (root / 'release').touch()
            completed = subprocess.run(command, cwd=root, env=env, timeout=20,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.assertEqual(completed.returncode, 0, (root/'native.out').read_text()[-3000:])
            self.assertEqual((root/'completed').read_text().splitlines(),
                             ['A 99 -1004', 'B 99 -1004', 'C 99 -1004'])
            log = (root/'run.dag.nodes.log').read_text()
            self.assertNotIn('034 (', log)
            self.assertNotIn('000 (', log)
            self.assertIn('RECOVERY mode', (root/'native.out').read_text())
            # A real PRE error must still fail the DAG through POST.
            (root/'run.dag.nodes.log').unlink()
            dag(['ERROR'])
            failed = subprocess.run(command, cwd=root, env=env, timeout=20,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.assertEqual(failed.returncode, 1)
            self.assertIn('ERROR 42 -1004', (root/'completed').read_text())


if __name__ == '__main__':
    unittest.main()
