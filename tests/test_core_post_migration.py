"""Stopped-run conversion preserves graph and prevents partial activation."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from full_mc.application.dagman_recovery import prepared_failure
from full_mc.io import atomic_json
spec = importlib.util.spec_from_file_location('post_migration', Path(__file__).resolve().parents[1] / 'tools/migrate_post_completion.py')
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


class Migration(unittest.TestCase):
    def test_conversion_and_apply_with_backup(self):
        with tempfile.TemporaryDirectory(dir='/tmp/chiw') as tmp:
            root = Path(tmp) / 'run'; root.mkdir()
            base = root / 'recovery/attempt'; base.mkdir(parents=True)
            build = Path(tmp) / 'build'
            for file, text in [('src/full_mc/application/shards.py', 'def _post_pre_status('),
                               ('src/full_mc/planning/shards.py', '"DAGMAN_ALWAYS_RUN_POST": True'),
                               ('src/full_mc/cli.py', '--pre-return-code')]:
                p = build / 'controller/core' / file; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(text)
            atomic_json(build / 'controller.json', dict(source_dirty=False, build_id='new', git_revision='revision'))
            atomic_json(root / 'submission.json', dict(root_cluster_id='42'))
            atomic_json(root / 'recovery/current.json', dict(state='prepared', root_cluster_id='42',
                        directory=str(base), schedd='schedd', clusters=[42], groups=[dict(dag=str(root/'run.dag'))]))
            original = ('SUBDAG EXTERNAL SOURCE_000000 child.dag\nPRE_SKIP SOURCE_000000 99\n'
                        'SCRIPT DEFER 75 10 POST SOURCE_000000 /run/controller.sh complete-source --return-code $RETURN\n'
                        'PARENT SOURCE_000000 CHILD FINALIZE\n')
            (root/'run.dag').write_text(original)
            (root/'dagman.config').write_text('DAGMAN_ALWAYS_RUN_POST = FALSE\nMAX_OTHER = 10\n')
            calls = []
            def runner(command, **kwargs):
                calls.append(command)
                if command[0] == 'condor_q': return SimpleNamespace(returncode=0, stdout='', stderr='')
                return SimpleNamespace(returncode=0, stdout=json.dumps({'managed': False}), stderr='')
            preview = migration.migrate(root, build, runner=runner)
            self.assertEqual(preview['state'], 'preview')
            self.assertEqual((root/'run.dag').read_text(), original)
            migrated = migration.migrate(root, build, True, runner)
            self.assertEqual(migrated['state'], 'complete')
            self.assertEqual((base/'post-completion/original/run.dag').read_text(), original)
            text = (root/'run.dag').read_text()
            self.assertNotIn('PRE_SKIP', text)
            self.assertIn('DEFER 75 10 POST', text)
            self.assertIn('--pre-return-code $PRE_SCRIPT_RETURN', text)
            self.assertIn('PARENT SOURCE_000000 CHILD FINALIZE', text)
            self.assertEqual(migration.convert(text), text)
            self.assertFalse(any(c[0] == 'condor_submit_dag' for c in calls))
            self.assertEqual(migration.migrate(root, build, True, runner)['state'], 'complete')
            atomic_json(base/'post-completion/migration.json', dict(state='applying'))
            with self.assertRaisesRegex(RuntimeError, 'migration is incomplete'):
                prepared_failure(root, '42')
