"""Offline fanout bounds and writer/reader contracts for bucketed local files."""
from collections import defaultdict
from pathlib import Path
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from full_mc.application import shards
from full_mc.backends.condor import _worker_proxy_paths
from full_mc.io import read_json
from full_mc.local_paths import (LAYOUT_MARKER, dag_path, identity_bucket,
                                log_path, record_path)
from full_mc.planning.shards import child_path
from tests.test_core_shards import ShardFixture
from tools.dag_progress import parse_dag_graph


class BucketPaths(unittest.TestCase):
    def test_numeric_boundaries_and_namespaces(self):
        self.assertEqual(identity_bucket("PLAN_000999"), Path("PLAN_/000000"))
        self.assertEqual(identity_bucket("PLAN_001000"), Path("PLAN_/000001"))
        self.assertEqual(identity_bucket("PLAN_000332", 333), Path("PLAN_/000000"))
        self.assertEqual(identity_bucket("PLAN_000333", 333), Path("PLAN_/000001"))
        self.assertEqual(identity_bucket("PLAN_000999", 333), Path("PLAN_/000003"))
        self.assertEqual(identity_bucket("PLAN_001000", 333), Path("PLAN_/000003"))
        ids = ["MIX_C_JOB000000_BLOCK001000", "MIX_D_JOB000000_BLOCK001000",
               "MINIAOD_C_v1_JOB000000_BLOCK001000", "MINIAOD_C_v2_JOB000000_BLOCK001000",
               "ARCHIVE_SHARD_C_001000", "RECLAIM_SHARD_C_001000"]
        self.assertEqual(len({identity_bucket(i) for i in ids}), len(ids))
        with self.assertRaises(ValueError):
            identity_bucket("../PLAN_000000")

    def test_formal_scale_file_counts_and_directory_bounds(self):
        # 12,460 generated sources, six campaigns * 1000 nodes, 10-node shards,
        # one detector variant; source/shard completion triggers add cleanups.
        workers = [f"{stage}_{i:06d}" for i in range(12460)
                   for stage in ("LHE", "PLAN", "SOURCE_ARCHIVE", "SOURCE_RECLAIM")]
        children = [f"SOURCE_{i:06d}" for i in range(12460)]
        for campaign in range(6):
            workers.extend(f"{stage}_C{campaign}_JOB000000_BLOCK{i:06d}"
                           for i in range(1000) for stage in ("MIX", "MINIAOD_v1"))
            for i in range(100):
                name = f"SHARD_C{campaign}_{i:06d}"
                children.append(name)
                workers.extend(("ARCHIVE_" + name, "RECLAIM_" + name))
        self.assertEqual(len(workers), 63040)
        workers.extend(f"CLEANUP_p{pool}_{i:06d}"
                       for pool, count in enumerate((3416, 3416, 3416, 3116))
                       for i in range(count))
        self.assertEqual(len(workers), 76404)
        files, entries = defaultdict(set), defaultdict(set)

        def add(path):
            files[path.parent].add(path.name)
            while path.parent != Path("/run"):
                entries[path.parent].add(path.name)
                path = path.parent

        with patch("full_mc.local_paths.bucketed", new=lambda root: True):
            for identity in workers:
                for directory in ("tasks", "results", "ledger/accepted"):
                    add(record_path("/run", directory, identity))
                for suffix in (".out", ".err", ".tar.gz"):
                    add(log_path("/run", identity, suffix))
            for identity in children:
                add(record_path("/run", "ledger/shards", identity))
                dag = dag_path("/run", identity)
                # Native DAGMan side files share only this child's directory.
                for suffix in ("", ".condor.sub", ".dagman.log", ".lib.out", ".lib.err",
                               ".metrics", ".nodes.log", ".lock", ".rescue001", ".rescue002"):
                    add(Path(str(dag) + suffix))
        self.assertEqual(sum(len(v) for p, v in files.items() if "/logs/" in str(p)), 229212)
        self.assertEqual(max(len(v) for p, v in files.items() if "/logs/" in str(p)), 999)
        self.assertEqual(max(map(len, files.values())), 1000)
        self.assertLessEqual(max(map(len, entries.values())), 1000)


class BucketLifecycle(ShardFixture):
    def test_generated_submit_paths_receipts_replay_and_legacy_flat(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy), tempfile.TemporaryDirectory() as directory:
                run = self.fixture(directory, nodes=1, campaigns=1)
                if legacy:
                    (run / LAYOUT_MARKER).unlink()
                    spec = read_json(run / "run.json")
                    from full_mc.planning.graph import write_submit_files
                    from full_mc.planning.shards import write_root_dag
                    write_submit_files(run, spec["core_root"], spec["runtime_dir"], "/afs/test/proxy")
                    write_root_dag(run, spec)
                shards.prepare_source(run, 0)
                dag = child_path(run, "SOURCE_000000")
                graph = parse_dag_graph(run / "run.dag")
                self.assertEqual(graph.children["SOURCE_000000"].dag_file, dag)
                frozen = dag.read_bytes()
                shards.prepare_source(run, 0)
                self.assertEqual(dag.read_bytes(), frozen)
                identity = "PLAN_000000"
                variables = next(line for line in dag.read_text().splitlines()
                                 if line.startswith("VARS " + identity + " "))
                macros = dict(re.findall(r'(\w+)="([^"]*)"', variables))
                submit = (run / "submit/plan.sub").read_text()
                for key, value in macros.items():
                    submit = submit.replace("$(" + key + ")", value)
                self.assertIn(str(record_path(run, "tasks", identity)), submit)
                self.assertIn(str(record_path(run, "results", identity)), submit)
                for suffix in (".out", ".err", ".tar.gz"):
                    self.assertIn(str(log_path(run, identity, suffix)), submit)
                self.assertIn("arguments = bootstrap.py --task PLAN_000000.json", submit)
                self.assertEqual(_worker_proxy_paths(run), ["/afs/test/proxy"])
                self.source_plan(run)
                self.assertEqual(shards.accept_result(run, identity)["exit_code"], 0)
                self.assertTrue(shards._receipt_path(run, identity).is_file())
                self.source_done(run)
                shards.prepare_shard(run, "C", 0)
                self.finish_shard(run, "C", 0)
                cleanup = shards.prepare_cleanup(run, "pool", 2)
                self.assertEqual(cleanup["status"], "prepared")
                self.assertTrue(record_path(run, "tasks", cleanup["task_id"]).is_file())
                self.reclaim_result(run, cleanup["task_id"])
                self.assertEqual(shards.finalize_run(run)["merge_groups"], 1)
                self.assertEqual(len(shards.committed_groups(run)), 1)
                self.assertEqual(len(shards.committed_archives(run)), 2)
                if legacy:
                    self.assertEqual(dag, run / "subdags/SOURCE_000000.dag")
                    self.assertEqual(record_path(run, "tasks", identity), run / "tasks/PLAN_000000.json")

    @unittest.skipUnless(shutil.which("condor_submit_dag"), "native DAGMan unavailable")
    def test_native_subdag_adjacent_files_and_auto_rescue(self):
        with tempfile.TemporaryDirectory() as directory:
            run = self.fixture(directory, nodes=1, campaigns=1)
            shards.prepare_source(run, 0)
            child = child_path(run, "SOURCE_000000")
            # -no_submit is offline: render native DAGMan descriptors only.
            outcome = subprocess.run(["condor_submit_dag", "-no_submit", "-do_recurse",
                                      "-AutoRescue", "1", str(child)], cwd=run,
                                     env={**os.environ, "CONDOR_CONFIG": "/dev/null"},
                                     capture_output=True, text=True, check=False)
            self.assertEqual(outcome.returncode, 0, outcome.stderr[-1000:])
            native = Path(str(child) + ".condor.sub").read_text()
            self.assertIn(str(child), native)
            self.assertIn("-AutoRescue 1", native)
            self.assertTrue(Path(str(child) + ".condor.sub").is_file())
            self.assertIn(str(run / "submit/plan.sub"), child.read_text())
