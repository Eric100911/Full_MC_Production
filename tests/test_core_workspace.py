import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from full_mc.application.workspace import initialize
from full_mc.settings import load_run_spec


def git(*args, cwd):
    env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull)
    return subprocess.run(["git", *map(str, args)], cwd=cwd, env=env,
                          check=True, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE).stdout.strip()


class WorkspaceTests(unittest.TestCase):
    def make_core(self, root):
        core = root / "core-source"
        core.mkdir()
        git("init", cwd=core)
        (core / "mcprod").write_text("#!/bin/sh\n", encoding="utf-8")
        git("add", "mcprod", cwd=core)
        git("-c", "user.name=Test User", "-c", "user.email=test@example.invalid",
            "commit", "-m", "add mcprod", cwd=core)
        return core

    def test_initializes_pinned_uncommitted_wrapper(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            core = self.make_core(root)
            target = root / "workspace"
            result = initialize(target, core)

            self.assertEqual(result["core_revision"], git("rev-parse", "HEAD", cwd=core))
            self.assertEqual(git("rev-parse", "HEAD", cwd=target / "core"),
                             result["core_revision"])
            self.assertTrue((target / ".gitmodules").is_file())
            self.assertTrue((target / "campaigns" / "example.json").is_file())
            example = load_run_spec(target / "campaigns" / "example.json")
            self.assertEqual(example["campaigns"][0]["name"], "JJP_SPS_CS")
            self.assertEqual(example["sources"][0]["generation"]["max_events"], 10000)
            self.assertEqual(example["sources"][0]["seed"], 1)
            self.assertEqual(example["runtime_dir"],
                             str((target / "runtimes/production-v1/cern").resolve()))
            self.assertEqual(example["limits"]["source_lhe_bytes"], 2000000000)
            self.assertEqual(example["limits"]["processing_nodes_per_wave"], 1)
            self.assertTrue((target / "history" / "README.md").is_file())
            self.assertIn("work/", (target / ".gitignore").read_text(encoding="utf-8"))
            self.assertIn("core", git("status", "--short", cwd=target))
            with self.assertRaises(subprocess.CalledProcessError):
                git("rev-parse", "HEAD", cwd=target)

    def test_rejects_nonempty_target_and_core_without_mcprod_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            core = root / "core-source"
            core.mkdir()
            git("init", cwd=core)
            (core / "README").write_text("old\n", encoding="utf-8")
            git("add", "README", cwd=core)
            git("-c", "user.name=Test User", "-c", "user.email=test@example.invalid",
                "commit", "-m", "old core", cwd=core)
            target = root / "workspace"
            target.mkdir()
            (target / "keep").write_text("user data\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                initialize(target, core)
            self.assertEqual((target / "keep").read_text(encoding="utf-8"), "user data\n")

            empty = root / "empty"
            empty.mkdir()
            with self.assertRaises(ValueError):
                initialize(empty, core)
            self.assertEqual(list(empty.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
