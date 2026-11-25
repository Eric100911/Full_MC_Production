"""File layout for bounded AFS directory fanout."""
from collections import Counter
from pathlib import Path
import unittest

from full_mc.planning.graph import log_stem, node_stem


class BucketPaths(unittest.TestCase):
    def test_numbered_job_records_are_bucketed(self):
        self.assertEqual(node_stem("SOURCE_000999"), "SOURCE_/000000/SOURCE_000999")
        self.assertEqual(node_stem("SOURCE_001000"), "SOURCE_/000001/SOURCE_001000")
        self.assertEqual(node_stem("PLAN"), "PLAN")
        self.assertEqual(node_stem("RECLAIM"), "RECLAIM")
        stems = [node_stem(f"PROC_C_JOB000123_BLOCK{i:06d}") for i in range(12460)]
        counts = Counter(Path(stem).parent for stem in stems)
        self.assertEqual(len(set(stems)), 12460)
        self.assertEqual(max(counts.values()), 1000)
        logs = Counter(Path(log_stem(f"PROC_C_JOB000123_BLOCK{i:06d}")).parent
                       for i in range(12460))
        self.assertLessEqual(max(logs.values()) * 3, 1000)
