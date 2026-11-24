"""Contracts for the optional accepted-producer lookup index."""
from copy import deepcopy
import os
import unittest
from unittest.mock import patch

from full_mc.application import producer_index


def ref(artifact_id, *, events=5, url=None):
    return {"artifact_id": artifact_id, "url": url or "stage/" + artifact_id,
            "kind": "lhe", "size_bytes": 100, "events": events}


def state_with(*entries):
    return {"accepted": {task: {"stage": stage} for task, stage in entries}}


class ReceiptReader:
    def __init__(self, receipts):
        self.receipts = receipts
        self.calls = []

    def __call__(self, root, state, task_id):
        self.calls.append(task_id)
        return deepcopy(self.receipts[task_id])


class ProducerIndexTests(unittest.TestCase):
    def setUp(self):
        self.enabled = patch.dict(os.environ, {producer_index.ENVIRONMENT_VARIABLE: "1"})
        self.enabled.start()
        self.addCleanup(self.enabled.stop)

    def test_old_writer_acceptance_makes_count_stale_and_rebuilds_once(self):
        state = state_with(("PLAN_000001", "plan"), ("MIX_C_000001", "lhe"))
        receipts = {"PLAN_000001": {"outputs": [ref("BLOCK:A")]},
                    "MIX_C_000001": {"outputs": [ref("HEPMC:B")]}}
        reader = ReceiptReader(receipts)
        result = producer_index.find("run", state, "BLOCK:A", reader)
        self.assertEqual(result, ref("BLOCK:A"))
        self.assertEqual(state["producer_index"]["accepted_count"], 2)
        self.assertCountEqual(reader.calls, ["PLAN_000001", "MIX_C_000001"])

        # A legacy writer changes accepted but leaves the auxiliary index alone.
        state["accepted"]["ARCHIVE_000001"] = {"stage": "archive"}
        state["accepted"]["MIX_C_000002"] = {"stage": "lhe"}
        receipts["MIX_C_000002"] = {"outputs": [ref("HEPMC:NEW")]}
        reader.calls.clear()
        self.assertEqual(producer_index.find("run", state, "HEPMC:NEW", reader),
                         ref("HEPMC:NEW"))
        self.assertEqual(state["producer_index"]["accepted_count"], 4)
        self.assertCountEqual(reader.calls,
                              ["PLAN_000001", "MIX_C_000001", "MIX_C_000002"])
        reader.calls.clear()
        self.assertEqual(producer_index.find("run", state, "HEPMC:NEW", reader),
                         ref("HEPMC:NEW"))
        self.assertEqual(reader.calls, ["MIX_C_000002"])

    def test_duplicate_generated_original_uses_lexically_first_producer(self):
        original = ref("LHE_000001", url="stage/source.lhe.gz")
        state = state_with(("PLAN_000001", "plan"), ("LHE_000001", "lhe"))
        receipts = {"PLAN_000001": {"outputs": [], "originals": [deepcopy(original)]},
                    "LHE_000001": {"outputs": [deepcopy(original)]}}
        reader = ReceiptReader(receipts)

        result = producer_index.find("run", state, "LHE_000001", reader)
        self.assertEqual(result, original)
        self.assertEqual(state["producer_index"]["artifacts"]["LHE_000001"], "LHE_000001")
        self.assertEqual(reader.calls, ["LHE_000001", "PLAN_000001"])

    def test_duplicate_with_optional_sidecar_size_difference_still_indexes(self):
        original = ref("LHE_000001", url="stage/source.lhe.gz")
        plan_copy = dict(original, sidecar_size_bytes=24)
        lhe_output = dict(original, sidecar_size_bytes=32)
        state = state_with(("PLAN_000001", "plan"), ("LHE_000001", "lhe"))
        receipts = {"PLAN_000001": {"outputs": [], "originals": [plan_copy]},
                    "LHE_000001": {"outputs": [lhe_output]}}
        reader = ReceiptReader(receipts)

        self.assertEqual(producer_index.find("run", state, "LHE_000001", reader), lhe_output)
        self.assertEqual(state["producer_index"]["artifacts"]["LHE_000001"], "LHE_000001")

    def test_duplicate_metadata_disagreement_falls_back_without_caching(self):
        state = state_with(("PLAN_000001", "plan"), ("LHE_000001", "lhe"))
        receipts = {"PLAN_000001": {"originals": [ref("LHE:source", events=6)]},
                    "LHE_000001": {"outputs": [ref("LHE:source", events=5)]}}
        reader = ReceiptReader(receipts)

        result = producer_index.find("run", state, "LHE:source", reader)
        self.assertEqual(result["events"], 5)
        self.assertNotIn("producer_index", state)

    def test_corrupt_missing_and_stale_entries_rebuild_before_returning_hit(self):
        state = state_with(("LHE_000001", "lhe"), ("PLAN_000002", "plan"),
                           ("ARCHIVE_000003", "archive"))
        receipts = {"LHE_000001": {"outputs": [ref("A"), ref("DUP")]},
                    "PLAN_000002": {"outputs": [ref("B"), ref("DUP")]}}
        cases = [
            {"schema": "wrong", "accepted_count": 3, "artifacts": {"A": "LHE_000001"}},
            {"schema": producer_index.SCHEMA, "accepted_count": 3,
             "artifacts": {"A": "LHE_000001"}},  # Missing B.
            {"schema": producer_index.SCHEMA, "accepted_count": 3,
             "artifacts": {"B": "ARCHIVE_000003"}},  # Non-producer hit.
            {"schema": producer_index.SCHEMA, "accepted_count": 3,
             "artifacts": {"B": "LHE_000001"}},  # Producer receipt lacks B.
            {"schema": producer_index.SCHEMA, "accepted_count": 2,
             "artifacts": {"B": "PLAN_000002"}},  # Old writer advanced state.
        ]
        for existing in cases:
            with self.subTest(existing=existing):
                current = deepcopy(state)
                current["producer_index"] = deepcopy(existing)
                reader = ReceiptReader(receipts)
                self.assertEqual(producer_index.find("run", current, "B", reader), ref("B"))
                self.assertEqual(current["producer_index"]["artifacts"]["B"], "PLAN_000002")

    def test_index_hit_reads_and_verifies_only_target_receipt(self):
        state = state_with(("LHE_000001", "lhe"), ("PLAN_000002", "plan"),
                           ("MIX_C_000003", "lhe"))
        receipts = {"LHE_000001": {"outputs": [ref("A")]},
                    "PLAN_000002": {"outputs": [ref("B")]},
                    "MIX_C_000003": {"outputs": [ref("C")]}}
        reader = ReceiptReader(receipts)
        producer_index.find("run", state, "B", reader)  # Rebuild once.
        reader.calls.clear()

        self.assertEqual(producer_index.find("run", state, "B", reader), ref("B"))
        self.assertEqual(reader.calls, ["PLAN_000002"])

    def test_miss_falls_back_and_repairs_missing_mapping(self):
        state = state_with(("LHE_000001", "lhe"), ("PLAN_000002", "plan"))
        state["producer_index"] = {"schema": producer_index.SCHEMA, "accepted_count": 2,
                                   "artifacts": {"A": "LHE_000001"}}
        receipts = {"LHE_000001": {"outputs": [ref("A")]},
                    "PLAN_000002": {"outputs": [ref("B")]}}
        reader = ReceiptReader(receipts)

        self.assertEqual(producer_index.find("run", state, "B", reader), ref("B"))
        self.assertEqual(state["producer_index"]["artifacts"]["B"], "PLAN_000002")

    def test_record_updates_incrementally_including_nonproducers_and_retries(self):
        state = state_with(("ARCHIVE_000000", "archive"))
        state["producer_index"] = {"schema": producer_index.SCHEMA, "accepted_count": 1,
                                   "artifacts": {}}
        state["accepted"]["MIX_C_000000"] = {"stage": "lhe"}
        outcome = {"outputs": [ref("HEPMC:C")]}
        reader = ReceiptReader({})

        producer_index.record("run", state, "MIX_C_000000", outcome, reader)
        self.assertEqual(state["producer_index"], {"schema": producer_index.SCHEMA,
                         "accepted_count": 2, "artifacts": {"HEPMC:C": "MIX_C_000000"}})
        self.assertEqual(reader.calls, [])
        before_retry = deepcopy(state)

        # Repeated acceptance is idempotent; non-producers still count toward
        # the freshness marker without scanning prior producer receipts.
        producer_index.record("run", state, "MIX_C_000000", outcome, reader)
        self.assertEqual(state, before_retry)
        state["accepted"]["CLEANUP_pool_000000"] = {"stage": "reclaim"}
        producer_index.record("run", state, "CLEANUP_pool_000000", {}, reader)
        self.assertEqual(state["producer_index"]["artifacts"],
                         before_retry["producer_index"]["artifacts"])
        self.assertEqual(state["producer_index"]["accepted_count"], 3)
        self.assertEqual(reader.calls, [])

    def test_rebuild_failure_leaves_existing_auxiliary_state_untouched(self):
        state = state_with(("LHE_000001", "lhe"), ("MIX_C_000002", "lhe"))
        state["producer_index"] = {"schema": producer_index.SCHEMA, "accepted_count": 0,
                                   "artifacts": {"stale": "LHE_000001"}}
        before = deepcopy(state["producer_index"])

        def broken_reader(root, current, task_id):
            raise OSError("receipt read failed")

        with self.assertRaisesRegex(OSError, "receipt read failed"):
            producer_index.find("run", state, "target", broken_reader)
        self.assertEqual(state["producer_index"], before)

        state["accepted"]["MIX_C_000003"] = {"stage": "lhe"}
        with self.assertRaisesRegex(OSError, "receipt read failed"):
            producer_index.record("run", state, "MIX_C_000003",
                                  {"outputs": [ref("new")]}, broken_reader)
        self.assertEqual(state["producer_index"], before)

    def test_disabled_mode_scans_without_index_mutation(self):
        state = state_with(("PLAN_000001", "plan"), ("LHE_000001", "lhe"))
        receipts = {"PLAN_000001": {"originals": [ref("DUP")]},
                    "LHE_000001": {"outputs": [ref("DUP")]}}
        reader = ReceiptReader(receipts)
        with patch.dict(os.environ, {producer_index.ENVIRONMENT_VARIABLE: "0"}):
            self.assertEqual(producer_index.find("run", state, "DUP", reader), ref("DUP"))
            producer_index.record("run", state, "LHE_000001", receipts["LHE_000001"], reader)

        self.assertNotIn("producer_index", state)
        self.assertEqual(reader.calls, ["LHE_000001"])

    def test_explicit_rebuild_works_when_feature_is_disabled_without_enabling_it(self):
        state = state_with(("LHE_000001", "lhe"), ("ARCHIVE_000002", "archive"))
        receipts = {"LHE_000001": {"outputs": [ref("A")]}}
        reader = ReceiptReader(receipts)
        with patch.dict(os.environ, {producer_index.ENVIRONMENT_VARIABLE: "0"}):
            index = producer_index.rebuild("run", state, reader)
            self.assertEqual(index, state["producer_index"])
            reader.calls.clear()
            self.assertEqual(producer_index.find("run", state, "A", reader), ref("A"))

        # The explicit sync is available to a controller, while the env switch
        # still selects the sorted scan and does not mutate the built index.
        self.assertEqual(reader.calls, ["LHE_000001"])
        self.assertEqual(index["accepted_count"], 2)


if __name__ == "__main__":
    unittest.main()
