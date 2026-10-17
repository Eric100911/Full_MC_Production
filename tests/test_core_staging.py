"""Offline authoritative staging accounting and lock recovery contracts."""
from copy import deepcopy
import fcntl
import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from full_mc.application.staging import (initialize, reserve_bundle,
                                         settle_publication, settle_reclaim,
                                         transaction)

SCRATCH = Path('/tmp/chiw')
SCRATCH.mkdir(exist_ok=True)


def obj(identity='a', size=20, url=None, kind='lhe'):
    return dict(artifact_id=identity, url=url or '/stage/' + identity,
                size_bytes=size, kind=kind)


def _reserve_process(root, producer, results, wait_for=None, locked=None, release=None):
    """Independent process with an independently opened permanent lock inode."""
    try:
        if wait_for is not None and not wait_for.wait(10):
            raise RuntimeError('holder did not acquire its lock')
        with transaction(root) as state:
            code = reserve_bundle(state, {producer: 100})
            if locked is not None:
                locked.set()
            if release is not None and not release.wait(10):
                raise RuntimeError('holder was not released')
        results.put((producer, code))
    except BlockingIOError:
        results.put((producer, 75))
    except Exception as exc:
        results.put((producer, repr(exc)))


class StagingContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = initialize(self.root, 100, {'next_source': 0})

    def publish(self):
        self.assertEqual(reserve_bundle(self.state, {'producer': 60}), 0)
        settle_publication(self.state, 'producer', [obj(), obj('a:sidecar', 5, kind='sidecar')])

    def test_initialize_refuses_existing_staging(self):
        with self.assertRaises(FileExistsError):
            initialize(self.root, 100, {})

    def test_bundle_hold_does_not_mutate(self):
        reserve_bundle(self.state, {'one': 60})
        before = deepcopy(self.state)
        self.assertEqual(reserve_bundle(self.state, {'two': 20, 'three': 30}), 75)
        self.assertEqual(self.state, before)

    def test_reserve_replay_claim_and_settled(self):
        self.publish()
        self.assertEqual(reserve_bundle(self.state, {'producer': 60}), 0)
        self.assertEqual(self.state['staging']['claims'], {})
        self.assertEqual(self.state['staging']['resident_bytes'], 25)
        with self.assertRaisesRegex(ValueError, 'ceiling changed'):
            reserve_bundle(self.state, {'producer': 59})

    def test_publication_exact_replay_and_physical_dedup(self):
        self.publish()
        settle_publication(self.state, 'producer', [obj('a:sidecar', 5, kind='sidecar'), obj()])
        reserve_bundle(self.state, {'another': 30})
        settle_publication(self.state, 'another', [obj(), obj()])
        self.assertEqual(self.state['staging']['resident_bytes'], 25)
        with self.assertRaisesRegex(ValueError, 'publication changed'):
            settle_publication(self.state, 'producer', [obj(size=21)])

    def test_publication_failure_keeps_claim(self):
        reserve_bundle(self.state, {'producer': 10})
        before = deepcopy(self.state)
        with self.assertRaisesRegex(ValueError, 'ceiling'):
            settle_publication(self.state, 'producer', [obj()])
        self.assertEqual(before, self.state)

    def test_conflicting_url_identity_fails_before_mutation(self):
        self.publish()
        reserve_bundle(self.state, {'other': 30})
        before = deepcopy(self.state)
        with self.assertRaisesRegex(ValueError, 'identity'):
            settle_publication(self.state, 'other', [obj('new', 1), obj('wrong', 20, '/stage/a')])
        self.assertEqual(before, self.state)

    def test_same_artifact_identity_cannot_move_to_another_url(self):
        self.publish()
        reserve_bundle(self.state, {'other': 30})
        before = deepcopy(self.state)
        with self.assertRaisesRegex(ValueError, 'identity'):
            settle_publication(self.state, 'other', [obj(url='/stage/other')])
        self.assertEqual(self.state, before)

    def test_corrupt_claim_ceiling_is_hard_failure(self):
        self.state['staging']['claims']['unknown'] = 1
        with self.assertRaisesRegex(ValueError, 'ceiling'):
            reserve_bundle(self.state, {'next': 20})

    def test_reclaim_payload_and_sidecar_once(self):
        self.publish()
        refs = [obj(), obj('a:sidecar', 5, kind='sidecar')]
        results = [{'artifact_id': 'a', 'objects': [
            {'url': '/stage/a', 'kind': 'payload', 'disposition': 'removed'},
            {'url': '/stage/a:sidecar', 'kind': 'sidecar', 'disposition': 'already-missing'}]}]
        settle_reclaim(self.state, 'clean', refs, results)
        settle_reclaim(self.state, 'clean', refs, results)
        settle_reclaim(self.state, 'clean-again', refs, results)
        self.assertEqual(self.state['staging']['resident_bytes'], 0)
        self.assertEqual(reserve_bundle(self.state, {'next': 100}), 0)

    def test_reclaim_requires_complete_exact_success(self):
        self.publish()
        before = deepcopy(self.state)
        for refs, results in [
            ([obj()], []),
            ([obj()], [{'artifact_id': 'a', 'url': '/stage/a', 'status': 'failed'}]),
            ([obj(size=21)], [{'artifact_id': 'a', 'url': '/stage/a', 'status': 'removed'}]),
            ([obj()], [{'artifact_id': 'wrong', 'url': '/stage/a', 'status': 'removed'}]),
        ]:
            with self.assertRaises(ValueError):
                settle_reclaim(self.state, 'clean', refs, results)
            self.assertEqual(self.state, before)

    def test_transaction_success_and_exception_rollback(self):
        inode = (self.root / 'staging/ledger.lock').stat().st_ino
        with transaction(self.root) as state:
            reserve_bundle(state, {'a': 40})
        with self.assertRaisesRegex(RuntimeError, 'abort'):
            with transaction(self.root) as state:
                reserve_bundle(state, {'b': 20})
                raise RuntimeError('abort')
        with transaction(self.root) as state:
            self.assertEqual(state['staging']['claims'], {'a': 40})
        self.assertEqual(inode, (self.root / 'staging/ledger.lock').stat().st_ino)

    def test_lock_busy_is_nonblocking_and_missing_lock_is_not_created(self):
        lockpath = self.root / 'staging/ledger.lock'
        with lockpath.open('r+b') as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                with transaction(self.root):
                    self.fail('busy lock was entered')
        lockpath.unlink()
        with self.assertRaises(FileNotFoundError):
            with transaction(self.root):
                self.fail('missing lock was recreated')
        self.assertFalse(lockpath.exists())

    def test_two_processes_cannot_both_take_last_credit(self):
        context = multiprocessing.get_context('spawn')
        locked, release = context.Event(), context.Event()
        results = context.Queue()
        holder = context.Process(target=_reserve_process,
                                 args=(str(self.root), 'holder', results),
                                 kwargs={'locked': locked, 'release': release})
        contender = context.Process(target=_reserve_process,
                                    args=(str(self.root), 'contender', results),
                                    kwargs={'wait_for': locked})
        processes = [holder, contender]
        try:
            holder.start()
            contender.start()
            self.assertEqual(results.get(timeout=10), ('contender', 75))
            release.set()
            self.assertEqual(results.get(timeout=10), ('holder', 0))
            for process in processes:
                process.join(timeout=10)
                self.assertEqual(process.exitcode, 0)
            retry = context.Process(target=_reserve_process,
                                    args=(str(self.root), 'contender', results))
            processes.append(retry)
            retry.start()
            self.assertEqual(results.get(timeout=10), ('contender', 75))
            retry.join(timeout=10)
            self.assertEqual(retry.exitcode, 0)
            with transaction(self.root) as state:
                self.assertEqual(state['staging']['claims'], {'holder': 100})
        finally:
            release.set()
            for process in processes:
                if process.pid is not None:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=10)
            results.close()
            results.join_thread()

    def test_directory_fsync_failure_is_hard_and_retry_rereads_replaced_state(self):
        real_fsync = os.fsync
        calls = 0
        def fail_directory(fd):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError('directory fsync unsupported')
            real_fsync(fd)
        with patch('full_mc.application.staging.os.fsync', side_effect=fail_directory):
            with self.assertRaisesRegex(OSError, 'directory fsync'):
                with transaction(self.root) as state:
                    reserve_bundle(state, {'holder': 100})
        with transaction(self.root) as state:
            self.assertEqual(state['staging']['claims'], {'holder': 100})
            self.assertEqual(reserve_bundle(state, {'holder': 100}), 0)
            self.assertEqual(reserve_bundle(state, {'other': 1}), 75)
        self.assertEqual(list(self.root.glob('.state-*')), [])

    def test_corruption_is_hard_failure(self):
        (self.root / 'state.json').write_text('{broken')
        with self.assertRaises(json.JSONDecodeError):
            with transaction(self.root):
                self.fail('corrupt state accepted')

    def test_fsync_failure_does_not_replace_state(self):
        before = (self.root / 'state.json').read_bytes()
        with patch('full_mc.application.staging.os.fsync', side_effect=OSError('disk')):
            with self.assertRaises(OSError):
                with transaction(self.root) as state:
                    state['status'] = 'changed'
        self.assertEqual((self.root / 'state.json').read_bytes(), before)
        self.assertEqual(list(self.root.glob('.state-*')), [])


if __name__ == '__main__':
    unittest.main()
