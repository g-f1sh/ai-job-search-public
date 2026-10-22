"""Bounded CV claiming, bulk scope, and isolated task lifecycle."""
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.portal_store import PortalStore, ConflictError
from tools.portal_worker import PortalWorker
from tests.test_portal_worker import FakeCodex, TRACKER_HEADER


class ParallelCVTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'job_scraper').mkdir()
        (self.root / 'cv').mkdir()
        (self.root / 'job_search_tracker.csv').write_text(TRACKER_HEADER)
        (self.root / 'job_scraper/seen_jobs.json').write_text(json.dumps({'seen': {
            str(i): {'company': f'Company {i}', 'title': 'Implementation Engineer',
                     'url': f'https://example.com/{i}', 'status': 'new',
                     'qualification_status': 'recommended', 'responsibilities_reviewed': True,
                     'qualification_reasons': ['Reviewed technical implementation responsibilities']}
            for i in range(5)}}))
        self.store = PortalStore(self.root)
        self.jobs = sorted(self.store.list_jobs(), key=lambda j: j['company'])

    def tearDown(self):
        self.temp.cleanup()

    def wait(self, predicate):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            if predicate():
                return
            time.sleep(.02)
        self.fail('Timed out waiting for concurrent task state')

    def test_bulk_skips_ready_active_and_non_new_and_preserves_status(self):
        pdf = self.root / 'cv/ready.pdf'
        pdf.write_bytes(b'fixture PDF')
        with patch('tools.portal_store.verify_pdf'):
            self.store.set_artifact(self.jobs[0]['id'], 'cv', 'cv/ready.pdf', source='test')
        active = self.store.enqueue_run('generate_cv', job_id=self.jobs[1]['id'])
        self.store.update_application_status(self.jobs[3]['id'], 'shortlisted', source='test')
        self.store.set_qualification(self.jobs[4]['id'], 'filtered_out', reasons=['Wrong requirements'], responsibilities_reviewed=True, source='test')
        result = self.store.enqueue_new_cvs(idempotency_key='click')
        self.assertEqual((result['queued'], result['ready'], result['already_active']), (1, 1, 1))
        self.assertEqual(result['runs'][0]['job_id'], self.jobs[2]['id'])
        again = self.store.enqueue_new_cvs(idempotency_key='click-again')
        self.assertEqual((again['queued'], again['already_active']), (0, 2))
        self.assertEqual(self.store.get_run(active['id'])['state'], 'queued')
        self.assertEqual(self.store.get_job(self.jobs[2]['id'])['application_status'], 'new')
        with self.assertRaises(ConflictError):
            self.store.enqueue_run('generate_cv', job_id=self.jobs[3]['id'], require_new=True)

    def test_concurrent_clicks_only_create_one_task_per_new_job(self):
        results = []
        start = threading.Barrier(5)
        def click(i):
            start.wait()
            results.append(self.store.enqueue_new_cvs(idempotency_key=f'click-{i}'))
        threads = [threading.Thread(target=click, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        start.wait()
        for t in threads:
            t.join(5)
            self.assertFalse(t.is_alive())
        self.assertEqual(len(results), 4)
        runs = self.store.list_runs()
        self.assertEqual(len(runs), 5)
        self.assertEqual(len({r['job_id'] for r in runs}), 5)

    def test_claim_limit_generic_exclusion_and_same_conversation_serialization(self):
        conversation = self.store.create_conversation('Shared')
        first = self.store.enqueue_run('generate_cv', job_id=self.jobs[0]['id'], conversation_id=conversation['id'])
        second = self.store.enqueue_run('generate_cv', job_id=self.jobs[1]['id'], conversation_id=conversation['id'])
        third = self.store.enqueue_run('generate_cv', job_id=self.jobs[2]['id'])
        search = self.store.enqueue_run('search')
        self.assertEqual(self.store.claim_next_run(cv_limit=2)['id'], first['id'])
        self.assertEqual(self.store.claim_next_run(cv_limit=2)['id'], third['id'])
        self.assertIsNone(self.store.claim_next_run(cv_limit=2))
        self.store.finish_run(third['id'], 'completed')
        self.assertIsNone(self.store.claim_next_run(cv_limit=2))  # no overlap with same chat or search
        self.store.finish_run(first['id'], 'completed')
        self.assertEqual(self.store.claim_next_run(cv_limit=2)['id'], second['id'])
        self.store.finish_run(second['id'], 'completed')
        self.assertEqual(self.store.claim_next_run(cv_limit=2)['id'], search['id'])
        self.store.enqueue_run('generate_cv', job_id=self.jobs[3]['id'])
        self.assertIsNone(self.store.claim_next_run(cv_limit=2))

    def test_four_cv_slots_leave_fifth_durably_queued(self):
        runs = [self.store.enqueue_run('generate_cv', job_id=j['id']) for j in self.jobs]
        claimed = [self.store.claim_next_run(cv_limit=4) for _ in range(4)]
        self.assertEqual([r['id'] for r in claimed], [r['id'] for r in runs[:4]])
        self.assertIsNone(self.store.claim_next_run(cv_limit=4))
        self.assertEqual(self.store.get_run(runs[4]['id'])['state'], 'queued')
        self.store.finish_run(claimed[0]['id'], 'completed')
        self.assertEqual(self.store.claim_next_run(cv_limit=4)['id'], runs[4]['id'])

    def test_parallel_execution_cancel_isolated_and_remaining_queue_completes(self):
        release = threading.Event()
        entered = threading.Barrier(3)
        fakes = []
        def factory():
            fake = FakeCodex(before_complete=lambda: (entered.wait(timeout=5), release.wait(5))) if len(fakes) < 2 else FakeCodex()
            fakes.append(fake)
            return fake
        runs = [self.store.enqueue_run('generate_cv', job_id=j['id']) for j in self.jobs[:3]]
        worker = PortalWorker(self.store, self.root, codex=FakeCodex(), codex_factory=factory, poll_interval=.01)
        worker.start()
        try:
            entered.wait(timeout=5)
            self.assertEqual([self.store.get_run(r['id'])['state'] for r in runs], ['running', 'running', 'queued'])
            worker.cancel(runs[0]['id'])
            self.assertTrue(fakes[0].interrupted)
            self.assertFalse(fakes[1].interrupted)
            release.set()
            self.wait(lambda: all(self.store.get_run(r['id'])['state'] in ('failed', 'interrupted') for r in runs))
            self.assertEqual(self.store.get_run(runs[0]['id'])['state'], 'interrupted')
            self.assertIn('associated PDF', self.store.get_run(runs[1]['id'])['error'])
            self.assertEqual(len(fakes), 3)
            self.assertTrue(all(self.store.get_job(j['id'])['application_status'] == 'new' for j in self.jobs))
        finally:
            release.set()
            worker.stop()

    def test_stop_all_fences_parallel_and_queued_and_restart_preserves_history(self):
        self.store.enqueue_new_cvs(idempotency_key='all')
        a = self.store.claim_next_run(cv_limit=2)
        b = self.store.claim_next_run(cv_limit=2)
        stop = self.store.request_stop(idempotency_key='stop')
        self.assertEqual((stop['cancelled_queued'], stop['stopping']), (3, 2))
        self.assertIsNone(self.store.claim_next_run(cv_limit=2))
        self.store.reconcile_unfinished_runs()
        reopened = PortalStore(self.root)
        self.assertEqual(reopened.get_run(a['id'])['state'], 'interrupted')
        self.assertEqual(reopened.get_run(b['id'])['state'], 'interrupted')
        self.assertEqual(len(reopened.list_runs()), 5)

    def test_two_validated_artifacts_are_linked_to_their_own_jobs(self):
        entered = threading.Barrier(2)
        fakes = []
        runs = [self.store.enqueue_run('generate_cv', job_id=j['id']) for j in self.jobs[:2]]
        def factory():
            job = self.jobs[len(fakes)]
            def draft():
                entered.wait(timeout=5)
                path = f'cv/Candidate_CV_{job["id"]}.pdf'
                (self.root / path).write_bytes(b'fixture isolated PDF')
                self.store.set_artifact(job['id'], 'cv', path, source='test')
            fake = FakeCodex(before_complete=draft)
            fakes.append(fake)
            return fake
        worker = PortalWorker(self.store, self.root, codex=FakeCodex(), codex_factory=factory, poll_interval=.01)
        with patch('tools.portal_store.verify_pdf'):
            worker.start()
            try:
                self.wait(lambda: all(self.store.get_run(r['id'])['state'] == 'completed' for r in runs))
                for job, run in zip(self.jobs, runs):
                    ready = self.store.get_job(job['id'])
                    self.assertEqual(ready['cv']['state'], 'ready')
                    self.assertIn(job['id'], ready['cv']['path'])
                    self.assertEqual(self.store.get_run(run['id'])['artifacts'][0]['url'], ready['cv']['url'])
                    self.assertEqual(ready['application_status'], 'new')
            finally:
                worker.stop()

    def test_mid_turn_disconnect_does_not_stop_sibling_or_block_next_cv(self):
        release = threading.Event()
        fakes = []
        def factory():
            fake = FakeCodex(disconnect_once=True) if not fakes else FakeCodex(before_complete=lambda: release.wait(5)) if len(fakes) == 1 else FakeCodex()
            fakes.append(fake)
            return fake
        runs = [self.store.enqueue_run('generate_cv', job_id=j['id']) for j in self.jobs[:3]]
        worker = PortalWorker(self.store, self.root, codex=FakeCodex(), codex_factory=factory, poll_interval=.01)
        worker.start()
        try:
            self.wait(lambda: len(fakes) == 3 and self.store.get_run(runs[0]['id'])['state'] == 'failed')
            self.assertIn('connection closed', self.store.get_run(runs[0]['id'])['error'])
            self.assertEqual(self.store.get_run(runs[1]['id'])['state'], 'running')
            self.assertTrue(fakes[1].running)
            self.assertFalse(fakes[1].interrupted)
            release.set()
            self.wait(lambda: all(self.store.get_run(r['id'])['state'] == 'failed' for r in runs))
            self.assertIn('associated PDF', self.store.get_run(runs[1]['id'])['error'])
        finally:
            release.set()
            worker.stop()
