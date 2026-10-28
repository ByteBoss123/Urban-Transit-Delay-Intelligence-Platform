import copy
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from transit.app import events, handler_for
from transit.store import Store


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / 'transit.db')
        self.now = datetime.now(timezone.utc)
        self.epoch = self.now.timestamp()

    def test_simulator_to_persisted_metrics_and_replay(self):
        batch = list(events(0, self.now))
        for event in batch:
            self.assertTrue(self.store.ingest(event))
            self.assertFalse(self.store.ingest(event))
        # Reopening proves the read model does not depend on process memory.
        snapshot = Store(self.store.path).snapshot(self.epoch)
        self.assertEqual(snapshot['events_stored'], 60)
        red = next(r for r in snapshot['routes'] if r['route_id'] == 'RED')
        self.assertEqual((red['total_trips'], red['delayed_trips']), (20, 10))
        self.assertTrue(red['is_cascade'])
        self.assertEqual(sum(r['is_cascade'] for r in snapshot['routes']), 1)

    def test_window_expiry_out_of_order_and_trip_grain(self):
        event = next(events(0, self.now))
        self.store.ingest(event)
        older = copy.deepcopy(event)
        older['timestamp'] = (self.now - timedelta(seconds=30)).isoformat()
        older['stop_time_updates'][0]['arrival_delay_sec'] = 600
        self.store.ingest(older)
        same = self.store.snapshot(self.epoch)['routes'][0]
        self.assertEqual(same['total_trips'], 1)
        self.assertEqual(same['max_delay_sec'], 600)
        self.assertEqual(self.store.snapshot(self.epoch + 600)['routes'], [])
        other_agency = copy.deepcopy(event)
        other_agency['agency'] = 'other'
        self.store.ingest(other_agency)
        self.assertEqual(len(self.store.snapshot(self.epoch)['routes']), 2)

    def test_invalid_records_are_quarantined(self):
        for bad in [None, {}, {'timestamp': 'bad'}]:
            self.assertFalse(self.store.ingest(bad))
        event = next(events(0, self.now))
        event['stop_time_updates'][0]['arrival_delay_sec'] = float('nan')
        self.assertFalse(self.store.ingest(event))
        self.assertFalse(self.store.ingest(event))  # replay does not inflate rejects
        self.assertEqual(self.store.snapshot(self.epoch)['rejected_events'], 4)

    def test_threshold_boundary_and_service_date_grain(self):
        for i in range(20):
            event = next(events(0, self.now))
            event['trip_id'] = str(i)
            event['stop_time_updates'][0]['arrival_delay_sec'] = 180 if i < 7 else 179
            self.store.ingest(event)
        route = self.store.snapshot(self.epoch)['routes'][0]
        self.assertEqual(route['delayed_pct'], .35)
        self.assertTrue(route['is_cascade'])
        event['start_date'] = (self.now - timedelta(days=1)).strftime('%Y%m%d')
        self.store.ingest(event)
        self.assertEqual(self.store.snapshot(self.epoch)['routes'][0]['total_trips'], 21)

    def test_http_serves_pipeline_output_and_readiness(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(self.store))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f'http://127.0.0.1:{server.server_port}'
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(base + '/health')
            self.assertEqual(error.exception.code, 503)
            for event in events(0, self.now):
                self.store.ingest(event)
            with urllib.request.urlopen(base + '/health') as response:
                self.assertEqual(response.status, 200)
            with urllib.request.urlopen(base + '/api/routes') as response:
                result = json.load(response)
                self.assertEqual(result['events_stored'], 60)
                self.assertEqual(len(result['routes']), 3)
            with urllib.request.urlopen(base) as response:
                page = response.read().decode()
                self.assertIn("fetch('/api/routes'", page)
                self.assertNotIn('Math.random', page)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == '__main__':
    unittest.main()
