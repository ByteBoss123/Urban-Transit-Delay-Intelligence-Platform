"""HTTP dashboard, deterministic simulator, and optional Kafka transport."""
import argparse
import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .store import Store

LOG = logging.getLogger(__name__)
DASHBOARD = Path(__file__).resolve().parents[1] / 'dashboard' / 'index.html'


def events(tick, when=None, vehicles=60):
    """Repeatable synthetic scenario: half of RED's trips cross the threshold."""
    when = when or datetime.now(timezone.utc)
    rng = random.Random(tick)
    for i in range(vehicles):
        route = ('RED', 'BLUE', 'GREEN')[i % 3]
        delay = rng.randint(180, 420) if route == 'RED' and i % 2 == 0 else rng.randint(0, 90)
        yield dict(agency='sim', route_id=route, trip_id=f'TRIP_{i:04}',
                   start_date=when.strftime('%Y%m%d'), timestamp=when.isoformat(),
                   stop_time_updates=[dict(stop_id=f'STOP_{i:04}', arrival_delay_sec=delay)])


def handler_for(store, worker_ok=lambda: True):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split('?', 1)[0]
            if path == '/':
                body, kind, status = DASHBOARD.read_bytes(), 'text/html; charset=utf-8', 200
            elif path in ('/api/routes', '/health'):
                data = store.snapshot()
                data['worker_alive'] = worker_ok()
                if path == '/health':
                    # Fresh source data and a live worker are required for readiness.
                    status = 200 if not data['stale'] and data['worker_alive'] else 503
                else:
                    status = 200
                body, kind = json.dumps(data, allow_nan=False).encode(), 'application/json'
            else:
                body, kind, status = b'Not found', 'text/plain', 404
            self.send_response(status)
            self.send_header('Content-Type', kind)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            LOG.info(fmt, *args)
    return Handler


def simulate(store, stop, interval):
    tick = 0
    while not stop.is_set():
        for event in events(tick):
            store.ingest(event)
        tick += 1
        stop.wait(interval)


def consume(store, stop, broker):
    from confluent_kafka import Consumer, KafkaError
    consumer = Consumer({'bootstrap.servers': broker, 'group.id': 'transit-local-v1',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe(['gtfs.trip_updates'])
    try:
        while not stop.is_set():
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(msg.error()))
            try:
                event = json.loads(msg.value())
            except (ValueError, TypeError) as exc:
                store.reject(repr(msg.value()), str(exc))
            else:
                store.ingest(event)
            # Durable SQLite write/quarantine precedes offset commit. A crash between
            # the two replays the event; content hashing prevents duplicate rows.
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


def produce(stop, broker, interval):
    from confluent_kafka import Producer
    producer = Producer({'bootstrap.servers': broker, 'enable.idempotence': True,
                         'message.timeout.ms': 30000})
    tick = 0
    errors = []
    def delivered(err, msg):
        if err:
            errors.append(str(err))
    while not stop.is_set():
        for event in events(tick):
            producer.produce('gtfs.trip_updates', key=event['trip_id'],
                             value=json.dumps(event), callback=delivered)
            producer.poll(0)
        if producer.flush(35) or errors:
            raise RuntimeError(f'Kafka delivery failed: {errors}')
        tick += 1
        stop.wait(interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['local', 'kafka', 'producer'], nargs='?', default='local')
    parser.add_argument('--db', default=os.getenv('TRANSIT_DB', 'data/transit.db'))
    parser.add_argument('--broker', default=os.getenv('KAFKA_BROKER', 'localhost:9092'))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error('--interval must be positive')
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    if args.mode == 'producer':
        try:
            produce(stop, args.broker, args.interval)
        except KeyboardInterrupt:
            stop.set()
        return
    store = Store(args.db)
    target = simulate if args.mode == 'local' else consume
    extra = args.interval if args.mode == 'local' else args.broker
    worker = threading.Thread(target=target, args=(store, stop, extra), daemon=True)
    worker.start()
    server = ThreadingHTTPServer((args.host, args.port), handler_for(store, worker.is_alive))
    LOG.info('Dashboard: http://%s:%s — synthetic source', args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        worker.join(timeout=5)


if __name__ == '__main__':
    main()
