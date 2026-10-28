"""Replay-safe event storage and a wall-clock, trailing ten-minute read model."""
from contextlib import contextmanager
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def normalize(event):
    if not isinstance(event, dict):
        raise ValueError('event must be an object')
    for key in ('agency', 'route_id', 'trip_id', 'start_date'):
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise ValueError(f'{key} is required')
    try:
        ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        if ts.tzinfo is None:
            raise ValueError('timestamp needs a timezone')
        epoch = ts.timestamp()
        datetime.strptime(event['start_date'], '%Y%m%d')
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        raise ValueError('invalid timestamp or service date') from exc
    if epoch > datetime.now(timezone.utc).timestamp() + 60:
        raise ValueError('event timestamp is in the future')
    stops = event.get('stop_time_updates')
    if not isinstance(stops, list) or not stops:
        raise ValueError('stop_time_updates must be a nonempty list')
    delays = []
    for stop in stops:
        if not isinstance(stop, dict):
            raise ValueError('stop update must be an object')
        delay = stop.get('arrival_delay_sec')
        if type(delay) not in (int, float) or not math.isfinite(delay) or not -120 <= delay <= 7200:
            raise ValueError('arrival delay must be numeric and between -120 and 7200')
        delays.append(delay)
    payload = json.dumps(event, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest(), epoch, max(delays), payload


class Store:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as con:
            con.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY, event_ts REAL NOT NULL,
                    agency TEXT NOT NULL, route_id TEXT NOT NULL,
                    trip_id TEXT NOT NULL, start_date TEXT NOT NULL,
                    delay REAL NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS event_time ON events(event_ts);
                CREATE TABLE IF NOT EXISTS rejected (
                    payload_hash TEXT PRIMARY KEY, reason TEXT NOT NULL,
                    received_at REAL NOT NULL);
            ''')

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path, timeout=30)
        try:
            with con:
                yield con
        finally:
            con.close()

    def reject(self, payload, reason):
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self.connect() as con:
            con.execute('INSERT OR IGNORE INTO rejected VALUES (?, ?, ?)',
                        (digest, reason, datetime.now(timezone.utc).timestamp()))

    def ingest(self, event):
        try:
            digest, ts, delay, payload = normalize(event)
        except ValueError as exc:
            self.reject(json.dumps(event, sort_keys=True, default=str), str(exc))
            return False
        with self.connect() as con:
            cursor = con.execute('INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                 (digest, ts, event['agency'], event['route_id'], event['trip_id'],
                                  event['start_date'], delay, payload))
            return cursor.rowcount == 1

    def snapshot(self, now=None):
        now = datetime.now(timezone.utc).timestamp() if now is None else now
        with self.connect() as con:
            count, latest = con.execute('SELECT COUNT(*), MAX(event_ts) FROM events').fetchone()
            rejected = con.execute('SELECT COUNT(*) FROM rejected').fetchone()[0]
            rows = con.execute('''
                WITH trips AS (
                    SELECT agency, route_id, trip_id, start_date, MAX(delay) AS delay
                    FROM events WHERE event_ts > ? AND event_ts <= ?
                    GROUP BY agency, route_id, trip_id, start_date
                )
                SELECT agency, route_id, COUNT(*), SUM(delay >= 180), AVG(delay), MAX(delay)
                FROM trips GROUP BY agency, route_id ORDER BY agency, route_id
            ''', (now - 600, now)).fetchall()
        routes = [dict(agency=a, route_id=r, total_trips=n, delayed_trips=d,
                       delayed_pct=d/n, is_cascade=d/n >= .35,
                       avg_delay_sec=round(avg, 2), max_delay_sec=maximum)
                  for a, r, n, d, avg, maximum in rows]
        return dict(generated_at=datetime.fromtimestamp(now, timezone.utc).isoformat(),
                    source='synthetic simulation', window_seconds=600,
                    metric='Maximum reported stop delay per distinct agency/route/trip/service-date in the last 10 minutes',
                    events_stored=count, rejected_events=rejected,
                    last_event_at=None if latest is None else datetime.fromtimestamp(latest, timezone.utc).isoformat(),
                    stale=latest is None or now-latest > 60, routes=routes)
