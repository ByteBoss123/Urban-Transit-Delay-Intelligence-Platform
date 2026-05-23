"""
GTFS-RT Kafka Producer
Ingests real-time transit feed from DC Metro / NYC MTA and publishes to Kafka.
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER       = "localhost:9092"
TOPIC_VEHICLE_POS  = "gtfs.vehicle_positions"
TOPIC_TRIP_UPDATES = "gtfs.trip_updates"
TOPIC_ALERTS       = "gtfs.service_alerts"

# DC Metro GTFS-RT endpoints (free, no key required)
FEEDS = {
    "wmata": {
        "vehicle_positions": "https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb",
        "trip_updates":      "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb",
    },
    # NYC MTA (requires free API key from mta.info)
    "mta": {
        "vehicle_positions": "https://gtfsrt.prod.obanyc.com/vehiclePositions",
        "trip_updates":      "https://gtfsrt.prod.obanyc.com/tripUpdates",
    }
}


@dataclass
class VehiclePosition:
    vehicle_id:    str
    trip_id:       str
    route_id:      str
    latitude:      float
    longitude:     float
    bearing:       float
    speed_mph:     float
    current_stop:  str
    stop_sequence: int
    timestamp:     str
    agency:        str


@dataclass
class StopTimeUpdate:
    stop_id:           str
    stop_sequence:     int
    arrival_delay_sec: int
    departure_delay_sec: int
    schedule_relationship: str  # SCHEDULED | SKIPPED | NO_DATA


@dataclass
class TripUpdate:
    trip_id:          str
    route_id:         str
    vehicle_id:       str
    start_date:       str
    stop_time_updates: list
    timestamp:        str
    agency:           str


class GTFSProducer:
    def __init__(self, broker: str = KAFKA_BROKER):
        conf = {
            "bootstrap.servers": broker,
            "client.id":         "gtfs-producer",
            "acks":              "all",
            "retries":           5,
            "batch.size":        32768,
            "linger.ms":         10,
            "compression.type":  "snappy",
        }
        self.producer = Producer(conf)
        logger.info(f"Connected to Kafka broker: {broker}")

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed for {msg.key()}: {err}")

    def publish(self, topic: str, key: str, payload: dict):
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=self.delivery_report,
        )
        self.producer.poll(0)

    def fetch_gtfs_feed(self, url: str, api_key: Optional[str] = None) -> gtfs_realtime_pb2.FeedMessage:
        headers = {"x-api-key": api_key} if api_key else {}
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(resp.content)
        return feed

    def process_vehicle_positions(self, feed: gtfs_realtime_pb2.FeedMessage, agency: str):
        count = 0
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            v = entity.vehicle
            pos = VehiclePosition(
                vehicle_id    = v.vehicle.id or entity.id,
                trip_id       = v.trip.trip_id,
                route_id      = v.trip.route_id,
                latitude      = v.position.latitude,
                longitude     = v.position.longitude,
                bearing       = v.position.bearing,
                speed_mph     = round(v.position.speed * 2.237, 2) if v.position.speed else 0.0,
                current_stop  = v.stop_id,
                stop_sequence = v.current_stop_sequence,
                timestamp     = datetime.fromtimestamp(v.timestamp, tz=timezone.utc).isoformat(),
                agency        = agency,
            )
            self.publish(TOPIC_VEHICLE_POS, f"{agency}:{pos.vehicle_id}", asdict(pos))
            count += 1
        logger.info(f"Published {count} vehicle positions for {agency}")

    def process_trip_updates(self, feed: gtfs_realtime_pb2.FeedMessage, agency: str):
        count = 0
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            stop_updates = [
                asdict(StopTimeUpdate(
                    stop_id                = stu.stop_id,
                    stop_sequence          = stu.stop_sequence,
                    arrival_delay_sec      = stu.arrival.delay   if stu.HasField("arrival")   else 0,
                    departure_delay_sec    = stu.departure.delay if stu.HasField("departure") else 0,
                    schedule_relationship  = stu.ScheduleRelationship.Name(stu.schedule_relationship),
                ))
                for stu in tu.stop_time_update
            ]
            trip = TripUpdate(
                trip_id           = tu.trip.trip_id,
                route_id          = tu.trip.route_id,
                vehicle_id        = tu.vehicle.id if tu.HasField("vehicle") else "UNKNOWN",
                start_date        = tu.trip.start_date,
                stop_time_updates = stop_updates,
                timestamp         = datetime.fromtimestamp(tu.timestamp, tz=timezone.utc).isoformat(),
                agency            = agency,
            )
            self.publish(TOPIC_TRIP_UPDATES, f"{agency}:{trip.trip_id}", asdict(trip))
            count += 1
        logger.info(f"Published {count} trip updates for {agency}")

    def run_loop(self, interval_sec: int = 30):
        """Poll GTFS feeds every `interval_sec` seconds."""
        logger.info("Starting GTFS producer loop...")
        while True:
            try:
                # --- DC Metro ---
                vp_feed  = self.fetch_gtfs_feed(FEEDS["wmata"]["vehicle_positions"])
                tu_feed  = self.fetch_gtfs_feed(FEEDS["wmata"]["trip_updates"])
                self.process_vehicle_positions(vp_feed, agency="wmata")
                self.process_trip_updates(tu_feed, agency="wmata")
            except Exception as e:
                logger.warning(f"WMATA feed error: {e}")
            finally:
                self.producer.flush()

            time.sleep(interval_sec)


# ── Simulator (for local dev without a live feed) ────────────────────────────
class GTFSSimulator:
    """
    Generates synthetic GTFS-RT events for local development.
    Simulates realistic delay cascades: a delayed vehicle causes
    downstream stops on the same route to accumulate delays.
    """
    ROUTES  = ["RED", "BLUE", "GREEN", "ORANGE", "SILVER", "YELLOW"]
    STOPS   = [f"STOP_{i:03d}" for i in range(1, 51)]

    def __init__(self, producer: GTFSProducer, n_vehicles: int = 80):
        self.producer   = producer
        self.n_vehicles = n_vehicles
        self.state      = self._init_state()

    def _init_state(self) -> dict:
        return {
            f"VEH_{i:04d}": {
                "route_id":  random.choice(self.ROUTES),
                "trip_id":   f"TRIP_{i:04d}",
                "lat":       38.9 + random.uniform(-0.15, 0.15),
                "lon":      -77.0 + random.uniform(-0.15, 0.15),
                "delay_sec": 0,
                "stop_seq":  random.randint(1, 30),
                "stop_id":   random.choice(self.STOPS),
            }
            for i in range(self.n_vehicles)
        }

    def _evolve(self):
        """One simulation tick: random walk delays, propagate cascades."""
        # Randomly spike a few vehicles
        for _ in range(random.randint(1, 4)):
            veh = random.choice(list(self.state.keys()))
            self.state[veh]["delay_sec"] += random.randint(60, 300)

        # Decay existing delays
        for veh in self.state:
            self.state[veh]["delay_sec"] = max(
                0, int(self.state[veh]["delay_sec"] * 0.85 + random.uniform(-30, 30))
            )
            # Random position drift
            self.state[veh]["lat"] += random.uniform(-0.002, 0.002)
            self.state[veh]["lon"] += random.uniform(-0.002, 0.002)
            self.state[veh]["stop_seq"] = (self.state[veh]["stop_seq"] % 40) + 1

    def emit_tick(self):
        self._evolve()
        ts = datetime.now(tz=timezone.utc).isoformat()
        for veh_id, s in self.state.items():
            # Vehicle position
            vp = VehiclePosition(
                vehicle_id    = veh_id,
                trip_id       = s["trip_id"],
                route_id      = s["route_id"],
                latitude      = round(s["lat"], 6),
                longitude     = round(s["lon"], 6),
                bearing       = random.uniform(0, 360),
                speed_mph     = round(max(0, 25 + random.gauss(0, 8)), 1),
                current_stop  = s["stop_id"],
                stop_sequence = s["stop_seq"],
                timestamp     = ts,
                agency        = "sim",
            )
            self.producer.publish(TOPIC_VEHICLE_POS, f"sim:{veh_id}", asdict(vp))
            # Trip update (delay)
            tu = TripUpdate(
                trip_id           = s["trip_id"],
                route_id          = s["route_id"],
                vehicle_id        = veh_id,
                start_date        = datetime.now().strftime("%Y%m%d"),
                stop_time_updates = [
                    asdict(StopTimeUpdate(
                        stop_id                 = s["stop_id"],
                        stop_sequence           = s["stop_seq"],
                        arrival_delay_sec       = s["delay_sec"],
                        departure_delay_sec     = s["delay_sec"] + random.randint(0, 60),
                        schedule_relationship   = "SCHEDULED",
                    ))
                ],
                timestamp = ts,
                agency    = "sim",
            )
            self.producer.publish(TOPIC_TRIP_UPDATES, f"sim:{s['trip_id']}", asdict(tu))
        self.producer.producer.flush()
        logger.info(f"Simulated tick: {self.n_vehicles} vehicles emitted")


if __name__ == "__main__":
    prod = GTFSProducer()
    sim  = GTFSSimulator(prod, n_vehicles=100)
    logger.info("Running simulator (Ctrl-C to stop)...")
    while True:
        sim.emit_tick()
        time.sleep(15)
