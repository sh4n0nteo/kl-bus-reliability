"""
Poll Rapid KL's live bus positions (GTFS-Realtime) and store them as Parquet.

WHY THIS DESIGN
- Poll every 30s       -> the feed itself only refreshes every 30s; faster is wasted calls.
- Dedupe on (vehicle_id, vehicle_ts)
                       -> a bus that hasn't sent a new GPS fix shows up identical in the
                          next poll. Keeping duplicates would fake "the bus stood still".
- Poll log             -> every poll is recorded (status, how many buses, errors). Later you
                          MUST be able to tell "no buses on the road" apart from "my collector
                          was down". Without this, gaps in your data look like gaps in service.
- Parquet, not CSV     -> typed columns, ~10x smaller, and pandas/DuckDB read it directly.
- Flush every N minutes-> if the job gets killed mid-run you lose minutes, not the hour.

USAGE
  python collector/collect.py --once                 # one poll, print what the feed contains
  python collector/collect.py --minutes 55 --out data
  ROUTES="300,T789" python collector/collect.py ...  # optional route allowlist
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import pathlib
import sys
import time

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

FEED_URL = (
    "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana"
    "?category=rapid-bus-kl"
)
MYT = dt.timezone(dt.timedelta(hours=8))  # partition by Malaysian date, not UTC
USER_AGENT = "kl-bus-reliability-collector/0.1 (open-data research project)"

STATUS_NAMES = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus
CONGESTION_NAMES = gtfs_realtime_pb2.VehiclePosition.CongestionLevel


def parse_feed(raw: bytes, polled_at: dt.datetime) -> list[dict]:
    """Decode one GTFS-RT protobuf payload into flat rows (one per vehicle).

    GTFS-RT uses proto2, so HasField() tells us whether the agency actually sent a
    value or we're seeing a default zero. That distinction matters: lat=0.0 means
    "missing", not "bus is in the Gulf of Guinea".
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)
    feed_ts = feed.header.timestamp if feed.header.HasField("timestamp") else None

    rows = []
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        pos = v.position if v.HasField("position") else None
        rows.append(
            {
                "polled_at": polled_at,
                "feed_ts": feed_ts,
                "vehicle_ts": v.timestamp if v.HasField("timestamp") else None,
                "entity_id": ent.id or None,
                "vehicle_id": v.vehicle.id or None,
                "license_plate": v.vehicle.license_plate or None,
                "trip_id": v.trip.trip_id or None,
                "route_id": v.trip.route_id or None,
                "start_time": v.trip.start_time or None,
                "start_date": v.trip.start_date or None,
                "lat": pos.latitude if pos is not None else None,
                "lon": pos.longitude if pos is not None else None,
                "bearing": pos.bearing if pos is not None and pos.HasField("bearing") else None,
                "speed": pos.speed if pos is not None and pos.HasField("speed") else None,
                "stop_id": v.stop_id or None,
                "current_status": (
                    STATUS_NAMES.Name(v.current_status) if v.HasField("current_status") else None
                ),
                "congestion": (
                    CONGESTION_NAMES.Name(v.congestion_level)
                    if v.HasField("congestion_level")
                    else None
                ),
            }
        )
    return rows


def vehicle_key(row: dict) -> tuple:
    """Identity of one GPS fix. Falls back to plate, then entity id, if vehicle_id is blank."""
    who = row["vehicle_id"] or row["license_plate"] or row["entity_id"]
    return (who, row["vehicle_ts"], row["lat"], row["lon"])


def fetch(session: requests.Session, timeout: float = 15) -> bytes:
    resp = session.get(FEED_URL, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def flush(rows: list[dict], polls: list[dict], out: pathlib.Path) -> None:
    """Write buffered rows to date-partitioned Parquet part files, then clear buffers."""
    now = dt.datetime.now(MYT)
    stamp = now.strftime("%H%M%S")
    day = now.strftime("%Y-%m-%d")
    if rows:
        p = out / "positions" / f"date={day}"
        p.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(p / f"part-{stamp}.parquet", index=False)
    if polls:
        p = out / "polls" / f"date={day}"
        p.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(polls).to_parquet(p / f"part-{stamp}.parquet", index=False)
    rows.clear()
    polls.clear()


def coverage_report(rows: list[dict]) -> str:
    """For --once: which fields does the agency actually populate? This decides the method."""
    if not rows:
        return "Feed returned 0 vehicles (outside ~06:00-23:00 MYT the feed is often empty)."
    df = pd.DataFrame(rows)
    lines = [f"{len(df)} vehicles in feed. Field coverage (% non-empty):"]
    for col in ["vehicle_id", "license_plate", "trip_id", "route_id", "start_time",
                "stop_id", "current_status", "speed", "bearing", "congestion"]:
        lines.append(f"  {col:<15} {df[col].notna().mean():6.0%}")
    if df["route_id"].notna().any():
        lines.append(f"Distinct routes: {df['route_id'].nunique()}")
    if df["vehicle_ts"].notna().any():
        lag = df["feed_ts"].max() - df["vehicle_ts"]
        lines.append(f"GPS fix age vs feed header (s): median {lag.median():.0f}, max {lag.max():.0f}")
    lines.append("\nSample rows:")
    lines.append(df.head(3).to_string())
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true", help="poll once and print a field-coverage report")
    ap.add_argument("--minutes", type=float, default=55, help="how long to keep polling")
    ap.add_argument("--interval", type=float, default=30, help="seconds between polls")
    ap.add_argument("--flush-every", type=float, default=10, help="minutes between Parquet writes")
    ap.add_argument("--max-polls", type=int, default=0, help="stop after N polls (0 = no limit; for testing)")
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("data"))
    args = ap.parse_args(argv)

    allow = {r.strip() for r in os.environ.get("ROUTES", "").split(",") if r.strip()}
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    if args.once:
        polled_at = dt.datetime.now(dt.timezone.utc)
        rows = parse_feed(fetch(session), polled_at)
        print(coverage_report(rows))
        return 0

    rows: list[dict] = []
    polls: list[dict] = []
    seen: set[tuple] = set()
    deadline = time.monotonic() + args.minutes * 60
    next_flush = time.monotonic() + args.flush_every * 60
    failures_in_a_row = 0

    n_polls = 0
    while time.monotonic() < deadline and not (args.max_polls and n_polls >= args.max_polls):
        n_polls += 1
        started = time.monotonic()
        polled_at = dt.datetime.now(dt.timezone.utc)
        log = {"polled_at": polled_at, "ok": False, "n_vehicles": 0, "n_new": 0, "error": None}
        try:
            batch = parse_feed(fetch(session), polled_at)
            if allow:
                batch = [r for r in batch if r["route_id"] in allow]
            new = []
            for r in batch:
                k = vehicle_key(r)
                if k not in seen:
                    seen.add(k)
                    new.append(r)
            rows.extend(new)
            log.update(ok=True, n_vehicles=len(batch), n_new=len(new))
            failures_in_a_row = 0
        except Exception as exc:  # network blips must not kill a 55-minute job
            log["error"] = f"{type(exc).__name__}: {exc}"[:300]
            failures_in_a_row += 1
            print(f"[warn] poll failed ({failures_in_a_row} in a row): {log['error']}", file=sys.stderr)
        polls.append(log)

        if time.monotonic() >= next_flush:
            flush(rows, polls, args.out)
            next_flush = time.monotonic() + args.flush_every * 60

        # Back off on repeated failures (30s, 60s, 120s... capped at 5 min) instead of hammering.
        wait = args.interval * (2 ** min(failures_in_a_row, 4)) if failures_in_a_row else args.interval
        time.sleep(max(0.0, min(wait, 300) - (time.monotonic() - started)))

    flush(rows, polls, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
