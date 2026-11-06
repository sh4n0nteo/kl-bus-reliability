"""
Snapshot the Rapid KL static schedule (GTFS zip) -- but only when it changes.

WHY
Schedules change (new routes, new frequencies). If you analyse October's bus positions
against December's timetable, your "lateness" numbers are wrong and you won't know it.
So we keep every distinct version, dated, and the analysis joins positions to the
schedule that was valid on that day.

Hashing the content means re-running this hourly costs one download and zero storage
unless Prasarana actually published something new.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import pathlib
import sys

import requests

STATIC_URL = "https://api.data.gov.my/gtfs-static/prasarana?category=rapid-bus-kl"
MYT = dt.timezone(dt.timedelta(hours=8))


def save_if_changed(content: bytes, out: pathlib.Path, today: str) -> pathlib.Path | None:
    digest = hashlib.sha256(content).hexdigest()[:16]
    base = out / "static"
    base.mkdir(parents=True, exist_ok=True)
    latest = base / "LATEST"
    if latest.exists() and latest.read_text().split()[0] == digest:
        return None
    target = base / f"{today}_{digest}.zip"
    target.write_bytes(content)
    latest.write_text(f"{digest} {target.name}\n")
    return target


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("data"))
    args = ap.parse_args(argv)
    resp = requests.get(STATIC_URL, timeout=60,
                        headers={"User-Agent": "kl-bus-reliability-collector/0.1"})
    resp.raise_for_status()
    saved = save_if_changed(resp.content, args.out, dt.datetime.now(MYT).strftime("%Y-%m-%d"))
    print(f"new schedule saved: {saved}" if saved else "schedule unchanged")
    return 0


if __name__ == "__main__":
    sys.exit(main())
