"""Tests run on synthetic protobuf feeds, so they work offline and in CI."""

import datetime as dt
import pathlib
import sys

import pandas as pd
from google.transit import gtfs_realtime_pb2

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "collector"))
import collect  # noqa: E402
import fetch_static  # noqa: E402

NOW = dt.datetime(2026, 11, 6, 10, 0, tzinfo=dt.timezone.utc)


def make_feed(vehicles):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1_783_275_030
    for i, v in enumerate(vehicles):
        ent = feed.entity.add()
        ent.id = f"e{i}"
        vp = ent.vehicle
        if "vid" in v:
            vp.vehicle.id = v["vid"]
        if "plate" in v:
            vp.vehicle.license_plate = v["plate"]
        if "route" in v:
            vp.trip.route_id = v["route"]
        if "trip" in v:
            vp.trip.trip_id = v["trip"]
        if "ts" in v:
            vp.timestamp = v["ts"]
        if "lat" in v:
            vp.position.latitude = v["lat"]
            vp.position.longitude = v["lon"]
            if "speed" in v:
                vp.position.speed = v["speed"]
        if "status" in v:
            vp.current_status = v["status"]
    return feed.SerializeToString()


def test_parse_full_vehicle():
    raw = make_feed([{"vid": "b1", "plate": "WXY1234", "route": "300", "trip": "t9",
                      "ts": 1_783_275_000, "lat": 3.07, "lon": 101.6, "speed": 8.5,
                      "status": gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO}])
    (row,) = collect.parse_feed(raw, NOW)
    assert row["route_id"] == "300"
    assert row["trip_id"] == "t9"
    assert abs(row["lat"] - 3.07) < 1e-5
    assert abs(row["speed"] - 8.5) < 1e-5
    assert row["current_status"] == "IN_TRANSIT_TO"
    assert row["feed_ts"] == 1_783_275_030


def test_missing_fields_are_none_not_zero():
    """The key proto2 trap: absent fields must be None, never a fake 0."""
    raw = make_feed([{"vid": "b2"}])
    (row,) = collect.parse_feed(raw, NOW)
    assert row["lat"] is None and row["vehicle_ts"] is None
    assert row["trip_id"] is None and row["speed"] is None
    assert row["current_status"] is None


def test_dedupe_key_identical_fix_across_polls():
    v = {"vid": "b1", "ts": 100, "lat": 3.0, "lon": 101.0}
    a = collect.parse_feed(make_feed([v]), NOW)[0]
    b = collect.parse_feed(make_feed([v]), NOW + dt.timedelta(seconds=30))[0]
    assert collect.vehicle_key(a) == collect.vehicle_key(b)
    c = collect.parse_feed(make_feed([{**v, "ts": 130}]), NOW)[0]
    assert collect.vehicle_key(a) != collect.vehicle_key(c)


def test_collection_loop_dedupes_and_logs(tmp_path, monkeypatch):
    # Two polls return the same fix, the third returns a new fix, the fourth fails.
    payloads = [
        make_feed([{"vid": "b1", "route": "300", "ts": 100, "lat": 3.0, "lon": 101.0}]),
        make_feed([{"vid": "b1", "route": "300", "ts": 100, "lat": 3.0, "lon": 101.0}]),
        make_feed([{"vid": "b1", "route": "300", "ts": 130, "lat": 3.01, "lon": 101.0},
                   {"vid": "b2", "route": "999", "ts": 130, "lat": 3.2, "lon": 101.5}]),
        RuntimeError("boom"),
    ]
    calls = iter(payloads)

    def fake_fetch(_session, timeout=15):
        item = next(calls)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(collect, "fetch", fake_fetch)
    monkeypatch.setattr(collect.time, "sleep", lambda s: None)
    monkeypatch.setenv("ROUTES", "300")

    collect.main(["--minutes", "60", "--interval", "0", "--flush-every", "999",
                  "--max-polls", "4", "--out", str(tmp_path)])

    pos = pd.concat(pd.read_parquet(p) for p in tmp_path.glob("positions/*/*.parquet"))
    polls = pd.concat(pd.read_parquet(p) for p in tmp_path.glob("polls/*/*.parquet"))
    assert len(pos) == 2, "duplicate fix dropped, route 999 filtered out"
    assert set(pos["route_id"]) == {"300"}
    assert polls["ok"].tolist() == [True, True, True, False]
    assert polls["n_new"].tolist() == [1, 0, 1, 0]
    assert polls["error"].iloc[3].startswith("RuntimeError: boom")


def test_static_snapshot_only_on_change(tmp_path):
    first = fetch_static.save_if_changed(b"zip-v1", tmp_path, "2026-11-06")
    again = fetch_static.save_if_changed(b"zip-v1", tmp_path, "2026-11-07")
    changed = fetch_static.save_if_changed(b"zip-v2", tmp_path, "2026-11-08")
    assert first is not None and again is None and changed is not None
    assert len(list((tmp_path / "static").glob("*.zip"))) == 2
