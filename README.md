# How late is my bus, really?

**Ranking Rapid KL bus routes by how reliable they actually are, measured from weeks of live GPS data rather than the published schedule.**

> Status: collector running since `<date>` · `<N>` days of data · see the `data` branch

---

## Why this exists

A Rapid KL route that says "every 15 minutes" can still leave you waiting 25. Riders feel this every day, but nobody publishes the number. Malaysia's open data portal (data.gov.my) streams the live position of every Rapid KL bus every 30 seconds, so the number can be measured.

## The core metric: Excess Wait Time

Most KL routes run on a frequency ("every 15 min"), not a fixed timetable, so "late vs. schedule" is often undefined. The honest question is **how much longer riders wait than the frequency promises.** Transport for London uses this metric, called *excess wait time (EWT)*, for its high-frequency buses.

If you turn up at a random moment, your average wait depends on the gaps between buses (headways):

```
average wait (AWT) = Σh² / (2 · Σh)
scheduled wait     = scheduled headway / 2
EWT                = AWT − scheduled wait
```

### Worked example: same frequency, very different waits

All three routes below run 4 buses an hour, an average gap of 15 min, so the scheduled wait is 7.5 min.

| Observed gaps (min) | Σh | Σh² | AWT | EWT |
|---|---|---|---|---|
| 15, 15, 15, 15 (perfect) | 60 | 900 | 7.5 | **0.0** |
| 5, 25, 10, 20 (irregular) | 60 | 1150 | 9.6 | **+2.1** |
| 2, 28, 2, 28 (bunching) | 60 | 1576 | 13.1 | **+5.6** |

The "average frequency" is identical in all three cases, yet riders on the bunched route wait nearly twice as long. That gap is invisible on the schedule, and it's what this project measures.

Where trips do have real timetables, schedule adherence (minutes early or late at each stop) is reported as well.

## Method

1. **Collect** (`collector/collect.py`): poll the GTFS-Realtime vehicle-position feed every 30s during operating hours. Duplicate GPS fixes are dropped and every poll is logged, so collector downtime is never mistaken for missing buses.
2. **Snapshot schedules** (`collector/fetch_static.py`): save each distinct version of the static schedule, so positions are always compared with the schedule that was valid that day.
3. **Detect stop passages**: project each GPS fix onto the route's shape and interpolate the time the bus passed each stop. At ~30 km/h, fixes 30s apart are ~250 m apart, so "nearest GPS point to the stop" isn't good enough.
4. **Compute headways**: time gaps between consecutive buses at each stop, then AWT, EWT and bunching rate (gaps under 25% of the scheduled headway) per route, stop and hour.
5. **Validate**: stand at a stop, log real arrivals by hand for ~2 hours, and compare them with the detected passages. This is the ground truth the rest of the analysis rests on.

## Proof-layer checklist

| | |
|---|---|
| **Owner** | Rapid KL riders, plus Prasarana's operations planners |
| **Baseline** | The published frequency/timetable, i.e. what riders are promised |
| **Metric** | Excess wait time per route, stop and hour; bunching rate |
| **Eval** | Manual arrival log at a real stop vs. detected passages (target: median error < 60s) |
| **Failure modes** | See *Known limitations* |
| **Decision log** | See *Design decisions* |

## Design decisions

| Decision | Chosen | Over | Why |
|---|---|---|---|
| Where the collector runs | GitHub Actions, public repo | Laptop, paid VPS | Free, runs 24/7, and the commit history publicly proves weeks of collection |
| Job shape | 1 job per hour, polling for 55 min | Many 5-min cron jobs | Scheduled runs can start late; one long job loses less data |
| Storage | Parquet on a `data` branch | CSV, database | Typed and ~10x smaller; DuckDB/pandas read it directly; no server to maintain |
| Reliability metric | Excess wait time | Raw "minutes late" | Most routes are frequency-based, so lateness against a timetable is undefined |
| Duplicates | Dedupe on vehicle + GPS timestamp + position | Keep every poll | Stale repeated fixes would fake "bus standing still" |

## Known limitations

- **GPS lag**: a fix can be 30–60s old when polled. `vehicle_ts` (when the bus reported) is used, never `polled_at`.
- **Collector gaps**: GitHub's scheduled runs can start late or skip under load. The poll log measures coverage, and hours below 90% coverage are excluded rather than silently counted.
- **Buses not in service**: buses driving to the depot may report positions with no trip attached. They're filtered by `trip_id` and route-shape distance.
- **Service day vs. calendar day**: trips after midnight belong to the previous service day.
- **Schedule changes**: handled by dated schedule snapshots, but routes renamed mid-study break the comparison.
- **Inactivity**: GitHub disables scheduled workflows in repos with no activity for 60 days. Check the Actions tab weekly.

## Setup

```bash
# 1. Create a PUBLIC GitHub repo and push this folder to main.
# 2. Actions tab -> "collect" -> Run workflow (tests run first, then collection).
# 3. After ~1 hour, check the `data` branch for positions/, polls/ and static/.
# 4. Optional: limit to chosen routes via Settings > Secrets and variables > Actions >
#    Variables > ROUTES = "300,T789"

# Local probe: one poll and a report of which fields the feed actually fills in
pip install -r requirements.txt
python collector/collect.py --once

# Tests (offline, synthetic feeds)
python -m pytest -q
```

## Roadmap

- [x] Collector, schedule snapshots, tests, CI
- [ ] Week 1: data-quality report (coverage %, GPS fix age, buses on road per hour)
- [ ] Week 2: stop-passage detection along route shapes
- [ ] Week 2: manual validation at one real stop
- [ ] Week 3: headways → AWT / EWT / bunching per route
- [ ] Week 4: public findings page ("The 5 least reliable routes, and when")

## Data

Live and static GTFS data from Prasarana Malaysia via the [data.gov.my GTFS APIs](https://developer.data.gov.my/realtime-api/gtfs-realtime). Check data.gov.my's terms of use and keep this attribution in anything you publish.
