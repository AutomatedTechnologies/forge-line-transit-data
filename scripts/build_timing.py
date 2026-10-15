#!/usr/bin/env python3
"""Build the endpoint/v1 event stream and timing model from telemetry history.

Reads data/*.jsonl (telemetry/v1 poll records), runs the per-vehicle segment
detector defined in PROTOCOL.md, and writes:

  data/vehicles/events-*.jsonl - endpoint/v1 event stream (append-only,
                                 monthly shards rolled at PART_BYTES; the
                                 legacy events.jsonl monolith is split into
                                 shards by rotate_events() when oversized)
  data/timing/segments.json    - aggregated empirical timing model
  data/timing/watermark.json   - incremental processing state

Incremental: per-day line counts in the watermark mean re-runs only process
new polls. Detector state (anchors, dwell, anomaly episodes) persists in the
watermark so event emission is idempotent.

Honesty rules: traversals spanning a poll gap > 30 min are discarded (anchor
reset, no event). Timing cells need >= 3 observations. Anomalies are flagged,
never deleted.

Dwell correction (added 2026-10-14, decision of record in the transit
goal's DWELL-REFRESH.md): the segment detector measures anchor-hold time
starting at a segment's from_stop, so layover dwell at a route terminus is
inside the raw traversed seconds. At aggregation time only, cells whose
from_stop is a terminus for the cell's route (or Rosa Parks Transit
Center, stop 531) have that (route, from_stop) pair's median observed
dwell subtracted from mean_s / p50_s / p90_s. Raw events are never
altered; the correction lives only in the aggregated model.
"""
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo
    DETROIT = ZoneInfo("America/Detroit")
except Exception:
    DETROIT = None

SCHEMA = "forge-line.endpoint/v1"
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REF = ROOT / "reference" / "route-stops.json"
EVENTS = DATA / "vehicles" / "events.jsonl"
EVENTS_DIR = DATA / "vehicles"
EVENTS_MONOLITH = EVENTS
# GitHub rejects any file >= 100 MB. The event stream grows without bound,
# so it lives in monthly shard parts kept well under that limit.
ROTATE_BYTES = 50 * 1024 * 1024   # split the monolith above this size
PART_BYTES = 40 * 1024 * 1024     # roll to a new shard part above this size
SEGMENTS = DATA / "timing" / "segments.json"
WATERMARK = DATA / "timing" / "watermark.json"

EARTH_R = 6371000.0
SWITCH_CONFIRM_M = 120.0   # candidate stop within this distance -> switch candidate
ANCHOR_BEHIND_M = 250.0    # ...or anchor this far behind -> confirm immediately
MAX_GAP_S = 1800           # poll gap beyond this: reset anchor, emit nothing
MAX_SPEED_MS = 31.3        # 70 mph: above this is an impossible jump
FROZEN_POLLS = 6           # identical rounded position this many polls -> frozen
OFF_ROUTE_M = 800.0
QUIET_POLLS = 3            # missed polls before endpoint.quiet
MIN_CELL_N = 3
MIN_DWELL_N = 3            # dwell observations needed for a per-pair median
MIN_SEGMENT_S = 30.0       # floor for dwell-corrected cell statistics
ROSA_PARKS_STOP = "531"    # Rosa Parks Transit Center - Deboarding

# Terminus stops per route, for the dwell correction. Derived from DDOT
# GTFS feed S1000182 (the feed the production timetable and
# reference/route-stops.json were built from): a stop is a terminus for a
# route if it is the first or last stop of any trip of that route in
# stop_times.txt. 98 distinct stops across 37 routes. Validated
# 2026-10-14: with stop 531 counted for every route, this set reproduces
# the origin-contamination measurements of record (2,121 cells, mean
# 805.5 s vs 321.9 s for all other cells). Regenerate when the reference
# feed changes.
ROUTE_TERMINI = {
    "1": {"531", "4080", "5225"},
    "2": {"531", "2086", "3604", "3659", "5230"},
    "3": {"152", "2261"},
    "4": {"5666", "5675"},
    "5": {"405", "531", "5248"},
    "6": {"797", "3375", "5482"},
    "7": {"6", "329"},
    "8": {"9", "331", "3755"},
    "9": {"425", "531", "3072"},
    "10": {"1423", "2086", "2116", "3659"},
    "11": {"69", "87", "1606"},
    "12": {"687", "800", "5664"},
    "13": {"405", "813", "814"},
    "15": {"2", "70", "798", "3008", "3009"},
    "16": {"6", "97", "447"},
    "17": {"152", "330", "5047"},
    "18": {"531", "1534", "1775", "5228"},
    "19": {"531", "1818", "1839", "5218"},
    "23": {"531", "5136", "5229"},
    "27": {"531", "2515", "2538", "5222"},
    "29": {"531", "1695", "5224"},
    "30": {"2908", "5668"},
    "31": {"531", "4279", "5226"},
    "32": {"6", "505", "542", "4279"},
    "38": {"509", "516", "541", "668", "1248", "3735", "3744"},
    "39": {"2", "70", "283", "297"},
    "40": {"304", "531", "5484"},
    "41": {"2908", "4074", "4086", "4161", "4168"},
    "42": {"2", "1128", "5259"},
    "43": {"2", "70", "2515", "4188"},
    "46": {"2086", "4471"},
    "47": {"562", "1116", "1131", "1134", "2012", "4430", "4530"},
    "52": {"531", "5219", "5319", "5352"},
    "54": {"1588", "1589", "5663"},
    "60": {"740", "741", "2086"},
    "67": {"330", "531", "5223"},
    "68": {"813", "814", "836", "2089"},
}


def haversine(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R * math.asin(math.sqrt(a))


def parse_ts(s):
    # telemetry/v1 stamps are UTC with Z; vehicle updated_at is Detroit wall time
    if s.endswith("Z"):
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    dt = datetime.fromisoformat(s)
    if DETROIT:
        return dt.replace(tzinfo=DETROIT)
    return dt.replace(tzinfo=timezone.utc)


def bucket(dt):
    local = dt.astimezone(DETROIT) if DETROIT else dt
    dow = local.weekday()  # 0=Mon
    if dow == 5:
        return "sat"
    if dow == 6:
        return "sun"
    h = local.hour
    if 6 <= h < 9:
        return "wkd_am"
    if 9 <= h < 15:
        return "wkd_mid"
    if 15 <= h < 19:
        return "wkd_pm"
    if 19 <= h < 24:
        return "wkd_eve"
    return "wkd_night"


def nearest_stop(lat, lon, stops):
    best, best_d = None, float("inf")
    for s in stops:
        d = haversine(lat, lon, s["lat"], s["lon"])
        if d < best_d:
            best, best_d = s, d
    return best, best_d


def shard_sort_key(path):
    """events-YYYY-MM.jsonl is part 1; events-YYYY-MM-NN.jsonl is part NN."""
    stem = path.name[len("events-"):-len(".jsonl")]
    parts = stem.split("-")
    if len(parts) >= 4:
        return ("-".join(parts[:3]), int(parts[3]))
    return (stem, 1)


def event_files():
    """All event-stream files in stream order: the legacy monolith first
    (whenever it exists it holds the oldest events), then monthly shards
    in (month, part) order."""
    files = []
    if EVENTS_MONOLITH.exists():
        files.append(EVENTS_MONOLITH)
    shards = [p for p in EVENTS_DIR.glob("events-*.jsonl")
              if p != EVENTS_MONOLITH]
    files.extend(sorted(shards, key=shard_sort_key))
    return files


def record_month(line):
    """YYYY-MM bucket for an event record, from its recorded_at stamp."""
    try:
        rec = json.loads(line)
    except json.JSONDecodeError:
        return None
    for field in ("recorded_at", "source_poll", "received_at"):
        ts = rec.get(field) or ""
        if len(ts) >= 7 and ts[4] == "-":
            return ts[:7]
    return None


def _part_path(month, part):
    if part <= 1:
        return EVENTS_DIR / f"events-{month}.jsonl"
    return EVENTS_DIR / f"events-{month}-{part:02d}.jsonl"


def rotate_events():
    """Split an oversized events.jsonl monolith into monthly shard parts.

    Lossless and order-preserving: lines are streamed in file order into
    events-YYYY-MM.jsonl, events-YYYY-MM-02.jsonl, ..., rolling to the
    next part before any part exceeds PART_BYTES. A record whose month
    cannot be determined stays in the current part. The monolith is
    removed only after every line has been written to a shard.
    """
    if not EVENTS_MONOLITH.exists():
        return
    if EVENTS_MONOLITH.stat().st_size <= ROTATE_BYTES:
        return
    handles = {}
    state = {"month": None, "part": 0, "path": None, "size": 0}

    def open_part(month, part):
        path = _part_path(month, part)
        if path not in handles:
            handles[path] = open(path, "a")
        state.update(month=month, part=part, path=path,
                     size=path.stat().st_size)
        return handles[path]

    def resume_part(month):
        # continue this month's last existing part (shards may already
        # exist next to a regrown monolith)
        part = 1
        while _part_path(month, part + 1).exists():
            part += 1
        return open_part(month, part)

    try:
        with open(EVENTS_MONOLITH) as src:
            for line in src:
                if not line.strip():
                    continue
                month = record_month(line) or state["month"] or "unknown"
                need = len(line.encode())
                if state["path"] is None or month != state["month"]:
                    resume_part(month)
                elif state["size"] + need > PART_BYTES:
                    open_part(month, state["part"] + 1)
                handles[state["path"]].write(line)
                state["size"] += need
    finally:
        for fh in handles.values():
            fh.close()
    EVENTS_MONOLITH.unlink()


class EventWriter:
    """Append handle for the event stream: writes to the current month's
    latest shard part, rolling to a new part before PART_BYTES."""

    def __init__(self, month):
        self.month = month
        self.part = 1
        while _part_path(month, self.part + 1).exists():
            self.part += 1
        self.path = _part_path(month, self.part)
        self.size = self.path.stat().st_size if self.path.exists() else 0
        self.fh = open(self.path, "a")

    def write(self, s):
        need = len(s.encode())
        if self.size > 0 and self.size + need > PART_BYTES:
            self.fh.close()
            self.part += 1
            self.path = _part_path(self.month, self.part)
            self.size = 0
            self.fh = open(self.path, "a")
        self.fh.write(s)
        self.size += need

    def close(self):
        self.fh.close()


def write_latest():
    """data/latest.json: the single most recent poll, for instant page paint
    and as a lightweight "last known positions" fallback. Rebuilt from the
    newest raw poll file on every run (the old log_poll.py wrote this inline;
    build_timing.py owns it now that polling moved to the Netlify logger)."""
    newest = None
    for p in sorted(DATA.glob("20*.jsonl")):
        newest = p
    if newest is None:
        return
    last = None
    with open(newest) as fh:
        for line in fh:
            line = line.strip()
            if line:
                last = line
    if last:
        with open(DATA / "latest.json", "w") as out:
            out.write(last if last.endswith("\n") else last + "\n")


def main():
    with open(REF) as f:
        route_stops = json.load(f)
    all_stops = [s for stops in route_stops.values() for s in stops]

    wm = {"days": {}, "vehicles": {}}
    if WATERMARK.exists():
        try:
            wm = json.loads(WATERMARK.read_text())
        except json.JSONDecodeError:
            pass
    days_done = wm.setdefault("days", {})
    vstate = wm.setdefault("vehicles", {})

    EVENTS.parent.mkdir(parents=True, exist_ok=True)
    SEGMENTS.parent.mkdir(parents=True, exist_ok=True)
    rotate_events()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    evf = EventWriter(now_utc[:7])

    def emit(event, vid, route, recorded_at, polled_at, **fields):
        emit.seq += 1
        rec = {"schema": SCHEMA, "event": event,
               "event_id": f"{polled_at}/{vid}/{event}/{emit.seq}",
               "recorded_at": recorded_at, "vehicle_id": vid,
               "route_id": route, "source_poll": polled_at}
        rec.update(fields)
        evf.write(json.dumps(rec, separators=(",", ":")) + "\n")
    emit.seq = 0

    new_events = 0

    for dayfile in sorted(DATA.glob("*.jsonl")):
        if dayfile.name in ("latest.json",):
            continue
        lines = dayfile.read_text().splitlines()
        start = days_done.get(dayfile.name, 0)
        for line in lines[start:]:
            line = line.strip()
            if not line:
                continue
            try:
                poll = json.loads(line)
            except json.JSONDecodeError:
                continue
            polled_at = poll.get("polled_at", "")
            try:
                pts = parse_ts(polled_at)
            except ValueError:
                continue
            seen = set()
            for v in poll.get("vehicles", []):
                vid = str(v.get("vehicle_id", ""))
                if not vid:
                    continue
                seen.add(vid)
                route = str(v.get("route_id", ""))
                lat, lon = v["lat"], v["lon"]
                obs_at = None
                if v.get("updated_at"):
                    try:
                        obs_at = parse_ts(v["updated_at"]).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except ValueError:
                        pass
                st = vstate.get(vid)
                if st is None:
                    st = {"anchor": None, "anchor_t": None, "cand": None,
                          "dwell_s": 0.0, "frozen_n": 0, "last_pos": None,
                          "quiet_n": 0, "quiet_emitted": False,
                          "offroute": False, "frozen_emitted": False}
                    vstate[vid] = st
                    emit("endpoint.enrolled", vid, route, now_utc, polled_at,
                         observed_at=obs_at, received_at=polled_at)
                    new_events += 1
                # assignment change
                if st.get("route") != route:
                    if st.get("route"):
                        emit("assignment.changed", vid, route, now_utc, polled_at,
                             old_route=st["route"], new_route=route,
                             observed_at=obs_at, received_at=polled_at)
                        new_events += 1
                    st["route"] = route
                    st["anchor"] = None  # re-anchor on new route
                    st["cand"] = None
                st["quiet_n"] = 0
                st["quiet_emitted"] = False

                stops = route_stops.get(route)
                ghost = False
                if not stops:
                    stops = all_stops
                    ghost = True
                    if not st.get("ghost_emitted"):
                        emit("anomaly.flagged", vid, route, now_utc, polled_at,
                             subtype="ghost_route", evidence={"route_id": route},
                             observed_at=obs_at, received_at=polled_at)
                        new_events += 1
                        st["ghost_emitted"] = True
                ns, nd = nearest_stop(lat, lon, stops)

                # impossible jump check (needs previous position + time)
                dt = None
                if st.get("last_t"):
                    try:
                        dt = (pts - parse_ts(st["last_t"])).total_seconds()
                    except ValueError:
                        dt = None
                if st["last_pos"] and dt and dt > 0:
                    d = haversine(st["last_pos"][0], st["last_pos"][1], lat, lon)
                    if d / dt > MAX_SPEED_MS:
                        emit("anomaly.flagged", vid, route, now_utc, polled_at,
                             subtype="impossible_jump",
                             evidence={"implied_mph": round(d / dt * 2.237, 1),
                                       "gap_s": round(dt, 1)},
                             observed_at=obs_at, received_at=polled_at,
                             at={"lat": lat, "lon": lon})
                        new_events += 1
                        st["last_pos"] = [lat, lon]
                        st["last_t"] = polled_at
                        continue  # bad GPS must not move the anchor

                # frozen check
                rpos = (round(lat, 4), round(lon, 4))
                if st["last_pos"] and (round(st["last_pos"][0], 4), round(st["last_pos"][1], 4)) == rpos:
                    st["frozen_n"] = st.get("frozen_n", 0) + 1
                else:
                    st["frozen_n"] = 0
                    st["frozen_emitted"] = False
                if st["frozen_n"] >= FROZEN_POLLS and not st["frozen_emitted"]:
                    emit("anomaly.flagged", vid, route, now_utc, polled_at,
                         subtype="frozen",
                         evidence={"polls": st["frozen_n"]},
                         observed_at=obs_at, received_at=polled_at,
                         at={"lat": lat, "lon": lon})
                    new_events += 1
                    st["frozen_emitted"] = True

                # off-route check
                if nd > OFF_ROUTE_M:
                    if not st["offroute"]:
                        emit("anomaly.flagged", vid, route, now_utc, polled_at,
                             subtype="off_route",
                             evidence={"nearest_stop_m": round(nd, 1)},
                             observed_at=obs_at, received_at=polled_at,
                             at={"lat": lat, "lon": lon})
                        new_events += 1
                        st["offroute"] = True
                    st["last_pos"] = [lat, lon]
                    st["last_t"] = polled_at
                    continue
                st["offroute"] = False

                anchor = st["anchor"]
                if anchor is None:
                    # (re)anchor on the nearest stop
                    st["anchor"] = ns["id"]
                    st["anchor_t"] = polled_at
                    st["cand"] = None
                    st["dwell_s"] = 0.0
                elif ns["id"] == anchor:
                    st["cand"] = None
                    # dwell accumulation
                    moved = haversine(st["last_pos"][0], st["last_pos"][1], lat, lon) if st["last_pos"] else 0
                    speed = v.get("speed_mph")
                    if (speed is not None and speed < 5) or moved < 30:
                        inc = min(dt if dt and dt > 0 else 0, 600)
                        st["dwell_s"] = st.get("dwell_s", 0) + inc
                else:
                    # candidate switch with debounce
                    anchor_stop = next((s for s in stops if s["id"] == anchor), None)
                    anchor_d = haversine(anchor_stop["lat"], anchor_stop["lon"], lat, lon) if anchor_stop else 1e9
                    confirmed = (st["cand"] == ns["id"]) or (nd < SWITCH_CONFIRM_M and anchor_d > ANCHOR_BEHIND_M)
                    if confirmed:
                        try:
                            secs = (pts - parse_ts(st["anchor_t"])).total_seconds()
                        except ValueError:
                            secs = -1
                        # Ghost routes (unknown route_id, matched against the
                        # global stop list) must not produce timing cells: the
                        # attribution is unreliable. The anchor still moves so
                        # detector state stays sane.
                        if not ghost and 0 < secs <= MAX_GAP_S:
                            seg_dist = haversine(anchor_stop["lat"], anchor_stop["lon"], ns["lat"], ns["lon"]) if anchor_stop else None
                            emit("segment.traversed", vid, route, now_utc, polled_at,
                                 observed_at=obs_at, received_at=polled_at,
                                 from_stop=anchor, to_stop=ns["id"],
                                 seconds=round(secs, 1),
                                 distance_m=round(seg_dist, 1) if seg_dist else None,
                                 bucket=bucket(pts),
                                 at={"lat": ns["lat"], "lon": ns["lon"],
                                     "route_id": route, "nearest_stop": ns["id"],
                                     "stop_distance_m": round(nd, 1),
                                     "segment": [anchor, ns["id"]]})
                            new_events += 1
                            if st.get("dwell_s", 0) >= 60:
                                emit("dwell.observed", vid, route, now_utc, polled_at,
                                     observed_at=obs_at, received_at=polled_at,
                                     stop=anchor, dwell_seconds=round(st["dwell_s"], 1))
                                new_events += 1
                        # move anchor regardless (gap resets silently per honesty rules)
                        st["anchor"] = ns["id"]
                        st["anchor_t"] = polled_at
                        st["cand"] = None
                        st["dwell_s"] = 0.0
                    else:
                        st["cand"] = ns["id"]
                st["last_pos"] = [lat, lon]
                st["last_t"] = polled_at

            # quiet detection for vehicles missing from this poll
            for vid, st in vstate.items():
                if vid in seen:
                    continue
                st["quiet_n"] = st.get("quiet_n", 0) + 1
                if st["quiet_n"] >= QUIET_POLLS and not st.get("quiet_emitted"):
                    emit("endpoint.quiet", vid, st.get("route", ""), now_utc, polled_at,
                         last_seen=st.get("last_t"), missed_polls=st["quiet_n"])
                    new_events += 1
                    st["quiet_emitted"] = True
        days_done[dayfile.name] = len(lines)
    evf.close()

    # timing model: aggregate segment.traversed events
    cells = defaultdict(list)
    dists = {}
    dwell = defaultdict(list)   # (route_id, stop) -> [dwell_seconds]
    for events_path in event_files():
        with open(events_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("event") == "dwell.observed":
                    dwell[(str(e["route_id"]), str(e["stop"]))].append(
                        float(e["dwell_seconds"]))
                    continue
                if e.get("event") != "segment.traversed":
                    continue
                key = (e["route_id"], e["from_stop"], e["to_stop"], e["bucket"])
                cells[key].append(e["seconds"])
                if e.get("distance_m"):
                    dists[key] = e["distance_m"]

    # Dwell correction (see module docstring). Pair medians need
    # MIN_DWELL_N observations; the fallbacks are the pooled median at
    # stop 531 and the pooled median over terminus/531 dwell, both
    # computed from the same event stream (600 s and 590 s on the
    # October 2026 stream at decision time).
    def _median(vals):
        s = sorted(vals)
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    pair_dwell = {k: _median(v) for k, v in dwell.items()
                  if len(v) >= MIN_DWELL_N}
    term_pool = [d for (r, s), v in dwell.items()
                 if s == ROSA_PARKS_STOP or s in ROUTE_TERMINI.get(r, ())
                 for d in v]
    pool_531 = [d for (r, s), v in dwell.items() if s == ROSA_PARKS_STOP
                for d in v]
    pooled_terminus_median = _median(term_pool) if term_pool else 600.0
    pooled_531_median = (_median(pool_531) if pool_531
                         else pooled_terminus_median)

    def dwell_correction(route, from_stop):
        """Median layover dwell (seconds) embedded in segment times that
        start at from_stop, or None when from_stop is not an origin where
        layover accrues (not a terminus for the route, not stop 531)."""
        if (from_stop != ROSA_PARKS_STOP
                and from_stop not in ROUTE_TERMINI.get(route, ())):
            return None
        med = pair_dwell.get((route, from_stop))
        if med is not None:
            return med
        if from_stop == ROSA_PARKS_STOP:
            return pooled_531_median
        return pooled_terminus_median

    model = {}
    for (route, fs, ts, b), vals in cells.items():
        if len(vals) < MIN_CELL_N:
            continue
        vals.sort()
        n = len(vals)
        p50 = vals[n // 2]
        p90 = vals[min(n - 1, int(n * 0.9))]
        rec = {
            "n": n,
            "mean_s": round(sum(vals) / n, 1),
            "p50_s": round(p50, 1),
            "p90_s": round(p90, 1),
            "dist_m": dists.get((route, fs, ts, b)),
        }
        corr = dwell_correction(route, fs)
        if corr is not None:
            for field in ("mean_s", "p50_s", "p90_s"):
                rec[field] = round(max(rec[field] - corr, MIN_SEGMENT_S), 1)
        model[f"{route}|{fs}|{ts}|{b}"] = rec
    with open(SEGMENTS, "w") as f:
        json.dump({"schema": SCHEMA,
                   "built_at": now_utc,
                   "cells": len(model),
                   "segments": model}, f, separators=(",", ":"))
        f.write("\n")
    with open(WATERMARK, "w") as f:
        json.dump(wm, f, separators=(",", ":"))
        f.write("\n")
    write_latest()
    print(f"processed polls; new events: {new_events}; timing cells: {len(model)}")


if __name__ == "__main__":
    main()
