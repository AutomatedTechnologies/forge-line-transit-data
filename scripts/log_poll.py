#!/usr/bin/env python3
"""Poll the Forge Line live feed and append one standardized telemetry record.

Repointed 2026-10-15: this fallback poller used to fetch the Netlify
proxy's /api/vehicles. The proxy site is quota-paused and the page's
feed is now the static JSON at FEED_URL (same envelope), so the
fallback polls that. scripts/log_static.py is the primary logger;
this script remains the build-timing workflow's schedule-path
fallback, as before.

Output: data/YYYY-MM-DD.jsonl (UTC date), one JSON object per line, one line
per successful poll. Failed polls are never logged as empty records -- a gap
in the 5-minute cadence means that poll failed or was skipped.

Record format: forge-line.telemetry/v1 (see SCHEMA.md).

Endpoint registry: data/vehicles/registry.json is a persistent per-vehicle
inventory, updated every successful poll. Each bus is a managed endpoint:
keyed by vehicle_id, with identity, route assignment, last check-in state,
and check-in history counters. Vehicles absent from a poll keep their record
and are marked "quiet" -- the inventory is cumulative, like any endpoint
manager. This registry (plus the JSONL check-in history) is the foundation
for empirical point-to-point timing.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

FEED_URL = "https://detroitautomationacademy.com/live-data/vehicles.json"
SOURCE_HOST = "detroitautomationacademy.com/live-data"
SCHEMA = "forge-line.telemetry/v1"
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REGISTRY = DATA / "vehicles" / "registry.json"


def fetch():
    req = urllib.request.Request(
        FEED_URL,
        headers={"User-Agent": "forge-line-transit-data-logger/1.0"},
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        if resp.status != 200:
            raise RuntimeError(f"feed HTTP {resp.status}")
        return json.load(resp)


def clean_vehicle(v):
    """Keep the live-API vehicle shape exactly; drop records with bad coords."""
    try:
        lat = float(v["lat"])
        lon = float(v["lon"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    out = {
        "agency": str(v.get("agency", "ddot")),
        "route_id": str(v.get("route_id", "")),
        "vehicle_id": str(v.get("vehicle_id", "")),
        "lat": round(lat, 6),
        "lon": round(lon, 6),
    }
    if v.get("bearing") is not None:
        out["bearing"] = v["bearing"]
    if v.get("speed_mph") is not None:
        out["speed_mph"] = v["speed_mph"]
    if v.get("destination") is not None:
        out["destination"] = str(v["destination"])
    if v.get("updated_at"):
        out["updated_at"] = v["updated_at"]
    return out


def update_registry(clean, stamp):
    """Maintain the persistent vehicle endpoint registry.

    Read-modify-write per poll: new endpoints are enrolled, known endpoints
    get fresh check-in state, and endpoints absent from this poll are marked
    quiet (record retained). Route reassignments are counted -- a bus moving
    routes is an assignment change, worth knowing about.
    """
    REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    reg = {"vehicles": {}}
    if REGISTRY.exists():
        try:
            reg = json.loads(REGISTRY.read_text())
        except json.JSONDecodeError:
            reg = {"vehicles": {}}
    vehicles = reg.setdefault("vehicles", {})
    seen = set()
    for v in clean:
        vid = v["vehicle_id"]
        if not vid:
            continue
        seen.add(vid)
        rec = vehicles.get(vid)
        if rec is None:
            rec = {
                "vehicle_id": vid,
                "agency": v.get("agency", "ddot"),
                "first_seen": stamp,
                "checkins": 0,
                "route_changes": 0,
            }
            vehicles[vid] = rec
        new_route = v.get("route_id", "")
        old_route = rec.get("route_id", "")
        if new_route != old_route:
            if old_route:
                rec["route_changes"] = rec.get("route_changes", 0) + 1
                rec["last_route_change"] = stamp
            rec["route_id"] = new_route
        rec["agency"] = v.get("agency", rec.get("agency", "ddot"))
        if v.get("destination") is not None:
            rec["destination"] = v["destination"]
        rec["last_seen"] = stamp
        rec["checkins"] = rec.get("checkins", 0) + 1
        rec["last_lat"] = v["lat"]
        rec["last_lon"] = v["lon"]
        if v.get("speed_mph") is not None:
            rec["last_speed_mph"] = v["speed_mph"]
        if v.get("bearing") is not None:
            rec["last_bearing"] = v["bearing"]
        if v.get("updated_at"):
            rec["last_updated_at"] = v["updated_at"]
        rec["checkin_state"] = "active"
    for vid, rec in vehicles.items():
        if vid not in seen:
            rec["checkin_state"] = "quiet"
    reg["schema"] = SCHEMA
    reg["registry_updated_at"] = stamp
    reg["vehicle_count"] = len(seen)
    reg["endpoints_total"] = len(vehicles)
    with open(REGISTRY, "w") as f:
        json.dump(reg, f, separators=(",", ":"))
        f.write("\n")
    return len(seen), len(vehicles)


def main():
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = fetch()
    vehicles = payload.get("vehicles")
    if not isinstance(vehicles, list):
        raise RuntimeError("feed response missing 'vehicles' list")
    clean = [c for c in (clean_vehicle(v) for v in vehicles) if c]

    sources = {}
    for name, s in (payload.get("sources") or {}).items():
        entry = {"ok": bool(s.get("ok"))}
        if "vehicles" in s:
            entry["vehicles"] = s["vehicles"]
        if s.get("reason"):
            entry["reason"] = s["reason"]
        sources[name] = entry

    record = {
        "schema": SCHEMA,
        "record_id": stamp,
        "polled_at": stamp,
        "proxy": SOURCE_HOST,
        "vehicle_count": len(clean),
        "vehicles": clean,
        "sources": sources,
    }

    DATA.mkdir(exist_ok=True)
    dayfile = DATA / (now.strftime("%Y-%m-%d") + ".jsonl")
    with open(dayfile, "a") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")
    # data/latest.json: the single most recent poll, for instant page paint
    # and as a lightweight "last known positions" fallback. Small enough to
    # fetch on every page load (~one poll, not the whole day).
    with open(DATA / "latest.json", "w") as f:
        json.dump(record, f, separators=(",", ":"))
        f.write("\n")
    active, total = update_registry(clean, stamp)
    update_index()
    print(f"appended poll {stamp}: {len(clean)} vehicles -> {dayfile.name}; "
          f"registry: {active} active, {total} endpoints")


def update_index():
    days = []
    for p in sorted(DATA.glob("*.jsonl")):
        polls = 0
        veh = 0
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                polls += 1
                veh += int(r.get("vehicle_count", 0))
        days.append({
            "date": p.stem,
            "polls": polls,
            "vehicle_records": veh,
            "path": f"data/{p.name}",
        })
    index = {
        "schema": SCHEMA,
        "index_updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": days,
    }
    with open(DATA / "index.json", "w") as f:
        json.dump(index, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
