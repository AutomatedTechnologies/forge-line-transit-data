#!/usr/bin/env python3
"""Poll the static live feed and append one standardized telemetry record.

Repointed 2026-10-15. The primary logger used to be the Netlify scheduled
function log-telemetry.js, which polled the DDOT/SMART proxy legs
in-process and appended to this repo via the Contents API. That site is
quota-paused (HTTP 503 usage_exceeded), which froze the stream at
2026-10-15T19:20:17.803Z. The page's feed now comes from a static JSON
file built by the site repo's Actions workflow, so this logger consumes
that same file -- publicly, no API key:

    https://detroitautomationacademy.com/live-data/vehicles.json

Semantics are a faithful port of the Netlify function (which itself
mirrored log_poll.py's cleaner):

  - One forge-line.telemetry/v1 record per upstream snapshot, appended
    to the UTC-hour shard data/YYYY-MM-DDTHH.jsonl.
  - record_id / polled_at are the snapshot's own generated_at (the time
    the feed build fetched the upstreams) -- the same meaning the
    Netlify function's invocation time carried, since it fetched
    in-process. This script claims no finer resolution than its input.
  - Vehicles cleaned exactly as before (6-decimal coords, bad-coord
    records dropped, optional fields only when present).
  - Sources trimmed by the original rule: {ok: true, vehicles: n} or
    {ok: false, reason}. All three sources the static file carries
    (ddot, smart, qline) are trimmed; per-agency counts are the
    payload's pre-clean counts, as the legs' array lengths were.
  - Nothing is logged when the fetch fails, the payload is malformed,
    or no source reports ok -- a failed poll is a gap, never an empty
    record (the original skip rule, over the new source set).
  - The same snapshot is never logged twice: if the target shard's
    last record already carries this generated_at, the run is a no-op.
    Re-polling an unchanged static file is not a new upstream poll.

Inputs have changed with the product: SMART was removed from Live
Transit Detroit on 2026-10-15, so no SMART vehicles arrive (the trimmed
smart source records the removal state); QLINE vehicles now enter the
stream (agency "qline") and ride build_timing.py's established
ghost-route path, exactly as SMART vehicles did -- the timing
reference is DDOT-only, so they produce enrollment/quiet events and
no timing cells.

Output signalling: writes logged=true/false, shard=<repo-relative
path>, and record_id=<generated_at> to $GITHUB_OUTPUT when present.
Exit code is 0 for every expected skip; a genuine bug should traceback
and fail the run loud.
"""
import json
import math
import os
import sys
import urllib.request
from pathlib import Path

FEED_URL = os.environ.get(
    "TELEMETRY_FEED_URL",
    "https://detroitautomationacademy.com/live-data/vehicles.json")
SCHEMA = "forge-line.telemetry/v1"
SOURCE_HOST = "detroitautomationacademy.com/live-data"
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def emit_output(name, value):
    print(f"{name}={value}")
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as f:
            f.write(f"{name}={value}\n")


def fetch():
    req = urllib.request.Request(
        FEED_URL,
        headers={"User-Agent": "forge-line-transit-data-logger/1.0"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        status = getattr(resp, "status", None)
        if status is not None and status != 200:
            raise RuntimeError(f"feed HTTP {status}")
        return json.load(resp)


def _round6(x):
    # JS Math.round(v * 1e6) / 1e6 (the Netlify cleaner's rounding).
    return math.floor(x * 1e6 + 0.5) / 1e6


def clean_vehicle(v):
    """Port of the Netlify function's cleanVehicle: live-API shape,
    6-decimal coords, drop records with bad coordinates."""
    lat_raw, lon_raw = v.get("lat"), v.get("lon")
    if lat_raw is None or lon_raw is None or lat_raw == "" or lon_raw == "":
        return None
    try:
        lat, lon = float(lat_raw), float(lon_raw)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lat) and math.isfinite(lon)):
        return None
    if lat < -90 or lat > 90 or lon < -180 or lon > 180:
        return None
    out = {
        "agency": str(v.get("agency") or "ddot"),
        "route_id": str(v.get("route_id") or ""),
        "vehicle_id": str(v.get("vehicle_id") or ""),
        "lat": _round6(lat),
        "lon": _round6(lon),
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


def trim_source(state, count):
    """Port of the Netlify function's trimSource."""
    if state and state.get("ok"):
        return {"ok": True, "vehicles": count}
    return {"ok": False, "reason": (state or {}).get("reason") or "upstream error"}


def last_record_id(shard):
    """record_id of the shard's last non-empty line, or None."""
    try:
        with open(shard, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return None
            back = min(size, 262144)
            f.seek(size - back)
            tail = f.read().decode("utf-8", "replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        line = line.strip()
        if line:
            try:
                return json.loads(line).get("record_id")
            except json.JSONDecodeError:
                return None
    return None


def main():
    try:
        payload = fetch()
    except Exception as e:
        print(f"log-static: feed fetch failed ({e}); not logging (gap)")
        emit_output("logged", "false")
        return 0
    generated_at = payload.get("generated_at")
    vehicles_in = payload.get("vehicles")
    if not generated_at or not isinstance(vehicles_in, list):
        print("log-static: payload missing generated_at/vehicles; not logging (gap)")
        emit_output("logged", "false")
        return 0

    raw_sources = payload.get("sources") or {}
    counts = {}
    for v in vehicles_in:
        agency = str(v.get("agency") or "ddot")
        counts[agency] = counts.get(agency, 0) + 1
    sources = {}
    any_ok = False
    for name in ("ddot", "smart", "qline"):
        if name in raw_sources:
            sources[name] = trim_source(raw_sources[name], counts.get(name, 0))
            any_ok = any_ok or bool(raw_sources[name].get("ok"))
    if not any_ok:
        # The original skip rule: upstream failed for every agency.
        print("log-static: no source reports ok; not logging (gap)")
        emit_output("logged", "false")
        return 0

    clean = [c for c in (clean_vehicle(v) for v in vehicles_in) if c]
    record = {
        "schema": SCHEMA,
        "record_id": generated_at,
        "polled_at": generated_at,
        "proxy": SOURCE_HOST,
        "vehicle_count": len(clean),
        "vehicles": clean,
        "sources": sources,
    }

    shard = DATA / (generated_at[:13] + ".jsonl")
    if shard.exists() and last_record_id(shard) == generated_at:
        print(f"log-static: snapshot {generated_at} already logged in {shard.name}; skipping")
        emit_output("logged", "false")
        return 0

    DATA.mkdir(parents=True, exist_ok=True)
    with open(shard, "a") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")
    print(f"log-static: appended {len(clean)} vehicles "
          f"(snapshot {generated_at}) to {shard.name}")
    emit_output("logged", "true")
    emit_output("shard", f"data/{shard.name}")
    emit_output("record_id", generated_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())
