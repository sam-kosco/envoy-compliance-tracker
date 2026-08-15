"""
PSA Overnight Location (RON) Forecast
=====================================
For every tail on the PSA tracker roster, asks FlightAware AeroAPI for the
aircraft's flights in a window around now and works out where it will spend
the night: the destination of its LAST arrival before the cutoff
(04:00 Eastern tomorrow). A tail with no remaining flights overnights
wherever its most recent completed flight landed.

Writes psa_ron_forecast.json (committed by the workflow):
  {
    "generated": "...UTC...",
    "program": "PSA",
    "night_of": "YYYY-MM-DD",          # Eastern date the forecast covers
    "tails": [
      {"tail": "N500AE", "overnight": "CLT", "confidence": "scheduled",
       "last_arrival": "...UTC...", "flights_seen": 4}, ...
    ],
    "by_airport": {"CLT": ["N500AE", ...], ...},
    "unknown": ["N123AB", ...]          # no flight data found
  }

confidence: "completed"  — last flight already landed (position known)
            "in_air"     — currently flying; destination per estimate
            "scheduled"  — future departure(s); destination per schedule
            "last_known" — nothing flying/scheduled in window; using the
                           most recent arrival before now
Scheduling: crons at 20:07 and 21:07 UTC; on scheduled runs the guard only
proceeds when it is 16:xx (4 PM) in America/New_York, so the forecast runs
at 4 PM Eastern year-round. Manual dispatch skips the guard.

Env: AERO_API_KEY (GitHub secret). Paces itself on HTTP 429.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests

BASE = "https://aeroapi.flightaware.com/aeroapi"
ET = ZoneInfo("America/New_York")


def guard_4pm_eastern():
    if os.environ.get("GITHUB_EVENT_NAME") != "schedule":
        return
    hour = datetime.now(ET).hour
    if hour != 16:
        print(f"Not 4 PM Eastern (hour={hour}) — skipping this scheduled run.")
        sys.exit(0)


def arrival_dt(flight):
    """Best-available arrival time for a flight, as aware datetime, or None."""
    for k in ("actual_in", "actual_on", "estimated_in", "estimated_on", "scheduled_in"):
        v = flight.get(k)
        if v:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
    return None


def dest_code(flight):
    d = flight.get("destination") or {}
    return d.get("code_iata") or d.get("code") or None


def fetch_flights(session, tail, start, end):
    """GET /flights/{registration} with 429 backoff. Returns list or None."""
    url = f"{BASE}/flights/{tail}"
    params = {"ident_type": "registration", "start": start, "end": end}
    for attempt in range(5):
        r = session.get(url, params=params, timeout=30)
        if r.status_code == 429:
            wait = 65
            print(f"  {tail}: rate limited — sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code in (400, 404):
            return None  # registration unknown to FlightAware
        r.raise_for_status()
        return r.json().get("flights", [])
    print(f"  {tail}: still rate limited after retries")
    return None


def main():
    guard_4pm_eastern()

    key = os.environ["AERO_API_KEY"]
    session = requests.Session()
    session.headers["x-apikey"] = key

    planes = json.load(open("psa_data.json"))["planes"]
    tails = [p["tail"] for p in planes]
    print(f"Forecasting overnight locations for {len(tails)} PSA tails")

    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # "Tonight" ends at 04:00 Eastern tomorrow — arrivals after that belong
    # to tomorrow's flying day.
    tonight = datetime.now(ET).date()
    cutoff = datetime.combine(tonight + timedelta(days=1),
                              datetime.min.time(), ET).replace(hour=4).astimezone(timezone.utc)

    results, unknown = [], []
    for i, tail in enumerate(tails, 1):
        flights = fetch_flights(session, tail, start, end)
        if not flights:
            unknown.append(tail)
            print(f"  [{i}/{len(tails)}] {tail}: no flight data")
            time.sleep(0.35)
            continue

        # Consider non-cancelled flights with a usable arrival before cutoff
        candidates = []
        for f in flights:
            if f.get("cancelled"):
                continue
            dt = arrival_dt(f)
            code = dest_code(f)
            if dt and code and dt <= cutoff:
                candidates.append((dt, f, code))

        if not candidates:
            unknown.append(tail)
            print(f"  [{i}/{len(tails)}] {tail}: no arrivals before cutoff")
            time.sleep(0.35)
            continue

        dt, f, code = max(candidates, key=lambda c: c[0])
        if f.get("actual_in") or f.get("actual_on"):
            conf = "completed" if dt <= now else "in_air"
        elif f.get("actual_off"):
            conf = "in_air"
        elif dt > now:
            conf = "scheduled"
        else:
            conf = "last_known"

        results.append({
            "tail": tail,
            "overnight": code,
            "confidence": conf,
            "last_arrival": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "flights_seen": len(flights),
        })
        print(f"  [{i}/{len(tails)}] {tail}: {code} ({conf})")
        time.sleep(0.35)  # gentle pacing under per-minute rate limits

    by_airport = {}
    for r in results:
        by_airport.setdefault(r["overnight"], []).append(r["tail"])
    by_airport = {k: sorted(v) for k, v in sorted(by_airport.items())}

    out = {
        "generated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "program": "PSA",
        "night_of": tonight.isoformat(),
        "tails": results,
        "by_airport": by_airport,
        "unknown": sorted(unknown),
    }
    with open("psa_ron_forecast.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"\nWritten psa_ron_forecast.json — {len(results)} located, "
          f"{len(unknown)} unknown, {len(by_airport)} airports")
    top = sorted(by_airport.items(), key=lambda kv: -len(kv[1]))[:10]
    for code, ts in top:
        print(f"  {code}: {len(ts)}")


if __name__ == "__main__":
    main()
