"""
PSA Overnight Location (RON) Forecast — aviationstack
=====================================================
Pulls ALL PSA Airlines flights (ICAO airline code JIA) for the flight day
from aviationstack, groups them by aircraft registration, and works out
where each rostered tail ends the night: the arrival airport of its LAST
arrival before 04:00 Eastern the morning after "night_of". Tails with no
flights in the window are reported under "unknown".

Querying by airline instead of per tail keeps API usage tiny: ~10-25
paginated calls per run (~750/mo) against the plan's 10,000/mo — versus
155 calls/run if queried per registration.

Writes psa_ron_forecast.json:
  {
    "generated": "...UTC...",
    "program": "PSA",
    "night_of": "YYYY-MM-DD",           # Eastern date the night begins
    "tails": [
      {"tail": "N500AE", "overnight": "CLT", "confidence": "scheduled",
       "last_arrival": "...", "flights_today": 5}, ...
    ],
    "by_airport": {"CLT": ["N500AE", ...], ...},
    "unknown": ["N123AB", ...]
  }

confidence: "completed" — flight already landed
            "in_air"    — currently active; destination per estimate
            "scheduled" — still to depart; destination per schedule

Scheduling: crons at 20:07/21:07 UTC; on scheduled runs the guard only
proceeds at 16:xx (4 PM) America/New_York. Manual dispatch skips the guard.
A run before noon Eastern forecasts the night already in progress
(night_of = yesterday) so late-night manual runs stay meaningful.

Env: AERO_API_KEY — the aviationstack access key (GitHub secret).
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

BASE = "https://api.aviationstack.com/v1/flights"
ET = ZoneInfo("America/New_York")
AIRLINE_ICAO = "JIA"  # PSA Airlines (operates as American Eagle)


def guard_4pm_eastern():
    if os.environ.get("GITHUB_EVENT_NAME") != "schedule":
        return
    hour = datetime.now(ET).hour
    if hour != 16:
        print(f"Not 4 PM Eastern (hour={hour}) — skipping this scheduled run.")
        sys.exit(0)


def fetch_day(key, flight_date):
    """All JIA flights for one flight_date, paginated. Returns list."""
    flights, offset, calls = [], 0, 0
    while calls < 30:
        r = requests.get(BASE, params={
            "access_key": key,
            "airline_icao": AIRLINE_ICAO,
            "flight_date": flight_date,
            "limit": 100,
            "offset": offset,
        }, timeout=30)
        calls += 1
        if r.status_code == 429:
            print("  rate limited — sleeping 60s")
            time.sleep(60)
            continue
        r.raise_for_status()
        payload = r.json()
        if "error" in payload:
            raise RuntimeError(f"aviationstack error: {json.dumps(payload['error'])[:300]}")
        batch = payload.get("data", [])
        flights.extend(batch)
        pag = payload.get("pagination", {})
        total = pag.get("total", 0)
        offset += pag.get("count", len(batch))
        if not batch or offset >= total:
            break
        time.sleep(0.3)
    print(f"  {flight_date}: {len(flights)} JIA flights ({calls} API calls)")
    return flights


def parse_dt(v):
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def main():
    guard_4pm_eastern()
    key = os.environ["AERO_API_KEY"].strip()

    planes = json.load(open("psa_data.json"))["planes"]
    roster = {p["tail"].upper() for p in planes}
    print(f"Roster: {len(roster)} PSA tails")

    now_utc = datetime.now(timezone.utc)
    now_et = datetime.now(ET)
    # Before noon Eastern, the night in progress began yesterday.
    night_of = now_et.date() if now_et.hour >= 12 else now_et.date() - timedelta(days=1)
    cutoff = datetime.combine(night_of + timedelta(days=1), datetime.min.time(),
                              ET).replace(hour=4).astimezone(timezone.utc)

    # Flights arriving during the night depart on night_of (or just after
    # midnight); fetch both flight days and merge.
    all_flights = fetch_day(key, night_of.isoformat())
    all_flights += fetch_day(key, (night_of + timedelta(days=1)).isoformat())

    # Group by registration
    by_reg = {}
    no_reg = 0
    for f in all_flights:
        reg = ((f.get("aircraft") or {}).get("registration") or "").strip().upper()
        if not reg:
            no_reg += 1
            continue
        by_reg.setdefault(reg, []).append(f)
    print(f"Registrations seen: {len(by_reg)} | flights without registration: {no_reg}")

    results, unknown = [], []
    for tail in sorted(roster):
        flights = by_reg.get(tail, [])
        candidates = []
        for f in flights:
            if (f.get("flight_status") or "") == "cancelled":
                continue
            arr = f.get("arrival") or {}
            dt = parse_dt(arr.get("actual") or arr.get("estimated") or arr.get("scheduled"))
            code = (arr.get("iata") or "").upper()
            if dt and code and dt <= cutoff:
                candidates.append((dt, f, code))
        if not candidates:
            unknown.append(tail)
            continue
        dt, f, code = max(candidates, key=lambda c: c[0])
        status = f.get("flight_status") or ""
        arr = f.get("arrival") or {}
        if arr.get("actual") or status == "landed":
            conf = "completed"
        elif status == "active":
            conf = "in_air"
        else:
            conf = "scheduled"
        results.append({
            "tail": tail,
            "overnight": code,
            "confidence": conf,
            "last_arrival": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "flights_today": len(flights),
        })

    by_airport = {}
    for r in results:
        by_airport.setdefault(r["overnight"], []).append(r["tail"])
    by_airport = {k: sorted(v) for k, v in sorted(by_airport.items())}

    out = {
        "generated": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "program": "PSA",
        "night_of": night_of.isoformat(),
        "tails": results,
        "by_airport": by_airport,
        "unknown": sorted(unknown),
    }
    with open("psa_ron_forecast.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"\nWritten psa_ron_forecast.json — night of {night_of}: "
          f"{len(results)} located, {len(unknown)} unknown, {len(by_airport)} airports")
    for code, ts in sorted(by_airport.items(), key=lambda kv: -len(kv[1])):
        print(f"  {code}: {len(ts)}")


if __name__ == "__main__":
    main()
