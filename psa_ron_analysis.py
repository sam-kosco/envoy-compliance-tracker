"""
PSA RON Capture Analysis
========================
Morning-after companion to psa_ron_forecast.py. Joins yesterday's 4 PM
overnight forecast against the debriefs actually submitted for that night
and measures how often a plane RON'ing at a covered station got cleaned
("capture rate"), per station and overall.

For each covered station:
  ron        — tails forecast to overnight there
  cleaned    — tails debriefed there with debrief date == night_of
  captured   — intersection (RON'd with us AND got cleaned)
  missed     — RON'd with us, no debrief
  extras     — debriefed there but NOT forecast to RON there (turns,
               forecast misses, or late tail swaps)
Debriefs dated the morning after (night_of + 1) are counted separately as
"next-day-dated" — techs occasionally date the calendar morning.

Emails an HTML summary from foxtrot.automation@ to RECIPIENTS and commits
psa_ron_analysis.json for the record.

Scheduling: crons Sat/Sun/Mon at 14:07 & 15:07 UTC; the guard only
proceeds at 10:xx AM America/New_York on scheduled runs. The workflow
refreshes psa_data.json (fresh debriefs) before this script runs.

Env: TENANT_ID / CLIENT_ID / CLIENT_SECRET (Graph email).
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

ET = ZoneInfo("America/New_York")

RECIPIENTS = ["samuel.kosco@foxtrotaviation.com"]
SENDER = "foxtrot.automation@foxtrotaviation.com"


def guard_10am_eastern():
    if os.environ.get("GITHUB_EVENT_NAME") != "schedule":
        return
    hour = datetime.now(ET).hour
    if hour != 10:
        print(f"Not 10 AM Eastern (hour={hour}) — skipping this scheduled run.")
        sys.exit(0)


def graph_token():
    r = requests.post(
        f"https://login.microsoftonline.com/{os.environ['TENANT_ID']}/oauth2/v2.0/token",
        data={"grant_type": "client_credentials", "client_id": os.environ["CLIENT_ID"],
              "client_secret": os.environ["CLIENT_SECRET"],
              "scope": "https://graph.microsoft.com/.default"}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def send_email(token, subject, body_html):
    r = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER}/sendMail",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"message": {"subject": subject,
                          "body": {"contentType": "HTML", "content": body_html},
                          "toRecipients": [{"emailAddress": {"address": a}} for a in RECIPIENTS]}},
        timeout=30)
    r.raise_for_status()


def main():
    guard_10am_eastern()

    forecast = json.load(open("psa_ron_forecast.json"))
    night_of = forecast["night_of"]
    yesterday = (datetime.now(ET).date() - timedelta(days=1)).isoformat()
    stale_note = ""
    if night_of != yesterday:
        stale_note = (f"<p style='color:#B45309'><b>Note:</b> latest forecast covers the night of "
                      f"{night_of}, not {yesterday} — yesterday's 4 PM run may have failed.</p>")

    data = json.load(open("psa_data.json"))
    debriefs = data.get("debriefs", [])
    covered = forecast.get("covered_airports") or sorted(
        {d.get("location") for d in debriefs if d.get("location")})

    next_day = (datetime.fromisoformat(night_of).date() + timedelta(days=1)).isoformat()
    per_loc, tot = [], {"ron": 0, "captured": 0, "missed": 0, "extras": 0}
    for loc in covered:
        ron = set((forecast.get("at_covered") or {}).get(loc, []))
        cleaned = {d["tail"] for d in debriefs if d.get("location") == loc and d.get("date") == night_of}
        cleaned_next = {d["tail"] for d in debriefs if d.get("location") == loc and d.get("date") == next_day}
        captured = ron & cleaned
        missed = ron - cleaned
        extras = cleaned - ron
        if not (ron or cleaned or cleaned_next):
            continue
        per_loc.append({"location": loc, "ron": sorted(ron), "cleaned": sorted(cleaned),
                        "captured": sorted(captured), "missed": sorted(missed),
                        "extras": sorted(extras), "next_day_dated": sorted(cleaned_next - cleaned)})
        tot["ron"] += len(ron); tot["captured"] += len(captured)
        tot["missed"] += len(missed); tot["extras"] += len(extras)

    rate = (100 * tot["captured"] / tot["ron"]) if tot["ron"] else 0.0

    out = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "night_of": night_of, "capture_rate_pct": round(rate, 1),
           "totals": tot, "locations": per_loc}
    with open("psa_ron_analysis.json", "w") as f:
        json.dump(out, f, indent=2)

    # ── Email ────────────────────────────────────────────
    th = 'style="text-align:left;padding:6px 10px;border:1px solid #ccc;background:#1F3864;color:#fff;font-size:13px"'
    td = 'style="padding:6px 10px;border:1px solid #ccc;font-size:13px;vertical-align:top"'
    rows = ""
    for L in per_loc:
        n_ron, n_cap = len(L["ron"]), len(L["captured"])
        loc_rate = f"{100*n_cap/n_ron:.0f}%" if n_ron else "—"
        rows += (f"<tr><td {td}><b>{L['location']}</b></td>"
                 f"<td {td}>{n_ron}</td><td {td}>{n_cap} ({loc_rate})</td>"
                 f"<td {td}>{', '.join(L['missed']) or '—'}</td>"
                 f"<td {td}>{', '.join(L['extras']) or '—'}</td>"
                 f"<td {td}>{', '.join(L['next_day_dated']) or '—'}</td></tr>")

    body = f"""
<div style="font-family:Arial,sans-serif;font-size:14px">
<p>RON capture analysis for the night of <b>{night_of}</b>.</p>
{stale_note}
<p><b>Overall: {tot['captured']} of {tot['ron']} planes that overnighted at a covered
station were cleaned — {rate:.0f}% capture rate.</b>
{tot['extras']} additional planes were cleaned that were not forecast to RON with us.</p>
<table style="border-collapse:collapse">
<tr><th {th}>Station</th><th {th}>RON'd</th><th {th}>Captured</th>
<th {th}>RON'd, not cleaned</th><th {th}>Cleaned, not forecast RON</th><th {th}>Next-day-dated</th></tr>
{rows}
</table>
<p style="margin-top:12px;color:#555;font-size:12px">
Forecast basis: {len(forecast.get('tails', []))} tails located at 4 PM ET
({len(forecast.get('unknown', []))} with no flights that day).
Covered stations: {', '.join(covered)}.</p>
<p style="color:#888;font-size:12px">— PSA RON experiment (Sat/Sun/Mon)</p>
</div>"""

    subject = f"[PSA RON] Night of {night_of} — {rate:.0f}% capture ({tot['captured']}/{tot['ron']})"
    token = graph_token()
    send_email(token, subject, body)
    print(f"Email sent: {subject}")
    print(json.dumps(out["totals"]))


if __name__ == "__main__":
    main()
