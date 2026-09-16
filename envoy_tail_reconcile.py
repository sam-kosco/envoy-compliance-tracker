"""Audit (and optionally fix) every Envoy tail list against the authority:
the Tail List sheet of Power Flows/Debriefs/Envoy Debriefs.xlsx (non-Disabled
rows). Targets = the same eight JotForm lists manage_envoy_fleet.yml maintains
plus the SafetyCulture "Envoy Tails" response set.

APPLY=false (default): report only — per-list missing/extra/order status.
APPLY=true: rewrite each out-of-sync list to exactly the roster, numeric order,
preserving each list's non-tail entries (leading placeholders in place,
NOT LISTED kept last, widget ":Please Select" trailing segments untouched).

Run via envoy_tail_reconcile.yml. Exit code 0 always unless a source errors;
"in sync" vs diffs is reported in the step summary.
"""
import io
import json
import os
import re
import sys

import requests
from openpyxl import load_workbook

TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
JOTFORM_KEY = os.environ["JOTFORM_KEY"]
JOTFORM_BASE = os.environ.get("JOTFORM_BASE", "https://foxtrotaviation.jotform.com/API")
SC_KEY = os.environ["SAFETYCULTURE_KEY"]
SC_SET_ID = os.environ.get("ENVOY_TAILS_SET_ID", "responseset_00749b53e9c34c618ee08f3e3e29f014")
DRIVE_ID = "b!_bzXaIx86kOufgJN3ih-BaDIDthKYuxJkJtLi1Bm5irGjCEnK-VHSpBRRm3_SDKU"
FILE_PATH = "Power Flows/Debriefs/Envoy Debriefs.xlsx"
APPLY = os.environ.get("APPLY", "false").strip().lower() == "true"

TAIL_RE = re.compile(r"^N\d{1,5}[A-Z]{0,2}$")


def sort_key(t):
    m = re.match(r"^N(\d+)", t.upper())
    return (0, int(m.group(1)), t.upper()) if m else (1, 0, t.upper())


def roster():
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
              "scope": "https://graph.microsoft.com/.default",
              "grant_type": "client_credentials"}, timeout=30)
    r.raise_for_status()
    tok = r.json()["access_token"]
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{FILE_PATH.replace(' ', '%20')}:/content",
        headers={"Authorization": f"Bearer {tok}"}, timeout=120)
    r.raise_for_status()
    wb = load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    ws = wb["Tail List"]
    tails, seen = [], set()
    for row in ws.iter_rows(min_row=2, values_only=True):
        t = str(row[0]).strip().upper() if row and row[0] else ""
        status = str(row[5]).strip().lower() if len(row) > 5 and row[5] else ""
        if t and TAIL_RE.match(t) and status != "disabled" and t not in seen:
            seen.add(t)
            tails.append(t)
    wb.close()
    return sorted(tails, key=sort_key)


def split_entries(options):
    """(tails_upper_in_order, specials) — specials are non-tail entries like
    'NOT LISTED' / 'Please Select', kept verbatim with their positions."""
    tails, specials = [], []
    for i, o in enumerate(options):
        o = o.strip()
        if not o:
            continue
        if TAIL_RE.match(o.upper()):
            tails.append(o.upper())
        else:
            specials.append((i, o))
    return tails, specials


def diff(name, current_tails, specials):
    cur = list(dict.fromkeys(current_tails))          # dedupe, keep order
    missing = [t for t in TARGET if t not in set(cur)]
    extra = [t for t in cur if t not in set(TARGET)]
    ordered = cur == [t for t in TARGET if t in set(cur)]
    dupes = len(current_tails) - len(cur)
    return {"list": name, "count": len(cur), "missing": missing, "extra": extra,
            "out_of_order": not ordered, "duplicates": dupes,
            "specials": [s for _, s in specials],
            "in_sync": not missing and not extra and ordered and dupes == 0}


def rebuilt(specials):
    """Roster in numeric order; specials whose label is NOT LISTED go last,
    any other special (e.g. a leading placeholder) goes back to the front."""
    front = [s for _, s in specials if s.upper() != "NOT LISTED"]
    back = [s for _, s in specials if s.upper() == "NOT LISTED"]
    return front + list(TARGET) + back


def do_dropdown(name, form_id, qid):
    url = f"{JOTFORM_BASE}/form/{form_id}/question/{qid}?apiKey={JOTFORM_KEY}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    opts = (r.json().get("content", {}).get("options") or "").split("|")
    tails, specials = split_entries(opts)
    d = diff(name, tails, specials)
    if APPLY and not d["in_sync"]:
        r = requests.post(url, data={"question[options]": "|".join(rebuilt(specials))}, timeout=30)
        r.raise_for_status()
        d["fixed"] = True
    return d


def do_widget(name, form_id, qid, line_label="Tail Number"):
    url = f"{JOTFORM_BASE}/form/{form_id}/question/{qid}?apiKey={JOTFORM_KEY}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    fields_raw = r.json().get("content", {}).get("fields", "")
    lines = fields_raw.replace("\r\n", "\n").split("\n")
    idx = next((i for i, ln in enumerate(lines)
                if re.match(rf"^\*?\s*{re.escape(line_label)}\s*:\s*dropdown\s*:", ln, re.I)), None)
    if idx is None:
        raise RuntimeError(f"'{line_label}' dropdown line not found")
    parts = lines[idx].split(":")
    label, ftype, opts_raw, trailing = parts[0], parts[1], parts[2], parts[3:]
    tails, specials = split_entries(opts_raw.split(","))
    d = diff(name, tails, specials)
    d["placeholder"] = ":".join(trailing) if trailing else ""
    if APPLY and not d["in_sync"]:
        lines[idx] = ":".join([label, ftype, ",".join(rebuilt(specials))] + trailing)
        r = requests.post(url, data={"question[fields]": "\n".join(lines)}, timeout=30)
        r.raise_for_status()
        d["fixed"] = True
    return d


def do_safetyculture():
    H = {"Authorization": f"Bearer {SC_KEY}", "Content-Type": "application/json"}
    r = requests.get(f"https://api.safetyculture.io/response_sets/{SC_SET_ID}", headers=H, timeout=30)
    r.raise_for_status()
    current = r.json()
    labels = [x["label"].strip() for x in current.get("responses", []) if x.get("label", "").strip()]
    tails, specials = split_entries(labels)
    d = diff('SafetyCulture "Envoy Tails"', tails, specials)
    if APPLY and not d["in_sync"]:
        # Label-match PUT preserves response IDs, so historical inspection
        # answers and template bindings survive the rewrite.
        r = requests.put(f"https://api.safetyculture.io/response_sets/{SC_SET_ID}", headers=H,
                         json={"name": current.get("name", "Envoy Tails"),
                               "responses": [{"label": l} for l in rebuilt(specials)]},
                         timeout=30)
        r.raise_for_status()
        d["fixed"] = True
    return d


TARGET = roster()
print(f"Roster (authority): {len(TARGET)} non-Disabled tails\n")

LISTS = [
    ("Envoy Debrief Q53", do_dropdown, "222916997891173", "53"),
    ("DFW Debrief Q51", do_dropdown, "222277068943160", "51"),
    ("Commercial Closeout Q45 (Envoy Fleet)", do_widget, "222916060752150", "45"),
    ("CMH Closeout Q6", do_widget, "261664495134058", "6"),
    ("XNA Closeout Q6", do_widget, "261954357644972", "6"),
    ("SGF Closeout Q6", do_widget, "261954499086979", "6"),
    ("LIT Closeout Q6", do_widget, "261955038475971", "6"),
    ("DFW Closeout Q6", do_widget, "261755203398967", "6"),
]

results, errors = [], 0
for name, fn, *args in LISTS:
    try:
        results.append(fn(name, *args))
    except Exception as e:
        errors += 1
        results.append({"list": name, "error": str(e)[:200]})
try:
    results.append(do_safetyculture())
except Exception as e:
    errors += 1
    results.append({"list": 'SafetyCulture "Envoy Tails"', "error": str(e)[:200]})

print(json.dumps(results, indent=2))

summary = os.environ.get("GITHUB_STEP_SUMMARY")
if summary:
    with open(summary, "a") as f:
        f.write(f"## Envoy tail reconcile — {'APPLY' if APPLY else 'AUDIT'} "
                f"(roster = {len(TARGET)} tails)\n\n")
        f.write("| List | Tails | Missing | Extra | Order | Dupes | State |\n|---|---|---|---|---|---|---|\n")
        for d in results:
            if "error" in d:
                f.write(f"| {d['list']} | — | — | — | — | — | ❌ {d['error']} |\n")
                continue
            state = ("✅ fixed" if d.get("fixed")
                     else ("✅ in sync" if d["in_sync"] else "⚠️ diffs"))
            f.write(f"| {d['list']} | {d['count']} | "
                    f"{', '.join(d['missing']) or '—'} | {', '.join(d['extra']) or '—'} | "
                    f"{'⚠️' if d['out_of_order'] else 'ok'} | {d['duplicates'] or '—'} | {state} |\n")
        specials = {d["list"]: d.get("specials") for d in results if d.get("specials")}
        if specials:
            f.write(f"\nNon-tail entries preserved per list: `{json.dumps(specials)}`\n")

sys.exit(1 if errors else 0)
