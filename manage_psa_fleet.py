"""PSA tail add/remove — run by manage_fleet.yml (platform-dispatched).

2026-09-17: remove is fully SYMMETRIC like the other programs (Sam) — both
actions hit the PSA Debrief dropdown, the Commercial Closeout PSA Fleet
widget, the SafetyCulture "PSA Tails" set, and the SharePoint Tail List row
via the consolidated "Add/Remove Tails" PA flow (ROSTER_FLOW_URL, async).
Removing an option is safe for history: submissions keep their stored text
and SC keeps inspection answers.

Env: ACTION, TAIL_INPUT, JOTFORM_KEY, JOTFORM_BASE, JOTFORM_FORM_ID,
JOTFORM_TAIL_QID, CLOSEOUT_FORM_ID, CLOSEOUT_PSA_QID, SAFETYCULTURE_KEY,
PSA_TAILS_SET_ID, ROSTER_FLOW_URL. Writes fleet_action_result.json
(keys: jotform / closeout / safetyculture / sharepoint — the platform's
result table renders these).
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

import requests

action = os.environ["ACTION"].strip()
tail = os.environ["TAIL_INPUT"].strip().upper()

if action not in ("add_tail", "remove_tail"):
    print(f"Unsupported action: {action}", file=sys.stderr)
    sys.exit(1)
if not re.match(r"^N\d{1,5}[A-Z]{0,2}$", tail):
    print(f"Invalid tail format: {tail!r}", file=sys.stderr)
    sys.exit(1)

adding = action == "add_tail"
print(f"Action: {action}  |  Tail: {tail}")

KEY = os.environ["JOTFORM_KEY"]
BASE = os.environ["JOTFORM_BASE"]


def sort_key(t):
    m = re.match(r"^N(\d+)", t.upper())
    return (0, int(m.group(1)), t.upper()) if m else (1, 0, t.upper())


def dedupe(options):
    seen, out = set(), []
    for o in options:
        o = o.strip()
        if o and o.upper() not in seen:
            seen.add(o.upper())
            out.append(o)
    return out


def apply_options(cleaned):
    """(new_list, message, changed) — NOT LISTED / Not Listed stays last."""
    nl = next((o for o in cleaned if o.upper() == "NOT LISTED"), None)
    others = [o for o in cleaned if o.upper() != "NOT LISTED"]
    present = tail in (o.upper() for o in others)
    if adding:
        if present:
            return None, f"already present ({len(others)} tails)", False
        others.append(tail)
        others.sort(key=sort_key)
        final = others + ([nl] if nl else [])
        return final, f"added at position {others.index(tail) + 1} of {len(final)}", True
    if not present:
        return None, "not present", False
    others = [o for o in others if o.upper() != tail]
    final = others + ([nl] if nl else [])
    return final, f"removed ({len(others)} tails remain)", True


def do_dropdown(form_id, qid):
    url = f"{BASE}/form/{form_id}/question/{qid}?apiKey={KEY}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    opts_raw = r.json().get("content", {}).get("options", "")
    final, msg, changed = apply_options(dedupe(opts_raw.split("|") if opts_raw else []))
    if not changed:
        return {"status": "noop", "message": msg}
    r = requests.post(url, data={"question[options]": "|".join(final)}, timeout=30)
    r.raise_for_status()
    return {"status": "ok", "message": msg}


def do_widget(form_id, qid, line_label):
    url = f"{BASE}/form/{form_id}/question/{qid}?apiKey={KEY}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    fields_raw = r.json().get("content", {}).get("fields", "")
    if not fields_raw:
        raise RuntimeError("widget 'fields' property is empty — format changed?")
    lines = fields_raw.replace("\r\n", "\n").split("\n")
    idx = next((i for i, ln in enumerate(lines)
                if re.match(rf"^\*?\s*{re.escape(line_label)}\s*:\s*dropdown\s*:", ln, re.I)), None)
    if idx is None:
        raise RuntimeError(f"'{line_label}' dropdown line not found in widget fields")
    parts = lines[idx].split(":")
    label, ftype, opts_raw = parts[0], parts[1], parts[2]
    trailing = parts[3:]
    final, msg, changed = apply_options(dedupe(opts_raw.split(",")))
    if not changed:
        return {"status": "noop", "message": msg}
    lines[idx] = ":".join([label, ftype, ",".join(final)] + trailing)
    r = requests.post(url, data={"question[fields]": "\n".join(lines)}, timeout=30)
    r.raise_for_status()
    return {"status": "ok", "message": msg}


result = {"tail": tail, "action": action,
          "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")}

# ── 1. JotForm PSA Debrief tail dropdown ─────────────────
try:
    result["jotform"] = do_dropdown(os.environ["JOTFORM_FORM_ID"],
                                    os.environ["JOTFORM_TAIL_QID"])
except Exception as e:
    result["jotform"] = {"status": "error", "message": str(e)[:200]}
print(f"JotForm: {result['jotform']}")

# ── 2. Commercial Closeout PSA Fleet widget (Q27, "Dropdown" line) ──
try:
    result["closeout"] = do_widget(os.environ["CLOSEOUT_FORM_ID"],
                                   os.environ["CLOSEOUT_PSA_QID"], "Dropdown")
except Exception as e:
    result["closeout"] = {"status": "error", "message": str(e)[:200]}
print(f"Closeout: {result['closeout']}")

# ── 3. SafetyCulture "PSA Tails" (label-match PUT keeps response IDs) ──
try:
    sc_key = os.environ["SAFETYCULTURE_KEY"]
    set_id = os.environ["PSA_TAILS_SET_ID"]
    H = {"Authorization": f"Bearer {sc_key}", "Content-Type": "application/json"}
    r = requests.get(f"https://api.safetyculture.io/response_sets/{set_id}", headers=H, timeout=30)
    r.raise_for_status()
    current = r.json()
    labels = [x["label"] for x in current.get("responses", [])]
    present = tail in (l.upper() for l in labels)
    if adding and present:
        result["safetyculture"] = {"status": "noop", "message": f"already present ({len(labels)} total)"}
    elif not adding and not present:
        result["safetyculture"] = {"status": "noop", "message": "not present"}
    else:
        labels = (labels + [tail]) if adding else [l for l in labels if l.upper() != tail]
        labels.sort(key=sort_key)
        r = requests.put(f"https://api.safetyculture.io/response_sets/{set_id}", headers=H,
                         json={"name": current.get("name", "PSA Tails"),
                               "responses": [{"label": l} for l in labels]},
                         timeout=30)
        r.raise_for_status()
        result["safetyculture"] = {"status": "ok", "message":
            (f"added at position {labels.index(tail) + 1} of {len(labels)}"
             if adding else f"removed ({len(labels)} remain)")}
except Exception as e:
    result["safetyculture"] = {"status": "error", "message": str(e)[:200]}
print(f"SafetyCulture: {result['safetyculture']}")

# ── 4. SharePoint Tail List row via the consolidated PA flow ──
# add -> row appended (Status=Active); remove -> Status=Disabled.
# Async (202): "ok" means accepted; failures land in PA run history.
try:
    flow_url = os.environ.get("ROSTER_FLOW_URL", "")
    if not flow_url:
        result["sharepoint"] = {"status": "skipped", "message": "ROSTER_FLOW_URL not configured"}
    else:
        r = requests.post(flow_url, json={"Program": "PSA", "Tail Number": tail,
                                          "Action": action}, timeout=60)
        r.raise_for_status()
        result["sharepoint"] = {"status": "ok", "message":
            ("row add sent (Status=Active)" if adding else "Status=Disabled sent") + " — async"}
except Exception as e:
    result["sharepoint"] = {"status": "error", "message": str(e)[:200]}
print(f"SharePoint: {result['sharepoint']}")

with open("fleet_action_result.json", "w") as f:
    json.dump(result, f, indent=2)

summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
if summary_path:
    with open(summary_path, "a") as f:
        f.write(f"## {action}: `{tail}`\n\n")
        f.write("| System | Status | Detail |\n|---|---|---|\n")
        for k in ("jotform", "closeout", "safetyculture", "sharepoint"):
            s = result[k]
            icon = {"ok": "✅", "noop": "➖", "error": "❌", "skipped": "⏭"}.get(s["status"], "?")
            f.write(f"| {k} | {icon} {s['status']} | {s['message']} |\n")

if all(result[k]["status"] == "error" for k in ("safetyculture", "jotform", "closeout")):
    sys.exit(1)
