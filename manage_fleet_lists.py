"""Add/remove one tail across a program's JotForm lists + SafetyCulture
response set — the shared engine behind manage_mesa_fleet.yml /
manage_gojet_fleet.yml / manage_jsx_fleet.yml (Envoy/PSA keep their own
workflows). NO SharePoint write: the roster row in the Debriefs workbook
stays a manual edit (owner decision, 2026-09-16).

Env: PROGRAM (mesa|gojet|jsx), ACTION (add_tail|remove_tail), TAIL_INPUT,
JOTFORM_KEY, SAFETYCULTURE_KEY. Writes <program>_fleet_action_result.json
for the platform to poll and a GitHub step summary.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

import requests

JOTFORM_BASE = os.environ.get("JOTFORM_BASE", "https://foxtrotaviation.jotform.com/API")
KEY = os.environ["JOTFORM_KEY"]
SC_KEY = os.environ["SAFETYCULTURE_KEY"]

# (key, label, kind, form_id, qid[, widget line label]) — same targets the
# tail_reconcile.py audit covers; keep the two in step.
PROGRAMS = {
    "mesa": {
        "tail_re": r"^N\d{1,5}[A-Z]{0,2}$",
        "sc_set": ("MESA Tails", "responseset_81917085c88a4d9f8f2b645163ebc546"),
        "lists": [
            ("mesa_debrief", "Mesa Debrief", "dropdown", "220187674891163", "47"),
            ("mesa_closeout", "Commercial Closeout (Mesa)", "widget", "222916060752150", "298", "Tail Number"),
        ],
        "result_file": "mesa_fleet_action_result.json",
    },
    "gojet": {
        "tail_re": r"^\d{1,4}$",            # bare ship numbers (501, 583, ...)
        "sc_set": ("GoJet Tails", "responseset_4d657b974486489cb23ba2bf224ba6d0"),
        "lists": [
            ("gojet_debrief", "GoJet Debrief", "dropdown", "250554449184058", "21"),
        ],
        "result_file": "gojet_fleet_action_result.json",
    },
    "jsx": {
        "tail_re": r"^N\d{1,5}[A-Z]{0,2}$",
        "sc_set": ("JSX Tails", "responseset_4209d5b39cbc465babe7ed96cd2aab25"),
        "lists": [
            ("jsx_debrief", "JSX Debrief", "dropdown", "260637830358058", "19"),
            ("jsx_closeout", "JSX Closeout", "widget", "262036208159051", "7", "Tail Number"),
        ],
        "result_file": "jsx_fleet_action_result.json",
    },
}

PROGRAM = os.environ["PROGRAM"].strip().lower()
CFG = PROGRAMS[PROGRAM]
action = os.environ["ACTION"].strip()
tail = os.environ["TAIL_INPUT"].strip().upper()

if action not in ("add_tail", "remove_tail"):
    print(f"Unsupported action: {action}", file=sys.stderr)
    sys.exit(1)
if not re.match(CFG["tail_re"], tail):
    print(f"Invalid {PROGRAM} tail format: {tail!r}", file=sys.stderr)
    sys.exit(1)
adding = action == "add_tail"
print(f"Program: {PROGRAM}  |  Action: {action}  |  Tail: {tail}")


def sort_key(t):
    m = re.match(r"^N?(\d+)", t.upper())
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
    """(new_list, message, changed) — NOT LISTED (any casing) stays last."""
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
    url = f"{JOTFORM_BASE}/form/{form_id}/question/{qid}?apiKey={KEY}"
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
    url = f"{JOTFORM_BASE}/form/{form_id}/question/{qid}?apiKey={KEY}"
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


result = {"tail": tail, "action": action, "program": PROGRAM,
          "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")}

rows = []
for entry in CFG["lists"]:
    key_name, label, kind, *args = entry
    try:
        result[key_name] = do_dropdown(*args) if kind == "dropdown" else do_widget(*args)
    except Exception as e:
        result[key_name] = {"status": "error", "message": str(e)[:200]}
    rows.append((label, result[key_name]))
    print(f"{label}: {result[key_name]}")

# SafetyCulture response set: label-match PUT preserves response IDs, so
# reorder/add/remove never invalidates bindings or historical answers.
sc_name, sc_id = CFG["sc_set"]
try:
    H = {"Authorization": f"Bearer {SC_KEY}", "Content-Type": "application/json"}
    r = requests.get(f"https://api.safetyculture.io/response_sets/{sc_id}", headers=H, timeout=30)
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
        r = requests.put(f"https://api.safetyculture.io/response_sets/{sc_id}", headers=H,
                         json={"name": current.get("name", sc_name),
                               "responses": [{"label": l} for l in labels]},
                         timeout=30)
        r.raise_for_status()
        result["safetyculture"] = {"status": "ok", "message":
            (f"added at position {labels.index(tail) + 1} of {len(labels)}"
             if adding else f"removed ({len(labels)} remain)")}
except Exception as e:
    result["safetyculture"] = {"status": "error", "message": str(e)[:200]}
rows.append((f'SafetyCulture ("{sc_name}")', result["safetyculture"]))
print(f"SafetyCulture: {result['safetyculture']}")

# The roster row is a manual workbook edit by design — say so in the result
# so the platform table reminds the admin.
verb = "add the tail row to" if adding else f"set Status=Disabled on"
result["sharepoint"] = {"status": "manual",
                        "message": f"{verb} the {PROGRAM} Debriefs roster sheet by hand"}
rows.append(("SharePoint roster", result["sharepoint"]))

with open(CFG["result_file"], "w") as f:
    json.dump(result, f, indent=2)

summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
if summary_path:
    icons = {"ok": "✅", "noop": "➖", "error": "❌", "manual": "✍️"}
    with open(summary_path, "a") as f:
        f.write(f"## {PROGRAM} {action}: `{tail}`\n\n| System | Status | Detail |\n|---|---|---|\n")
        for label, s in rows:
            f.write(f"| {label} | {icons.get(s['status'], '?')} {s['status']} | {s['message']} |\n")

if all(v.get("status") == "error" for k, v in result.items()
       if isinstance(v, dict) and k != "sharepoint"):
    sys.exit(1)
