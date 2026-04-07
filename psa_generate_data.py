"""
PSA Compliance Data Relay
==========================
Reads PSA_Debriefs.xlsx from SharePoint, calculates compliance windows
for all 155 tails, and writes psa_data.json for the GitHub Pages dashboard.

Tracked jobs (all from Debriefs sheet):
  CC  — Cockpit Clean            — 30-day cycle
  DSC — Deep Seat Clean          — 30-day cycle
  CE  — Carpet Extraction        — 30-day cycle
  ED1 — Exterior Detail #1       — 30-day cycle
  ED2 — Exterior Detail #2       — 30-day cycle
  Lav — Lav Tank Pressure Wash   — 90-day cycle

Informational only (shown in detail panel, no compliance window):
  IC  — Interior Clean
  EC  — Exterior Clean
  ED3 — Exterior Detail #3
  ED4 — Exterior Detail #4

Credentials (GitHub Secrets — same as Envoy tracker):
  TENANT_ID, CLIENT_ID, CLIENT_SECRET
"""

import os, json, sys, requests
from datetime import datetime, timezone, date

TENANT_ID     = os.environ["TENANT_ID"]
CLIENT_ID     = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]

DRIVE_ID  = "b!_bzXaIx86kOufgJN3ih-BaDIDthKYuxJkJtLi1Bm5irGjCEnK-VHSpBRRm3_SDKU"
FILE_PATH = "Power Flows/Debriefs/PSA Debriefs.xlsx"

CYCLES  = {"CC": 30, "DSC": 30, "CE": 30, "ED1": 30, "ED2": 30, "Lav": 90}
TRACKED = list(CYCLES.keys())
INFO    = ["IC", "EC", "ED3", "ED4"]


def get_token():
    print("Acquiring Graph API token...")
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET, "scope": "https://graph.microsoft.com/.default"}
    )
    resp.raise_for_status()
    print("  Token acquired.")
    return resp.json()["access_token"]


def download_excel(token):
    print(f"Downloading: {FILE_PATH}")
    encoded = FILE_PATH.replace(" ", "%20")
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/{encoded}:/content",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    path = "/tmp/psa_debriefs.xlsx"
    with open(path, "wb") as f:
        f.write(resp.content)
    print(f"  Downloaded {len(resp.content):,} bytes")
    return path


def flag(v):
    if v is None: return 0
    return 0 if str(v).strip().lower() in ("no", "0", "", "none", "nan") else 1


def parse_date(v):
    if v is None: return None
    if isinstance(v, datetime): return v.date()
    if isinstance(v, date): return v
    return None


def fmt(d): return d.isoformat() if d else None


def parse_workbook(path):
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)

    # Tails
    ws_tl = wb["Tail List"]
    tails = [str(r[0]).strip().upper() for r in ws_tl.iter_rows(values_only=True)
              if r[0] and str(r[0]).strip().upper() != "TAILS"]
    print(f"  Tail List: {len(tails)} tails")

    # Debriefs
    # Cols: 0=Date,1=Name,2=Location,3=Tail,4=IC,5=EC,6=CC,7=DSC,8=CE,
    #       9=ED1,10=ED2,11=ED3,12=ED4,13=Lav,14=SubID
    ws_d = wb["Debriefs"]
    debriefs = []
    for row in list(ws_d.iter_rows(values_only=True))[1:]:
        if not row[0]: continue
        loc = str(row[2] or "").strip()
        loc = loc.replace("-PSA", "").replace("-psa", "").strip()
        debriefs.append({
            "date":     parse_date(row[0]),
            "name":     str(row[1] or "").strip(),
            "location": loc,
            "tail":     str(row[3] or "").strip().upper(),
            "IC":  flag(row[4]),  "EC":  flag(row[5]),
            "CC":  flag(row[6]),  "DSC": flag(row[7]),
            "CE":  flag(row[8]),  "ED1": flag(row[9]),
            "ED2": flag(row[10]), "ED3": flag(row[11]),
            "ED4": flag(row[12]), "Lav": flag(row[13]),
        })
    print(f"  Debriefs: {len(debriefs)} rows")
    return tails, debriefs


def build_planes(tails, debriefs):
    today = date.today()
    by_tail = {}
    for d in debriefs:
        t = d["tail"]
        if t not in by_tail: by_tail[t] = []
        by_tail[t].append(d)

    planes = []
    for tail in tails:
        recs = by_tail.get(tail, [])
        last = {j: None for j in TRACKED + INFO}
        ls = None
        for d in recs:
            dd = d["date"]
            if dd is None: continue
            if ls is None or dd > ls: ls = dd
            for j in TRACKED + INFO:
                if d[j] == 1 and (last[j] is None or dd > last[j]):
                    last[j] = dd

        windows = {j: ("No Service" if last[j] is None else CYCLES[j] - (today - last[j]).days)
                   for j in TRACKED}

        sr = sorted([r for r in recs if r["date"]], key=lambda r: r["date"], reverse=True)
        planes.append({
            "tail": tail,
            "lastService":  fmt(ls),
            "lastCC":       fmt(last["CC"]),
            "lastDSC":      fmt(last["DSC"]),
            "lastCE":       fmt(last["CE"]),
            "lastED1":      fmt(last["ED1"]),
            "lastED2":      fmt(last["ED2"]),
            "lastLav":      fmt(last["Lav"]),
            "lastIC":       fmt(last["IC"]),
            "lastEC":       fmt(last["EC"]),
            "lastED3":      fmt(last["ED3"]),
            "lastED4":      fmt(last["ED4"]),
            "lastLocation": sr[0]["location"] if sr else None,
            "lastTech":     sr[0]["name"]     if sr else None,
            **windows,
        })

    print(f"  Built compliance for {len(planes)} planes")
    return planes


def format_debriefs(debriefs):
    out = []
    for d in debriefs:
        out.append({
            "tail": d["tail"], "date": fmt(d["date"]),
            "name": d["name"], "location": d["location"],
            "IC": d["IC"],  "EC": d["EC"],  "CC": d["CC"],
            "DSC": d["DSC"], "CE": d["CE"], "ED1": d["ED1"],
            "ED2": d["ED2"], "ED3": d["ED3"], "ED4": d["ED4"],
            "Lav": d["Lav"],
        })
    out.sort(key=lambda r: r["date"] or "", reverse=True)
    return out


if __name__ == "__main__":
    try:
        import openpyxl
    except ImportError:
        print("ERROR: pip install openpyxl requests")
        sys.exit(1)

    print("=== PSA Compliance Data Relay ===")
    print(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

    token     = get_token()
    xlsx_path = download_excel(token)

    print("\nParsing workbook...")
    tails, debriefs = parse_workbook(xlsx_path)

    print("\nBuilding compliance table...")
    planes          = build_planes(tails, debriefs)
    debriefs_out    = format_debriefs(debriefs)

    output = {"generated": datetime.now(timezone.utc).isoformat(),
              "planes": planes, "debriefs": debriefs_out}

    with open("psa_data.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    nc   = sum(1 for p in planes if any(p[j] == "No Service" or
               (isinstance(p[j], int) and p[j] < 0) for j in TRACKED))
    soon = sum(1 for p in planes if not any(p[j] == "No Service" or
               (isinstance(p[j], int) and p[j] < 0) for j in TRACKED)
               and any(isinstance(p[j], int) and 0 <= p[j] <= 7 for j in TRACKED))

    print(f"\n  Written: psa_data.json")
    print(f"  Noncompliant: {nc}  |  Due soon: {soon}  |  OK: {len(planes)-nc-soon}")
    print("\n=== Done ===")
