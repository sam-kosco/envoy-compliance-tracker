# envoy-compliance-tracker

**Owner:** Samuel Kosco — Data Analyst, Foxtrot Aviation Services  
**Repo:** `sam-kosco/envoy-compliance-tracker`  
**Hosted at:** `sam-kosco.github.io/envoy-compliance-tracker/`

This repo hosts four separate aircraft detailing compliance dashboards for Foxtrot Aviation Services. All four share the same GitHub repository, GitHub Secrets, and Microsoft Entra app registration.

---

## Programs

| Program | Client | Dashboard URL | File |
|---------|--------|---------------|------|
| Envoy | Envoy Air | `/` (index.html) | `index.html` |
| PSA | PSA Airlines | `/psa.html` | `psa.html` |
| Mesa | Mesa Airlines | `/mesa.html` | `mesa.html` |
| Crosswinds | Crosswinds Flight School | `/crosswinds.html` | `crosswinds.html` |

---

## Repository Structure

```
envoy-compliance-tracker/
├── index.html                    # Envoy compliance dashboard
├── psa.html                      # PSA compliance dashboard (incl. Admin tab)
├── mesa.html                     # Mesa compliance dashboard
├── crosswinds.html               # Crosswinds compliance dashboard
├── data.json                     # Envoy compliance data (auto-generated)
├── psa_data.json                 # PSA compliance data (auto-generated)
├── mesa_data.json                # Mesa compliance data (auto-generated)
├── crosswinds_data.json          # Crosswinds compliance data (auto-generated)
├── fleet_action_result.json      # Last result from manage_fleet.yml (auto-generated)
├── envoy_generate_data.py        # Envoy data relay script
├── psa_generate_data.py          # PSA data relay script
├── mesa_generate_data.py         # Mesa data relay script
├── crosswinds_generate_data.py   # Crosswinds data relay script
└── .github/
    └── workflows/
        ├── data_refresh.yml          # Envoy hourly cron
        ├── psa_data_refresh.yml      # PSA hourly cron
        ├── mesa_data_refresh.yml     # Mesa hourly cron
        ├── crosswinds_refresh.yml    # Crosswinds — triggered by Power Automate webhook
        └── manage_fleet.yml          # PSA admin actions (add tail) — workflow_dispatch
```

---

## How Each Program Works

### Envoy, PSA & Mesa
1. Field techs submit JotForm debriefs after each service
2. Power Automate appends submissions to Excel workbooks on SharePoint
3. GitHub Actions runs hourly (`data_refresh.yml` / `psa_data_refresh.yml` / `mesa_data_refresh.yml`)
4. Python script downloads Excel from SharePoint via Microsoft Graph API
5. Script calculates compliance windows and writes `data.json` / `psa_data.json` / `mesa_data.json`
6. GitHub commits the JSON; GitHub Pages serves the updated dashboard

**SharePoint source files:**
- Envoy: `Power Flows/Debriefs/Envoy Debriefs.xlsx`
- PSA: `Power Flows/Debriefs/PSA Debriefs.xlsx`
- Mesa: `Power Flows/Debriefs/Mesa Debriefs.xlsx`

### Crosswinds
1. Field techs complete SafetyCulture audits on mobile
2. Power Automate runs hourly, calls SafetyCulture API for 5 templates (last 7 days)
3. Power Automate handles all Excel writes (Add row / Update row) to `Power Flows/Debriefs/Crosswinds Debriefs.xlsx` — **the Python script never writes to Excel**
4. Power Automate fires `crosswinds_refresh.yml` via GitHub workflow dispatch with a JSON payload of the latest inspections
5. Python script downloads the Excel (read-only), merges it with the incoming payload in memory, calculates rolling 7-day compliance, and writes `crosswinds_data.json`
6. GitHub commits the JSON; GitHub Pages serves the updated dashboard

**SafetyCulture templates:**
| Template ID | Location |
|-------------|----------|
| `template_dcafd01fb49346fba12374215a9e9994` | KFNT |
| `template_c9e29c9d56864b7c8384d18c5c8236bf` | KLAN |
| `template_d48bc6efb7e94758a481ab1f13fbd290` | KOZW |
| `template_7bedfe6abbe04aa595b0a6eac9444da2` | KPTK |
| `template_2eea69a0a73a443ba374bd942db74c00` | KYIP |

**Crosswinds compliance rule:** Each tail must be cleaned **at least twice in a rolling 7-day window**.
- 2+ cleans → Compliant
- 1 clean → Due for Second Clean
- 0 cleans → Noncompliant

**Excel table name:** `Crosswind_Debriefs` (no S)  
**Excel columns:** Date | Name | Location | Tail | Audit ID | Template ID | Report Link

---

## Compliance Logic (Envoy, PSA & Mesa)

Each tracked service has a cycle length. The compliance window is:

```
Window = Cycle Length - Days Since Last Service
```

- **Positive** = days remaining
- **Negative** = days overdue
- **"No Service"** = never performed on this tail

| Status | Condition |
|--------|-----------|
| Noncompliant | Any tracked job window < 0 OR "No Service" |
| Due Soon | No jobs noncompliant AND at least one job window ≤ 7 days |
| Compliant | All tracked job windows > 7 days |

### Envoy Tracked Services
| Code | Service | Cycle |
|------|---------|-------|
| ED1 | Exterior Detail #1 | 30 days |
| ED2 | Exterior Detail #2 | 60 days |
| IHC | Interior Heavy Clean | Info only |

### PSA Tracked Services
| Code | Service | Cycle |
|------|---------|-------|
| CC | Cockpit Clean | 30 days |
| DSC | Deep Seat Clean | 30 days |
| CE | Carpet Extraction | 30 days |
| ED1 | Exterior Detail #1 | 30 days |
| ED2 | Exterior Detail #2 | 30 days |
| Lav | Lav Tank Pressure Wash | 90 days |
| IC/EC/ED3/ED4 | Various | Info only |

### Mesa Tracked Services
Cycles come from the **Service WIndow** sheet in `Mesa Debriefs.xlsx` (read dynamically by the script — edit the sheet to change a window).

| Code | Service | Cycle |
|------|---------|-------|
| IHC | Interior Heavy Clean | 45 days |
| ED | Exterior Detail | 30 days |
| DSC | Deep Seat Clean | 30 days |
| FD | Detailed Flight Deck Clean | 30 days |
| CE | Carpet Extraction | 30 days |
| RON/EC/ESS | RON Clean / Exterior Clean / Disinfection | Info only |
| FCD | Fleet Campaign Decal | **Ignored** (not shown) |

**Mesa specifics:** the Debriefs sheet has **no Location column** (PSA/Envoy do), so the dashboard shows no last-location. Service cells are recorded as `Yes. <number>`; the trailing number is not used for compliance. Flight Deck is the **last** debriefs column (after Sub ID). Tail roster comes from the **Tails** sheet (column A). No Admin/Add-Tail tab — onboard new tails by adding them to the Tails sheet.

---

## GitHub Secrets

All three compliance programs share these three secrets:

| Secret | Description |
|--------|-------------|
| `TENANT_ID` | `ede0c57f-549f-4a90-9f8c-7ea130346f95` — Microsoft Entra tenant |
| `CLIENT_ID` | `58191600-ab56-4141-bff6-806805fcbff4` — Foxtrot Report Automation app |
| `CLIENT_SECRET` | App secret — **expires every 24 months**, set a renewal reminder |

The PSA Admin tab / `manage_fleet.yml` workflow additionally requires:

| Secret | Description |
|--------|-------------|
| `SAFETYCULTURE_KEY` | SafetyCulture API token |
| `JOTFORM_KEY` | JotForm API key (Full Access, Enterprise tenant) — API host is `https://foxtrotaviation.jotform.com/API` |

The Power Automate flow URL is embedded in `psa.html` as `PA_TAIL_WEBHOOK_URL` (same value that lives in `Secrets.env` under the same name). That flow holds a GitHub PAT (stored as a secure variable inside PA), calls `workflow_dispatch` on `manage_fleet.yml`, responds to the dashboard, then appends a row to the SharePoint Tail List itself. See "PSA Admin → Add Tail" below.

The SharePoint Drive ID used by the compliance refresh scripts:  
`b!_bzXaIx86kOufgJN3ih-BaDIDthKYuxJkJtLi1Bm5irGjCEnK-VHSpBRRm3_SDKU`

---

## Workflows

### `data_refresh.yml` — Envoy
- **Trigger:** Hourly cron + manual dispatch
- **Script:** `envoy_generate_data.py`
- **Output:** commits `data.json`

### `psa_data_refresh.yml` — PSA
- **Trigger:** Hourly cron + manual dispatch
- **Script:** `psa_generate_data.py`
- **Output:** commits `psa_data.json`

### `mesa_data_refresh.yml` — Mesa
- **Trigger:** Hourly cron + manual dispatch
- **Script:** `mesa_generate_data.py`
- **Output:** commits `mesa_data.json`

### `crosswinds_refresh.yml` — Crosswinds
- **Trigger:** `workflow_dispatch` only (called by Power Automate via GitHub API)
- **Input:** `payload` — JSON array of inspections from Power Automate
- **Script:** `crosswinds_generate_data.py`
- **Output:** commits `crosswinds_data.json`

> **Note:** Every commit to main triggers `pages-build-deployment` automatically. This is expected — GitHub Pages rebuilds on every push.

---

## PSA Admin → Add Tail

The PSA dashboard has a password-gated **Admin** tab with an "Add Tail" form. Architecture:

```
psa.html (browser)
   ↓  POST {tail}
PA Flow  ──── holds GitHub PAT (secure var) ────┐
   │                                              │
   │  1. HTTP: POST workflow_dispatch  ──────────┘
   │     ↓                  (fires GH workflow async)
   │  2. Condition: dispatch succeeded?
   │     ├─ true  → Response 200 to dashboard → Add row in SharePoint Tail List
   │     └─ false → Response 502 to dashboard

GH Workflow (manage_fleet.yml, Python, runs ~30-60s):
   ├──→ SafetyCulture API   (GET set, append, PUT)
   └──→ JotForm API         (GET Q53, append, POST)
       ↓
   commits fleet_action_result.json

psa.html polls fleet_action_result.json for matching tail+timestamp,
displays a 2-row result table (SC + JotForm).
```

**Why this shape:** the dashboard is on public GitHub Pages, so a GitHub PAT can't live in `psa.html` — GitHub's secret scanner auto-revokes any PAT it finds in a public commit (verified empirically). PA's HTTP-trigger URL has its own SAS-style signature that GH doesn't scan, so the URL embedded in `psa.html` is safe. The PA flow holds the PAT server-side.

**Why PA does the SharePoint append directly** (instead of the workflow calling back to PA): the Excel "Add a row" connector in PA is the most reliable way to write to a SharePoint Excel table. The workflow Python skips SharePoint entirely; PA does it as a post-Response action.

**SharePoint failure visibility:** PA's Add-row runs **after** the Response is sent, so if it fails the dashboard won't know — the user will see SC + JotForm both succeed. Check the PA flow's run history if a tail is missing from the Tail List.

**Tails are inserted in numerical order.** The workflow sorts by the numeric portion after `N` (so `N205JK` lands between `N204NN` and `N206IR`, not after `N1999`). JotForm preserves `Not Listed` as the final option by removing it before the sort and re-appending it after. SafetyCulture's `PUT /response_sets/{id}` preserves response IDs by label-match, so reordering does not invalidate existing template bindings or historical inspection answers.

### PA Flow shape (4 actions inside the trigger)

1. **Trigger** — When a HTTP request is received. Body schema: `{ "tail": "string" }`.
2. **HTTP** — POST to `https://api.github.com/repos/sam-kosco/envoy-compliance-tracker/actions/workflows/manage_fleet.yml/dispatches`. Auth header uses the GH PAT held in a secure variable.
3. **Condition** — `outputs('HTTP')?['statusCode']` equals `204`:
   - **True branch:** Response `200 {"dispatched": true}` → Excel Online **Add a row into a table** on PSA Debriefs Tail List with `Tails = triggerBody()?['tail']`
   - **False branch:** Response `502 {"dispatched": false, github_status: ..., github_body: ...}`

### Flow contract (dashboard ↔ PA)

**Request:**
```json
POST {PA_TAIL_WEBHOOK_URL}
Content-Type: application/json

{"tail": "N205JK"}
```

The dashboard normalizes (`.trim().toUpperCase()`) and regex-validates (`^N\d{1,5}[A-Z]{0,2}$`) before sending. The workflow re-validates defensively.

**Response:** `200 {"dispatched": true}` (or `502` on dispatch failure). Fire-and-forget — the dashboard then polls `fleet_action_result.json` for a matching tail+timestamp.

### Result file format

`fleet_action_result.json` (committed by `manage_fleet.yml` after each run):

```json
{
  "tail": "N205JK",
  "action": "add_tail",
  "timestamp": "2026-06-04T15:30:00+00:00",
  "safetyculture": {"status": "ok|noop|error", "message": "..."},
  "jotform":       {"status": "ok|noop|error", "message": "..."}
}
```

The dashboard polls every 3s for up to 2 min, displaying a 2-row table (SC + JotForm) once it sees a result whose `tail` matches the submitted value and whose `timestamp` is newer than the dispatch.

---

## Troubleshooting

### Dashboard shows stale or sample data
The JSON file is missing or the last workflow run failed. Go to **Actions** → select the relevant workflow → **Run workflow** manually. For Crosswinds, use an empty payload `[]` — the script will still read SharePoint and recalculate.

### GitHub Actions fails with 401 Unauthorized
`CLIENT_SECRET` has expired. Renew in Microsoft Entra → App registrations → Foxtrot Report Automation → Certificates & secrets. Update the `CLIENT_SECRET` GitHub Secret in this repo and `sam-kosco/jsx-compliance-tracker`.

### GitHub Actions fails with 404 (file download)
The Excel workbook was moved or renamed on SharePoint. Update `FILE_PATH` in the relevant Python script and commit.

### GitHub Actions fails with 423 Locked (Crosswinds only — legacy)
The Excel file was open in Excel Online or desktop Excel when an older version of the script tried to upload. Current version is read-only so this should not occur. If it does, close the file and re-run.

### Crosswinds Excel table broken after a run
The script no longer writes to Excel — Power Automate handles all writes using native Add row / Update row actions. If the table is still being corrupted, the issue is in the Power Automate flow, not the Python script.

---

## Maintenance

| Task | When | Action |
|------|------|--------|
| Renew CLIENT_SECRET | Every 24 months | Entra → App registrations → Foxtrot Report Automation → new secret → update GitHub Secret |
| Add tail to Envoy fleet | As needed | Add to Tail List sheet in the Envoy SharePoint Excel |
| Add tail to Mesa fleet | As needed | Add to the Tails sheet (column A) in the Mesa SharePoint Excel |
| Add tail to PSA fleet | As needed | Use the PSA dashboard → Admin tab (password-gated). One submit updates SafetyCulture, JotForm, and SharePoint together. |
| Add tail to Crosswinds fleet | As needed | Add to the Crosswinds Tail Numbers global response set in SafetyCulture AND the `TAILS` list in `crosswinds_generate_data.py` |
| Pause a refresh | As needed | Comment out the `cron:` line in the relevant workflow YAML |
| Excel file moved on SharePoint | If relocated | Update `FILE_PATH` constant in the relevant Python script |

---

## Key Contacts

| Role | Name | Contact |
|------|------|---------|
| System owner / Data Analyst | Samuel Kosco | samuel.kosco@foxtrotaviation.com |
| Automation sender account | Foxtrot Automation | foxtrot.automation@foxtrotaviation.com |
