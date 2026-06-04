# envoy-compliance-tracker

**Owner:** Samuel Kosco — Data Analyst, Foxtrot Aviation Services  
**Repo:** `sam-kosco/envoy-compliance-tracker`  
**Hosted at:** `sam-kosco.github.io/envoy-compliance-tracker/`

This repo hosts three separate aircraft detailing compliance dashboards for Foxtrot Aviation Services. All three share the same GitHub repository, GitHub Secrets, and Microsoft Entra app registration.

---

## Programs

| Program | Client | Dashboard URL | File |
|---------|--------|---------------|------|
| Envoy | Envoy Air | `/` (index.html) | `index.html` |
| PSA | PSA Airlines | `/psa.html` | `psa.html` |
| Crosswinds | Crosswinds Flight School | `/crosswinds.html` | `crosswinds.html` |

---

## Repository Structure

```
envoy-compliance-tracker/
├── index.html                    # Envoy compliance dashboard
├── psa.html                      # PSA compliance dashboard (incl. Admin tab)
├── crosswinds.html               # Crosswinds compliance dashboard
├── data.json                     # Envoy compliance data (auto-generated)
├── psa_data.json                 # PSA compliance data (auto-generated)
├── crosswinds_data.json          # Crosswinds compliance data (auto-generated)
├── fleet_action_result.json      # Last result from manage_fleet.yml (auto-generated)
├── envoy_generate_data.py        # Envoy data relay script
├── psa_generate_data.py          # PSA data relay script
├── crosswinds_generate_data.py   # Crosswinds data relay script
└── .github/
    └── workflows/
        ├── data_refresh.yml          # Envoy hourly cron
        ├── psa_data_refresh.yml      # PSA hourly cron
        ├── crosswinds_refresh.yml    # Crosswinds — triggered by Power Automate webhook
        └── manage_fleet.yml          # PSA admin actions (add tail) — workflow_dispatch
```

---

## How Each Program Works

### Envoy & PSA
1. Field techs submit JotForm debriefs after each service
2. Power Automate appends submissions to Excel workbooks on SharePoint
3. GitHub Actions runs hourly (`data_refresh.yml` / `psa_data_refresh.yml`)
4. Python script downloads Excel from SharePoint via Microsoft Graph API
5. Script calculates compliance windows and writes `data.json` / `psa_data.json`
6. GitHub commits the JSON; GitHub Pages serves the updated dashboard

**SharePoint source files:**
- Envoy: `Power Flows/Debriefs/Envoy Debriefs.xlsx`
- PSA: `Power Flows/Debriefs/PSA Debriefs.xlsx`

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

## Compliance Logic (Envoy & PSA)

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
| `SAFETYCULTURE_KEY` | SafetyCulture API token for the Foxtrot tenant |
| `JOTFORM_KEY` | JotForm API key (must be Full Access). API host is the Enterprise subdomain `https://foxtrotaviation.jotform.com/API`, not `api.jotform.com` |
| `PA_TAIL_WEBHOOK_URL` | Power Automate HTTP-trigger URL — receives `{"tail": "N123XX"}` and appends a row to the PSA Debriefs Tail List |

Non-secret IDs are hardcoded as env vars in `manage_fleet.yml`:
- `PSA_TAILS_SET_ID` = `responseset_0602a202a6a2458cae66ab6b46640d28`
- `JOTFORM_FORM_ID` = `213263365115146` (PSA Debrief form)
- `JOTFORM_TAIL_QID` = `53` (the Tail dropdown)

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

### `crosswinds_refresh.yml` — Crosswinds
- **Trigger:** `workflow_dispatch` only (called by Power Automate via GitHub API)
- **Input:** `payload` — JSON array of inspections from Power Automate
- **Script:** `crosswinds_generate_data.py`
- **Output:** commits `crosswinds_data.json`

### `manage_fleet.yml` — PSA Admin actions
- **Trigger:** `workflow_dispatch` only (called by the PSA dashboard's Admin tab via GitHub API)
- **Inputs:** `action` (currently only `add_tail`), `tail` (string, normalized server-side via `.strip().upper()`)
- **What it does (add_tail):** in best-effort sequence, with per-step status reporting:
  1. **SafetyCulture** — `GET /response_sets/{PSA_TAILS_SET_ID}`, append new tail, sort by numeric portion (e.g. N205JK between N204NN and N206IR), `PUT` back. PUT preserves response IDs by label-match, so existing template bindings and inspection references stay intact.
  2. **JotForm** — `GET` Q53 options, append, sort, keep `Not Listed` last, `POST` updated pipe-delimited string back.
  3. **Power Automate** — `POST` `{"tail": "N123XX"}` to the webhook; PA flow appends to the SharePoint Tail List.
- **Output:** writes per-step `{status, message}` to `fleet_action_result.json` and commits it. Dashboard polls that file by tail+timestamp to display the result. Workflow exits non-zero only if **every** step errors — partial success still passes.

> **Note:** Every commit to main triggers `pages-build-deployment` automatically. This is expected — GitHub Pages rebuilds on every push.

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
