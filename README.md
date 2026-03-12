# SIPCA Integration

This project integrates SIPCA IPD survey data (from Kobo) into DHIS2.

There are three scripts:

| Script | Purpose | Who runs it |
|---|---|---|
| `generate_mapping.py` | Generates the DE/OptionSet mapping file. Run once, or whenever the DHIS2 program config changes. | Technician |
| `generate_dhis2.py` | Main transfer script. Downloads CSV from Datalake, uploads events to DHIS2, updates `event_index.json`. Run daily via cron. | Automated (cron) |
| `sipca_review.py` | Weekly review tool. Reads `event_index.json`, classifies all records, reports issues, optionally sends email alerts. | 1st line support |

---

## Virtual Environment

Modern Linux distributions implement PEP 668, which prevents pip from installing into the system Python. Use a virtual environment:

```bash
# 1. Create the environment
python3 -m venv sipca_env

# 2. Activate it
source sipca_env/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Credentials

Copy `credentials-template.ini` to `credentials.ini` and fill in the values:

```bash
cp credentials-template.ini credentials.ini
```

> **Note:** Use `%%` to escape the `%` character in `.ini` files (e.g. in SAS tokens).

---

## 1. Generate Mapping (`generate_mapping.py`)

Generates `mapping_generated.json` — the mapping between Kobo columns and DHIS2 data elements, including optionSet value mappings.

### Input files
| File | Description |
|---|---|
| `mapping_de.json` | Maps Kobo CSV column names to DHIS2 data element UIDs |
| `mapping_optionSet.json` | Maps Kobo values to DHIS2 optionSet codes |
| `retrieved_program_metadata.json` | DHIS2 program metadata (fetched from the DHIS2 API — see URL below) |

Fetch `retrieved_program_metadata.json` from:
```
https://staging.hmisocba.msf.es/api/programs/Nep6qUpNTNn?fields=programStages[id,name,programStageDataElements[compulsory,dataElement[name,valueType,id,optionSet[name,id,valueType,options[name,code]]]]]
```

### Output files
| File | Description |
|---|---|
| `mapping_generated.json` | Combined mapping file used by `generate_dhis2.py` |

### Run
```bash
python generate_mapping.py
```

> **Important:** This script must be run before `generate_dhis2.py`. Re-run it whenever the DHIS2 program configuration changes

---

## 2. Transfer Script (`generate_dhis2.py`)

Main ETL script. Steps:
1. Downloads `kobo_sipca_ocba.csv` and `event_index.json` from the OCBA Datalake
2. For each Kobo row:
   - Reserves or reuses a DHIS2 event UID
   - Skips records already uploaded
   - Skips records whose facility is not in `mapping_orgUnits.json` (logs the error and writes the reason to `event_index.json`)
   - Builds a DHIS2 event payload and sends it
   - Marks `uploaded: true` on success
3. Flags records deleted in Kobo
4. Saves and uploads the updated `event_index.json` to the Datalake
5. Logs an end-of-run summary, including all unknown OUs grouped by facility name
6. Sends an alert email if issues were detected (configure the `[email]` section in `credentials.ini`)

### Input files
| File | Source | Description |
|---|---|---|
| `raw/kobo_sipca_ocba.csv` | OCBA Datalake (`sipca` container) | Kobo survey responses |
| `raw/event_index.json` | OCBA Datalake (`sipca` container) | Transfer log / state tracker |
| `credentials.ini` | Local | Azure Datalake and DHIS2 credentials |
| `mapping_generated.json` | Local (output of `generate_mapping.py`) | DE and optionSet mapping |
| `mapping_orgUnits.json` | Local | Kobo facility label → DHIS2 OU uid mapping |

### Output files
| File | Description |
|---|---|
| `events_payload/{date}__{kobo_uuid}_{dhis2_uuid}.json` | DHIS2 event payload for each record |
| `events_payload/{date}__{kobo_uuid}_{dhis2_uuid}__response_event_payload.json` | DHIS2 API response |
| `log/YYYY-MM-DD_SIPCA.log` | Full run log |
| `log/YYYY-MM-DD_kobo_sipca_ocba.csv` | Backup of the downloaded CSV |
| `log/YYYY-MM-DD_event_index_initial.json` | Backup of event_index.json before the run |
| `log/YYYY-MM-DD_event_index_final.json` | Backup of event_index.json after the run |
| `raw/event_index.json` (Datalake) | Updated state tracker uploaded back to Datalake |

### The `event_index.json` format

Each record in `event_index.json` has the following fields:

| Field | Type | Description |
|---|---|---|
| `dhis2_uuid` | string | DHIS2 event UID reserved for this Kobo record |
| `uploaded` | boolean | `true` if successfully uploaded to DHIS2 |
| `deleted` | boolean | `true` if the survey was deleted in Kobo |
| `deleted_dhis2` | boolean | `true` if the event has been manually deleted from DHIS2 (set manually by 1st line support after confirming with the referent) |
| `error` | string | Written by the transfer script when a record fails. Values: `UNKNOWN_OU:<facility>`, `DHIS2_REJECTED`, `NO_RESPONSE_FROM_DHIS2`. Cleared automatically on next successful upload. |

### Status interpretation

| deleted | uploaded | deleted_dhis2 | Meaning |
|---|---|---|---|
| false | true | — | Uploaded correctly. Nothing to do. |
| false | false | — | Not uploaded. Check `error` field for reason. |
| true | true | false | Deleted in Kobo, still in DHIS2. Confirm with referent and delete manually. Set `deleted_dhis2: true` afterwards. |
| true | true | true | Deleted from both. Already resolved. |
| true | false | — | Deleted in Kobo, never reached DHIS2. Nothing to do. |

### Adding a new OU mapping

When a new facility starts using the SIPCA IPD form in Kobo:

1. Add the mapping to `mapping_orgUnits.json` on the EC2 server
2. Update the Excel file on SharePoint: `2024.05.27_Kobo - DHIS2 mapping.xlsx` (tab: SIPCA IPD KOBO Facilities)
3. In DHIS2:
   - Assign the **SIPCA IPD program** to the new OU
   - Add the OU to the **Data Capture tree** of the `sipca-admin` account

> Modifying or deleting an existing mapping is a **Technician task**.

### Run
```bash
python generate_dhis2.py
```

---

## 3. Periodic Review (`sipca_review.py`)

Run by 1st line support every week after downloading `event_index.json` from the Azure sipca container.

Features:
- Classifies all records and surfaces the exact failure reason from the `error` field written by `generate_dhis2.py`
- Initialises the `deleted_dhis2` field on all records (backward compatible with older `event_index.json` files)
- Saves a timestamped report: `sipca_report_YYYY-MM-DD.txt`
- Optionally sends an email alert when action is required (configure the `[email]` section in `credentials.ini` — see `credentials-template.ini`)

### Input files
| File | Description |
|---|---|
| `event_index.json` | Downloaded from the Azure `sipca` container before each run |
| `credentials.ini` | Read for email config (`[email]` section) — optional |

### Output files
| File | Description |
|---|---|
| `event_index.json` | Updated in-place: `deleted_dhis2` field added to any record missing it |
| `sipca_report_YYYY-MM-DD.txt` | Timestamped plain-text report saved alongside the script |

### How to use
1. Download `event_index.json` from the Azure `sipca` container to the script folder
2. Run:

```bash
python sipca_review.py
```

3. Review the terminal output and the generated report file
4. Follow the action instructions for any flagged records (see status table above)
5. If a `DELETED_IN_KOBO_BUT_EXISTS_IN_DHIS2` record has been cleaned up in DHIS2, manually set `deleted_dhis2: true` in `event_index.json` and re-upload it to the Azure container

A clean result looks like:
```
Total records              : 106
No action needed           : 106
Already handled            : 0
Action required            : 0
```

---

## Deployment

- **Server:** Alert/Notification/Monitoring server (`https://monitoring.hmisocba.msf.es/`)
- **Folder:** `/home/sipca/msf_sipca/`
- **User:** `sipca` (dedicated Unix user)
- **DHIS2 user:** `sipca-admin`
- **Cron schedule:** Daily at 03:00 UTC

```bash
/bin/bash -c "cd /home/sipca/msf_sipca/ && source /home/sipca/msf_sipca/sipca_env/bin/activate && python3 generate_dhis2.py" >> /home/sipca/msf_sipca/cron_log.log 2>&1
```

---

## Security & .gitignore

This repository is public. The following files contain secrets and **must never be committed**:

```
credentials.ini
log/
events_payload/
*.log
```

A `.gitignore` covering these is included in the repository. `credentials-template.ini` (no real values) is the only credentials file that should be committed. Never put passwords, SAS tokens, or SMTP credentials anywhere in source code — always use `credentials.ini`.

---

## Notes

- All events are sent with status `COMPLETED`
- `Event Date` and `Event Complete Date` are both set to the Kobo `ASSESSMENT_DATE`
- The file `SIPCA_mapping_file.xlsx` serves as the reference guide for DE and OptionSet mappings
- The `error` field in `event_index.json` is written automatically by `generate_dhis2.py` and cleared when a record is successfully uploaded — it does not need to be edited manually