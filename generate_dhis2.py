import json
import pandas as pd
import string, secrets  # stdlib UID generation — replaces dhis2.py dependency
import utils
from datetime import date, datetime, timezone
from azure.storage.blob import BlobServiceClient
from configparser import ConfigParser
import requests
from requests.auth import HTTPBasicAuth
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# DHIS2 UID GENERATION (11 chars, first char alpha, rest alphanumeric)
_UID_ALPHA    = string.ascii_letters
_UID_ALPHANUM = string.ascii_letters + string.digits

def _generate_dhis2_uid():
    first = secrets.choice(_UID_ALPHA)
    rest  = "".join(secrets.choice(_UID_ALPHANUM) for _ in range(10))
    return first + rest

# EMAIL CONFIG — loaded from credentials.ini [email] section.
EMAIL_SUBJECT = "SIPCA Transfer — Issues Detected"

def _load_email_config(parser):
    """Read email settings from credentials.ini. Returns a dict or None if section missing."""
    if not parser.has_section("email"):
        return None
    cfg = dict(parser.items("email"))
    return {
        "enabled":      cfg.get("enabled", "false").strip().lower() == "true",
        "smtp_host":    cfg.get("smtp_host", ""),
        "smtp_port":    int(cfg.get("smtp_port", 587)),
        "smtp_user":    cfg.get("smtp_user", ""),
        "smtp_password":cfg.get("smtp_password", ""),
        "email_from":   cfg.get("email_from", ""),
        "email_to":     [e.strip() for e in cfg.get("email_to", "").split(",") if e.strip()],
    }


def send_alert_email(subject, body, email_cfg):
    """Send an alert email using config loaded from credentials.ini."""
    if not email_cfg or not email_cfg.get("enabled"):
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = email_cfg["email_from"]
        msg["To"]      = ", ".join(email_cfg["email_to"])
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
            server.starttls()
            server.login(email_cfg["smtp_user"], email_cfg["smtp_password"])
            server.sendmail(email_cfg["email_from"], email_cfg["email_to"], msg.as_string())
        logger.info(f"Alert email sent to: {', '.join(email_cfg['email_to'])}")
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")


def send_payload_events(payload_events, dhis2_credentials):
    """Send a batch of events to the DHIS2 tracker API and wait for the job to complete."""
    SERVER_URL = dhis2_credentials["server"]
    USERNAME   = dhis2_credentials["user"]
    PASSWORD   = dhis2_credentials["password"]

    url_tracker = f"{SERVER_URL}tracker?importStrategy=CREATE_AND_UPDATE&importMode=COMMIT"
    try:
        response = requests.post(
            url_tracker,
            data=json.dumps(payload_events),
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to POST to DHIS2 tracker: {e}")
        return None

    # DHIS2 creates an async job — poll until complete
    completed = False
    location  = response.json()["response"]["location"]
    logger.info(f"Job URL = {location}")

    while not completed:
        try:
            response_job = requests.get(
                location,
                auth=HTTPBasicAuth(USERNAME, PASSWORD),
                headers={"Content-Type": "application/json"},
                timeout=30
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to poll job status: {e}")
            return None

        logger.debug(f"Job status = {response_job.json()}")

        if (not response_job.json()) or (response_job.json()[0]["completed"] != True):
            wait_secs = 5
            time.sleep(wait_secs)
            logger.debug(f"Job not finished. Waiting {wait_secs}s")
        else:
            completed = True
            logger.debug("Job finished")
            try:
                response_report = requests.get(
                    location + "/report",
                    auth=HTTPBasicAuth(USERNAME, PASSWORD),
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )
                response_content = response_report.content.decode("utf-8")
                if response_report.json()["status"] == "OK":
                    logger.info(f"Server response {response_content}")
                else:
                    logger.error(f"Server response {response_content}")
                return response_report.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to retrieve job report: {e}")
                return None

    return None


# CREDENTIALS loading from credentials.ini (no secrets in source code)
credentials_datalake = {}
parser = ConfigParser()
parser.read("credentials.ini")
params = parser.items("ocba_datalake")
for param in params:
    credentials_datalake[param[0]] = param[1]

ACCOUNT_URL      = credentials_datalake["account_url"]
SAS_TOKEN_SOURCE = credentials_datalake["sas_token_source"]
SAS_TOKEN_INDEX  = credentials_datalake["sas_token_index"]
CONTAINER        = credentials_datalake["container"]
BLOB_SOURCE      = credentials_datalake["blob_source"]
BLOB_INDEX       = credentials_datalake["blob_index"]

# GLOBAL CONSTANTS
SIPCA_PROGRAM_UID = "Nep6qUpNTNn"
TODAY             = date.today().strftime("%Y-%m-%d")
RUN_TIMESTAMP     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# LOGGER
logger = utils.get_logger("SIPCA")
logger.info("=" * 60)
logger.info(f"Starting the script  [{RUN_TIMESTAMP}]")
logger.info("=" * 60)

# DOWNLOAD: CSV from datalake
blob_service_client = BlobServiceClient(ACCOUNT_URL, credential=SAS_TOKEN_SOURCE)
blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=BLOB_SOURCE)

filename_sipca = f"./log/{TODAY}_kobo_sipca_ocba.csv"
with open(filename_sipca, mode="wb") as sample_blob:
    download_stream = blob_client.download_blob()
    sample_blob.write(download_stream.readall())

logger.info("Downloaded CSV from OCBA Datalake")

# Load CSV into pandas dataframe
df = pd.read_csv(filename_sipca, sep="~", encoding="utf-8", dtype=object)

# DOWNLOAD: event_index.json from datalake (or create if not exists)
blob_service_client_index = BlobServiceClient(ACCOUNT_URL, credential=SAS_TOKEN_INDEX)
blob_client_index = blob_service_client_index.get_blob_client(container=CONTAINER, blob=BLOB_INDEX)

filename_sipca_index_initial = f"./log/{TODAY}_event_index_initial.json"
with open(filename_sipca_index_initial, mode="wb") as blob_index:
    download_stream = blob_client_index.download_blob()
    blob_index.write(download_stream.readall())

with open(filename_sipca_index_initial, "r", encoding="utf-8") as f:
    event_index = json.load(f)

# Ensure all existing records have the deleted_dhis2 field (backward compatibility)
for record in event_index.values():
    if "deleted_dhis2" not in record:
        record["deleted_dhis2"] = False

logger.info("Downloaded event_index.json from OCBA Datalake")

# LOAD MAPPINGS
with open("mapping_generated.json", "r", encoding="utf-8") as f:
    sipca_program = json.load(f)

with open("mapping_orgUnits.json", "r", encoding="utf-8") as f:
    mapping_orgUnits = json.load(f)

# DHIS2 CREDENTIALS
params = parser.items("ocba_dhis2")
credentials_dhis2 = {}
for param in params:
    credentials_dhis2[param[0]] = param[1]

# Load email config from credentials.ini (no secrets in source code)
email_cfg = _load_email_config(parser)

# MAIN PROCESSING LOOP
logger.info("Starting DHIS2 payload generation and transfer")

retrieved_kobo_uuids = []

# Collect unknown OUs across the whole run — report them once at the end
unknown_org_units = {}   # {kobo_facility: [kobo_uuid, ...]}

# Run-level counters for the end-of-run summary
count_already_uploaded = 0
count_uploaded_ok      = 0
count_upload_failed    = 0
count_unknown_ou       = 0
count_new              = 0

df = df.reset_index()
for index, row in df.iterrows():
    kobo_uuid = row["_uuid"]
    logger.info(f"Processing kobo row UUID={kobo_uuid}")
    retrieved_kobo_uuids.append(kobo_uuid)

    # ── Get or reserve a DHIS2 event UID ─────────────────────
    if kobo_uuid in event_index:
        event_uid = event_index[kobo_uuid]["dhis2_uuid"]
    else:
        event_uid = _generate_dhis2_uid()
        event_index[kobo_uuid] = {
            "dhis2_uuid":    event_uid,
            "uploaded":      False,
            "deleted":       False,
            "deleted_dhis2": False
        }
        count_new += 1

    # ── Skip if already uploaded ──────────────────────────────
    if event_index[kobo_uuid]["uploaded"]:
        logger.info(
            f"Skipping — already uploaded to DHIS2. "
            f"[kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]"
        )
        count_already_uploaded += 1
        continue

    # ── Resolve Org Unit ──────────────────────────────────────
    kobo_facility = row["FACILITY"]
    if kobo_facility not in mapping_orgUnits:
        logger.error(
            f"Unknown org unit '{kobo_facility}' for kobo uuid={kobo_uuid}. "
            f"Skipping — add this facility to mapping_orgUnits.json to fix."
        )
        # Track all affected UUIDs per unknown OU for the end-of-run report
        unknown_org_units.setdefault(kobo_facility, []).append(kobo_uuid)
        # Record the failure reason in the event index for sipca_review.py
        event_index[kobo_uuid]["error"] = f"UNKNOWN_OU:{kobo_facility}"
        count_unknown_ou += 1
        continue
    else:
        # Clear any previous error if the OU is now resolved
        event_index[kobo_uuid].pop("error", None)

    event_ou = mapping_orgUnits[kobo_facility]

    # ── Assessment date ───────────────────────────────────────
    assessment_date = row["ASSESSMENT_DATE"]

    # ── Build dataValues ──────────────────────────────────────
    dataValues = []
    for de in sipca_program["programStages"][0]["programStageDataElements"]:
        de_uid      = de["dataElement"]["id"]
        de_col_name = de["dataElement"]["column"]
        de_value    = row[de_col_name]

        if pd.isna(de_value):
            logger.debug(f"{kobo_uuid}: skipping NaN value for column {de_col_name}")
            continue

        if "optionSet" in de["dataElement"]:
            de_value = de["dataElement"]["optionSet-mapping"][de_value]

        dataValues.append({"dataElement": de_uid, "value": de_value})

    # ── Build event payload ───────────────────────────────────
    event_payload = {
        "event":       event_uid,
        "occurredAt":  assessment_date,
        "completedAt": assessment_date,
        "status":      "COMPLETED",
        "orgUnit":     event_ou,
        "dataValues":  dataValues,
        "program":     SIPCA_PROGRAM_UID
    }

    utils.save_json_file(
        f"./events_payload/{TODAY}__{kobo_uuid}_{event_uid}.json",
        event_payload
    )
    logger.info(f"Payload generated for kobo uuid={kobo_uuid}")

    # ── Send to DHIS2 ─────────────────────────────────────────
    logger.info(f"Sending event to DHIS2 [kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]")
    response_dhis2 = send_payload_events({"events": [event_payload]}, credentials_dhis2)

    if response_dhis2 is None:
        logger.error(
            f"No response received from DHIS2 — network or server error. "
            f"[kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]"
        )
        event_index[kobo_uuid]["error"] = "NO_RESPONSE_FROM_DHIS2"
        count_upload_failed += 1
        continue

    utils.save_json_file(
        f"./events_payload/{TODAY}__{kobo_uuid}_{event_uid}__response_event_payload.json",
        response_dhis2
    )

    if response_dhis2["status"] == "OK":
        event_index[kobo_uuid]["uploaded"] = True
        event_index[kobo_uuid].pop("error", None)   # clear any previous error
        logger.info(
            f"Event uploaded successfully. "
            f"[kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]"
        )
        count_uploaded_ok += 1
    else:
        event_index[kobo_uuid]["error"] = "DHIS2_REJECTED"
        logger.error(
            f"DHIS2 rejected the event. "
            f"[kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]"
        )
        count_upload_failed += 1

# DELETION CHECK
logger.info("Checking for entries deleted in Kobo")
all_kobo_uuids = set(event_index.keys())
deleted_uuids  = [x for x in all_kobo_uuids if x not in retrieved_kobo_uuids]
count_deleted  = 0
for x in deleted_uuids:
    if not event_index[x].get("deleted", False):
        logger.warning(
            f"Newly detected deletion in Kobo. kobo uuid={x} | "
            f"dhis2 uuid={event_index[x].get('dhis2_uuid','?')} | "
            f"was uploaded={event_index[x].get('uploaded', False)}"
        )
    event_index[x]["deleted"] = True
    count_deleted += 1

# END-OF-RUN SUMMARY (logged + emailed if issues found)
total_rows = len(df)

summary_lines = [
    "=" * 60,
    f"  SIPCA TRANSFER RUN SUMMARY — {RUN_TIMESTAMP}",
    "=" * 60,
    f"  Total Kobo rows processed : {total_rows}",
    f"  New records registered    : {count_new}",
    f"  Already uploaded (skipped): {count_already_uploaded}",
    f"  Uploaded successfully     : {count_uploaded_ok}",
    f"  Upload failed             : {count_upload_failed}",
    f"  Unknown OU (skipped)      : {count_unknown_ou}",
    f"  Deleted in Kobo           : {count_deleted}",
]

if unknown_org_units:
    summary_lines.append("")
    summary_lines.append("  UNKNOWN ORG UNITS — add these to mapping_orgUnits.json:")
    for facility, uuids in unknown_org_units.items():
        summary_lines.append(f"    Facility : {facility}")
        summary_lines.append(f"    Affected : {len(uuids)} record(s)")
        for uid in uuids:
            summary_lines.append(f"      kobo uuid: {uid}")

if count_upload_failed > 0:
    summary_lines.append("")
    summary_lines.append("  UPLOAD FAILURES — check events_payload response files:")
    for kobo_uuid, record in event_index.items():
        if record.get("error") == "DHIS2_REJECTED" or record.get("error") == "NO_RESPONSE_FROM_DHIS2":
            summary_lines.append(
                f"    kobo uuid={kobo_uuid} | dhis2 uuid={record.get('dhis2_uuid','?')} | error={record.get('error')}"
            )

summary_lines.append("=" * 60)
summary_text = "\n".join(summary_lines)

for line in summary_lines:
    if "UNKNOWN" in line or "FAILED" in line or "failed" in line:
        logger.error(line.strip())
    elif line.strip():
        logger.info(line.strip())

# Send alert email if there are any issues
issues_found = count_unknown_ou > 0 or count_upload_failed > 0
if issues_found:
    send_alert_email(EMAIL_SUBJECT, summary_text, email_cfg)

# SAVE & UPLOAD event_index.json
filename_sipca_index_final = f"./log/{TODAY}_event_index_final.json"
utils.save_json_file(filename_sipca_index_final, event_index)
with open(filename_sipca_index_final, mode="rb") as blob_index:
    blob_client_index.upload_blob(blob_index, overwrite=True)

logger.info("event_index.json saved and uploaded to OCBA Datalake")
logger.info("Script finished")