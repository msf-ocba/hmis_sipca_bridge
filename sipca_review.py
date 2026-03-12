"""
SIPCA Event Index Periodic Review Script
=======================================
1st line support tool to:
  - Classify all records in event_index.json by transfer status
  - Surface the exact failure reason written by generate_dhis2.py
  - Track deleted_dhis2 field (Potential improvement from docs)
  - Generate a timestamped weekly summary report (.txt)
  - Send email notification if action is required/not required (requires SMTP config)

Usage:
    python sipca_review.py

Output files:
    event_index.json              -> updated in-place with deleted_dhis2 field
    sipca_report_<date>.txt       -> weekly summary report
"""

import json
import smtplib
import os
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# EMAIL CONFIG — loaded from credentials.ini [email] section.
EMAIL_SUBJECT = "SIPCA Report Review"

def _load_email_config(credentials_path="credentials.ini"):
    """Read email settings from credentials.ini. Returns a dict or None if unavailable."""
    from configparser import ConfigParser
    parser = ConfigParser()
    parser.read(credentials_path)
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

# FILE PATHS
EVENT_INDEX_PATH = "event_index.json"
REPORT_DIR       = "."


# STEP 1: Load event_index.json
def load_event_index(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# STEP 2: Classify records
def classify_records(data):
    """
    Classifies each record and ensures the deleted_dhis2 field exists.

    Statuses:
      NOT_UPLOADED_TO_DHIS2           — deleted=false, uploaded=false
        Sub-reason from generate_dhis2.py 'error' field:
          UNKNOWN_OU:<facility>       — missing OU mapping (most common)
          DHIS2_REJECTED              — DHIS2 returned a non-OK status
          NO_RESPONSE_FROM_DHIS2      — network/server error during transfer
      DELETED_IN_KOBO_BUT_EXISTS_IN_DHIS2 — deleted=true, uploaded=true, deleted_dhis2=false
      DELETED_IN_KOBO_AND_DHIS2       — deleted=true, uploaded=true, deleted_dhis2=true
      OK / NO_ACTION                  — everything else
    """
    action_required  = {}
    no_action        = {}
    already_handled  = {}

    for record_id, record in data.items():
        deleted       = record.get("deleted", False)
        uploaded      = record.get("uploaded", False)

        # Ensure deleted_dhis2 field always exists (backward compatibility)
        if "deleted_dhis2" not in record:
            record["deleted_dhis2"] = False
        deleted_dhis2 = record["deleted_dhis2"]

        if not deleted and not uploaded:
            record["status"] = "NOT_UPLOADED_TO_DHIS2"
            action_required[record_id] = record

        elif deleted and uploaded and not deleted_dhis2:
            record["status"] = "DELETED_IN_KOBO_BUT_EXISTS_IN_DHIS2"
            action_required[record_id] = record

        elif deleted and uploaded and deleted_dhis2:
            record["status"] = "DELETED_IN_KOBO_AND_DHIS2"
            already_handled[record_id] = record

        else:
            no_action[record_id] = record

    return action_required, no_action, already_handled

# STEP 3: Save updated event_index.json with deleted_dhis2 field added where missing
def save_event_index(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"[OK] event_index.json updated -> {path}")


# STEP 4: Build report
def format_action(record):
    """Return a plain-text action line based on the error field written by generate_dhis2.py."""
    error = record.get("error", "")
    if error.startswith("UNKNOWN_OU:"):
        facility = error.split(":", 1)[1]
        return (
            f"-> MISSING OU MAPPING for facility: {facility}\n"
            f"   Add '{facility}' to mapping_orgUnits.json, assign the SIPCA IPD\n"
            f"   program to the OU in DHIS2, and grant sipca-admin Data Capture access."
        )
    elif error == "DHIS2_REJECTED":
        return (
            "-> DHIS2 REJECTED the event payload.\n"
            "   Check the response file in events_payload/ for the DHIS2 error detail.\n"
            "   Common causes: invalid data value, missing compulsory DE, or program config issue."
        )
    elif error == "NO_RESPONSE_FROM_DHIS2":
        return (
            "-> NO RESPONSE from DHIS2 — possible network or server error.\n"
            "   Check DHIS2 availability and network connectivity on the server."
        )
    else:
        return (
            "-> Check logs for missing OU mapping, sipca-admin access,\n"
            "   or SIPCA IPD program assignment to OU."
        )


def build_report(data, action_required, no_action, already_handled, run_ts):
    lines = []
    lines.append("=" * 65)
    lines.append("  SIPCA REPORT REVIEW")
    lines.append(f"  Run timestamp : {run_ts}")
    lines.append("=" * 65)

    lines.append("\n-- SUMMARY " + "-" * 54)
    lines.append(f"  Total records              : {len(data)}")
    lines.append(f"  No action needed           : {len(no_action)}")
    lines.append(f"  Already handled            : {len(already_handled)}  (deleted in Kobo & DHIS2)")
    lines.append(f"  Action required            : {len(action_required)}")

    # Break down action_required by sub-type
    unknown_ou  = {k: v for k, v in action_required.items() if str(v.get("error","")).startswith("UNKNOWN_OU")}
    rejected    = {k: v for k, v in action_required.items() if v.get("error") == "DHIS2_REJECTED"}
    no_response = {k: v for k, v in action_required.items() if v.get("error") == "NO_RESPONSE_FROM_DHIS2"}
    deleted_dhis2_needed = {k: v for k, v in action_required.items() if v.get("status") == "DELETED_IN_KOBO_BUT_EXISTS_IN_DHIS2"}
    other       = {k: v for k, v in action_required.items()
                   if k not in unknown_ou and k not in rejected
                   and k not in no_response and k not in deleted_dhis2_needed}

    if action_required:
        lines.append(f"    of which:")
        if unknown_ou:
            # Group by facility
            facilities = {}
            for v in unknown_ou.values():
                fac = str(v.get("error","")).split(":",1)[1] if ":" in str(v.get("error","")) else "unknown"
                facilities[fac] = facilities.get(fac, 0) + 1
            for fac, cnt in facilities.items():
                lines.append(f"      - Unknown OU '{fac}': {cnt} record(s)")
        if rejected:
            lines.append(f"      - DHIS2 rejected: {len(rejected)} record(s)")
        if no_response:
            lines.append(f"      - No DHIS2 response: {len(no_response)} record(s)")
        if deleted_dhis2_needed:
            lines.append(f"      - Deleted in Kobo, still in DHIS2: {len(deleted_dhis2_needed)} record(s)")
        if other:
            lines.append(f"      - Other / unknown reason: {len(other)} record(s)")

    if action_required:
        lines.append("\n-- RECORDS REQUIRING ACTION " + "-" * 37)
        for rid, record in action_required.items():
            status = record.get("status", "UNKNOWN")
            error  = record.get("error", "—")
            lines.append(f"\n  ID           : {rid}")
            lines.append(f"  Status       : {status}")
            lines.append(f"  deleted      : {record.get('deleted')}")
            lines.append(f"  uploaded     : {record.get('uploaded')}")
            lines.append(f"  dhis2_uuid   : {record.get('dhis2_uuid', 'N/A')}")
            lines.append(f"  deleted_dhis2: {record.get('deleted_dhis2')}")
            lines.append(f"  error        : {error}")
            lines.append(f"  {format_action(record)}")
    else:
        lines.append("\n  All records are in order. No action needed this week.")

    if already_handled:
        lines.append("\n-- ALREADY HANDLED (deleted in Kobo & DHIS2) " + "-" * 19)
        for rid, record in already_handled.items():
            lines.append(f"  ID: {rid}  |  dhis2_uuid: {record.get('dhis2_uuid', 'N/A')}")

    lines.append("\n" + "=" * 65)
    return "\n".join(lines)


# STEP 5: Save report file
def save_report(report_text, run_ts, report_dir):
    date_str = run_ts[:10]
    filename = f"sipca_report_{date_str}.txt"
    filepath = os.path.join(report_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"[OK] Report saved -> {filepath}")
    return filepath


# STEP 6: Email report if action is required/not required (requires SMTP config)
def send_email(report_text, action_count, email_cfg):
    if not email_cfg or not email_cfg.get("enabled"):
        print("[i] Email disabled. Set enabled=true in credentials.ini [email] section to activate.")
        return
    
    #Enable this if you only want to send the report via email if action is required.
    """
    if action_count == 0:
        print("[i] No action required — email not sent.")
        return
    """    
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = EMAIL_SUBJECT
        msg["From"]    = email_cfg["email_from"]
        msg["To"]      = ", ".join(email_cfg["email_to"])
        msg.attach(MIMEText(report_text, "plain"))
        with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
            server.starttls()
            server.login(email_cfg["smtp_user"], email_cfg["smtp_password"])
            server.sendmail(email_cfg["email_from"], email_cfg["email_to"], msg.as_string())
        print(f"[OK] Email sent to: {', '.join(email_cfg['email_to'])}")
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")


# MAIN
def main():
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n{'='*65}")
    print(f"  SIPCA Report Review  —  {run_ts}")
    print(f"{'='*65}\n")

    data = load_event_index(EVENT_INDEX_PATH)
    action_required, no_action, already_handled = classify_records(data)
    save_event_index(EVENT_INDEX_PATH, data)

    report_text = build_report(data, action_required, no_action, already_handled, run_ts)
    print(report_text)

    save_report(report_text, run_ts, REPORT_DIR)
    email_cfg = _load_email_config()
    send_email(report_text, len(action_required), email_cfg)


if __name__ == "__main__":
    main()