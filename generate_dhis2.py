import json
import pandas as pd
from dhis2 import generate_uid
import utils
from datetime import date
from azure.storage.blob import BlobServiceClient
from configparser import ConfigParser
import requests
from requests.auth import HTTPBasicAuth
import time


def send_payload_events(payload_events, dhis2_credentials):

    SERVER_URL = dhis2_credentials["server"]
    USERNAME = dhis2_credentials["user"]
    PASSWORD = dhis2_credentials["password"]

    url_tracker = f"{SERVER_URL}tracker?importStrategy=CREATE_AND_UPDATE&importMode=COMMIT"  # importMode could be VALIDATE or COMMIT
    response = requests.post(url_tracker, data=json.dumps(payload_events), auth=HTTPBasicAuth(USERNAME, PASSWORD),
                             headers={"Content-Type": "application/json"})

    # Creates a job, that needs follow up
    completed = False
    location = response.json()["response"]["location"]
    logger.info(f"Job URL = {location}")
    while not completed:
        response_job = requests.get(location, auth=HTTPBasicAuth(USERNAME, PASSWORD),
                                    headers={"Content-Type": "application/json"})
        logger.debug(f"Job status = {response_job.json()}")

        if (not response_job.json()) or (
                response_job.json()[0]["completed"] != True):  # I don't know why, but the comparison != True is needed
            # Wait 5 seconds
            wait_secs = 5
            time.sleep(wait_secs)
            logger.debug(f"Job not finished. Wait {wait_secs} seconds")
        else:
            completed = True
            logger.debug("Job finished")
            response_report = requests.get(location + "/report", auth=HTTPBasicAuth(USERNAME, PASSWORD),
                                           headers={"Content-Type": "application/json"})
            response_content = response_report.content.decode("utf-8")
            if response_report.json()["status"] == "OK":
                logger.info(f"Server response {response_content}")
            else:
                logger.error(f"Server response {response_content}")

    return response_report.json()


# Obtain credentials
credentials_datalake = {}
parser = ConfigParser()
parser.read("credentials.ini")
params = parser.items("ocba_datalake")

for param in params:
    credentials_datalake[param[0]] = param[1]

ACCOUNT_URL = credentials_datalake["account_url"]
SAS_TOKEN_SOURCE = credentials_datalake["sas_token_source"]
SAS_TOKEN_INDEX = credentials_datalake["sas_token_index"]
CONTAINER = credentials_datalake["container"]
BLOB_SOURCE = credentials_datalake["blob_source"]
BLOB_INDEX = credentials_datalake["blob_index"]

# Global constants
SIPCA_PROGRAM_UID = "Nep6qUpNTNn"
TODAY = date.today().strftime("%Y-%m-%d")

# Set up a logger
logger = utils.get_logger("SIPCA")  # fixed

logger.info("Starting the script")

# The SAS token string can be assigned to credential here or appended to the account URL
blob_service_client = BlobServiceClient(ACCOUNT_URL, credential=SAS_TOKEN_SOURCE)
blob_client = blob_service_client.get_blob_client(container=CONTAINER, blob=BLOB_SOURCE)

# Save the CSV from the datalake in a local file
filename_sipca = f"{TODAY}_kobo_sipca_ocba.csv"
with open(filename_sipca, mode="wb") as sample_blob:
    download_stream = blob_client.download_blob()
    sample_blob.write(download_stream.readall())

logger.info("Read CSV file from OCBA Datalake")

# DEBUG: read csv file (already converted in UTF-8)
# filename_sipca = "kobo_sipca_ocba.csv" # For debug

# Load the CSV in a pandas dataframe
df = pd.read_csv(filename_sipca, sep="~", encoding='utf-8', dtype=object)  # dtype=object from https://stackoverflow.com/questions/40750670/prevent-pandas-read-csv-from-inferring-dtypes

# load previous event UID index
# The SAS token string can be assigned to credential here or appended to the account URL
blob_service_client_index = BlobServiceClient(ACCOUNT_URL, credential=SAS_TOKEN_INDEX)
blob_client_index = blob_service_client_index.get_blob_client(container=CONTAINER, blob=BLOB_INDEX)

# Save the index (JSON) from the datalake in a local file
filename_sipca_index_initial = f"{TODAY}_event_index_initial.json"
with open(filename_sipca_index_initial, mode="wb") as blob_index:
    download_stream = blob_client_index.download_blob()
    blob_index.write(download_stream.readall())

with open(filename_sipca_index_initial, 'r') as f:
    event_index = json.load(f)

logger.info("Starting the generation of dhis2 payload")

# load DE UID mapping (including optionSets)
with open('mapping_generated.json', 'r', encoding="utf-8") as f:
    sipca_program = json.load(f)

# load OU mapping
with open('mapping_orgUnits.json', 'r', encoding="utf-8") as f:
    mapping_orgUnits = json.load(f)

retrieved_kobo_uuids = []

params = parser.items("ocba_dhis2")
credentials_dhis2 = {}
for param in params:
    credentials_dhis2[param[0]] = param[1]

# Iterate over the pandas dataframe (retrieved Kobo lines)
df = df.reset_index()  # make sure indexes pair with number of rows
for index, row in df.iterrows():
    kobo_uuid = row['_uuid']
    logger.info(f"Processing kobo row UUID={kobo_uuid} ")
    retrieved_kobo_uuids.append(kobo_uuid)

    # Get/Generate Event UID
    event_uid = None
    if kobo_uuid in event_index:
        event_uid = event_index[kobo_uuid]["dhis2_uuid"]
    else:
        event_uid = generate_uid()
        event_index[kobo_uuid] = {"dhis2_uuid": event_uid, "uploaded": False, "deleted": False}

    if event_index[kobo_uuid]["uploaded"]:
        logger.info(f"Not processing event because it was already uploaded in dhis2. [kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]")
        continue

    # Event Date, Event Complete Date
    assessment_date = row["ASSESSMENT_DATE"]

    # Get Org Unit UID from mapping
    kobo_facility = row["FACILITY"]
    if kobo_facility not in mapping_orgUnits:
        logger.error(f"Unexpected org unit {kobo_facility}. Skip and go to the next case")
        continue
    event_ou = mapping_orgUnits[row["FACILITY"]]

    # Process dataValues
    dataValues = []
    for de in sipca_program["programStages"][0]["programStageDataElements"]:
        de_uid = de["dataElement"]["id"]
        de_col_name = de["dataElement"]["column"]
        de_value = row[de_col_name]

        # Skip NA values
        if pd.isna(de_value):
            logger.debug(f"{kobo_uuid}. Skipping value (NaN) for column {de_col_name}")
            continue

        # mapping optionSet
        if "optionSet" in de["dataElement"]:
            de_value = de["dataElement"]["optionSet-mapping"][de_value]

        dataValues.append({"dataElement": de_uid, "value": de_value})

    # Generate event payload
    event_payload = {
        "event": event_uid,
        "occurredAt": assessment_date,
        "completedAt": assessment_date,
        "status": "COMPLETED",
        "orgUnit": event_ou,
        "dataValues": dataValues,
        "program": SIPCA_PROGRAM_UID
    }

    utils.save_json_file(f"./events_payload/{TODAY}__{kobo_uuid}_{event_uid}.json", event_payload)

    logger.info("Finished the generation of dhis2 payload")

    # TODO user token instead of credentials?
    logger.info("Sending events to dhis2 instance")

    # Send events to the dhis2 instance
    response_dhis2 = send_payload_events({"events": [event_payload]}, credentials_dhis2)

    # response log
    utils.save_json_file(f"./events_payload/{TODAY}__{kobo_uuid}_{event_uid}__response_event_payload.json", response_dhis2)  # Save response
    if response_dhis2["status"] == "OK":
        event_index[kobo_uuid]["uploaded"] = True
        logger.info(f"Event uploaded to dhis2 instance [kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]")
    else:
        logger.error(f"Event not uploaded to dhis2 instance [kobo uuid={kobo_uuid}] [dhis2 uuid={event_uid}]")

logger.info("Checking if any entry was deleted")
# Check if any entry was deleted in kobo. If that case, add to the log.
all_kobo_uuids = set(event_index.keys())
deleted = [x for x in all_kobo_uuids if x not in retrieved_kobo_uuids]
for x in deleted:
    event_index[x]["deleted"] = True
    logger.warning(f"Deleted in Kobo. Kobo uuid: {x}")

# Saving index
filename_sipca_index_final = f"{TODAY}_event_index_final.json"
utils.save_json_file(filename_sipca_index_final, event_index)
with open(filename_sipca_index_final, mode="rb") as blob_index:
    blob_client_index.upload_blob(blob_index, overwrite=True)

logger.info("Script finished")
