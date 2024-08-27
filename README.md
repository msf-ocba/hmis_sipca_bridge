# SIPCA Integration

This script integrates SIPCA data (from Kobo) to dhis2.

The process has 2 parts:
1. Generating the mapping file (mapping between Kobo input and dhis2 output)
2. Get the CSV source file, process it, generates dhis2 payloads and send to the server.

## Getting Started

To use these scripts, you will need to have `Python3` installed on your system, and also the Python dependencies listed in `requirements.txt`. In order to install the dependencies you should run:

`pip install -r requirements.txt`

## Mapping
This script intends to generate the mapping file that will be used for generating dhis2 payloads using the CSV from Kobo.

### Input files
- `mapping_de.json`: This file contains the mapping between Kobo columns and dhis2 event dataElements.
- `mapping_optionSet.json`: This file contains the mapping between Kobo values and dhis2 optionSet codes.
- `retrieved_program_metadata.json`: This file contains the configuration of the program (including DE and its linked optionSets).
The file is obtained calling the dhis2 API (`https://staging.hmisocba.msf.es/api/programs/Nep6qUpNTNn?fields=programStages[id,name,programStageDataElements[compulsory,dataElement[name,valueType,id,optionSet[name,id,valueType,options[name,code]]]]`)

### Output files
- `mapping_generated.json`: This file is the mapping file obtained from the mapping script.

### Run

Once the dependencies have been installed, you have to navigate to the script directory path and run the script by running:

`python generate_mapping.py`


## Generate payload & send

The steps of the script are:
1. Obtain the input CSV file (with kobo registers) and the previous event index from MSF OCBA DataLake. This CSV file was previously generated from Kobo.
2. Process the CSV and generate a dhis2 payload file (event program)
   1. If the kobo line was already uploaded (`event_index.json` is used to that check), skip and process the next one. 
3. Send the dhis2 payload to the dhis2 instance.
4. Check for deleted kobo registers comparing CSV file and event index and marks as such in event index.
5. Save the event index (used for tracking) in the OCBA Data Lake

### Input files
- From OCBA datalake: `raw/kobo_sipca_ocba.csv`: The CSV that contains the kobo registers. This file is loaded from the OCBA datalake (inside the `sipca` container).
- From OCBA datalake: `raw/event_index.json`: This file contains the index of previous kobo registers processed. This file is loaded from the OCBA datalake (inside the `sipca` container).
- `credential.ini`: Configuration file that contains the credentials for accessing the CSV file in OCBA datalake and the OCBA dhis2. As a reminder, use a `%%` to escape the `%` sign (`%` is the only character that needs to be escaped) 
- `mapping_generated.json`: This file is the mapping file obtained from the mapping script.
- `mapping_orgUnits.json`: This file contains the mapping between Kobo facilities and dhis2 organisation units uids. 

### Output files
- `events_payload/{current_date}__{kobo_uuid}_{dhis2_uuid}.json`: dhis2 payload for that particular event.
- `events_payload/{current_date}__{kobo_uuid}_{dhis2_uuid}__response_event_payload.json`: This file contains the dhis2 response of the request for sending the event.
- `log/YYYY-MM-DD_SIPCA.log`: Log file of the ETL process.
- `log/YYYY-MM-DD_kobo_sipca_ocba.csv`: Copy of the CSV file retrieved from the OCBA Datalake.
- `log/YYYY-MM-DD_event_index_initial.json`: This file contains the index of previous kobo registers. It is a backup of the file retrieved from the OCBA datalake.
- `log/YYYY-MM-DD_event_index_final.json`: This file contains the updated index of kobo registers. It is a backup of the file uploaded to the OCBA datalake.
- To OCBA datalake: `raw/event_index.json`: This file contains the index of kobo registers after the execution of the script. This file is saved in the OCBA datalake (inside the `sipca` container).

### Run

Once the dependencies have been installed, you have to navigate to the script directory path and run the script by running:

`python generate_dhis2.py`

### Notes
- The *generate mapping* script MUST be run before this script. In case there is an update in the configuration of the program, the *generate mapping* script MUST be re-run.
- The `Event Date`, `Event Complete Date` are filled with the value of the assessment date.
- All events' status are `COMPLETED`
- The file `SIPCA_mapping_file.xlsx` is used as guide for the mapping of DataElements and OptionSets.
- **Add a new org unit**. If you want to add a new org Unit, you need to update the `mapping_orgUnits.json` and include that org unit in the *Data capture and maintenance organisation units* of the dhis2 user created for synchronizing the data.
- **Deleted entries in kobo**. If a kobo entry is deleted (in kobo) and it was already uploaded to dhis2, the process generate a warning message in the log. It is expected that a responsible from OCBA remove that entry in dhis2 manually.
- **Event index**. It is a file that contains information about: kobo uuid, dhis2 uuid, if the event was already uploaded, if the event was deleted.

## Deployment
- This script (for generating the payload and send it) is deployed currently in the alert/notification/monitoring server (https://monitoring.hmisocba.msf.es/).
- The folder where the script is deployed is `/home/sipca/msf_sipca/`
- The cronjob is run every day at 3:00 UTC.
- An ad-hoc user was created in the unix machine for running the script as a cron job (username: `sipca`).
- An ad-hoc user was created in the dhis2 instance for synchronizing the data (username: `sipca-admin`).
