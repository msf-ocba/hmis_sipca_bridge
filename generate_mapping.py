import json
import utils

SIPCA_PROGRAM_UID = "Nep6qUpNTNn"
print("Starting mapping script")
# Load manually generated mapping DE file
with open('mapping_de.json', 'r', encoding="utf-8") as f:
    mapping_de_raw = json.load(f)
    mapping_de = {v: k for k, v in mapping_de_raw.items()}

# Load manually generated mapping OptionSet file
with open('mapping_optionSet.json', 'r', encoding="utf-8") as f:
    mapping_optionSet = json.load(f)

# Load retrieved file with the current DE
# Source URL: https://staging.hmisocba.msf.es/api/programs/Nep6qUpNTNn?fields=programStages[id,name,programStageDataElements[compulsory,dataElement[name,valueType,id,optionSet[name,id,valueType,options[name,code]]]]
with open('retrieved_program_metadata.json', 'r', encoding="utf-8") as f:
    programStages = json.load(f)

for ps in programStages["programStages"]:
    for de in ps["programStageDataElements"]:
        de_uid = de["dataElement"]["id"]
        if de_uid in mapping_de:
            de["dataElement"]["column"] = mapping_de[de_uid]
        else:
            print(f"ERROR. Missed in mapping DE '{de['dataElement']['name']}' ({de_uid})")
        if "optionSet" in de["dataElement"]:
            optionSet_uid = de["dataElement"]["optionSet"]["id"]
            if optionSet_uid in mapping_optionSet:
                de["dataElement"]["optionSet-mapping"] = mapping_optionSet[optionSet_uid]

mapping_generated = {"programStages": programStages["programStages"],
                     "program_id": SIPCA_PROGRAM_UID,
                     "program_name": "IPC - SIPCA"
                     }

utils.save_json_file("mapping_generated.json", mapping_generated, ident=4)
print("Mapping script finished")