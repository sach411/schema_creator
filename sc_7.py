import os

import pandas as pd
import json
from datetime import datetime

# Define constants for column names
COLUMN_PARENT_TAG = 'parentTag'
COLUMN_OV_NAME = 'ovName'
COLUMN_DESCRIPTION = 'description'
COLUMN_TYPE = 'type'
COLUMN_PRIMARY_KEY = 'primaryKey'
COLUMN_SOURCE_NAME = 'sourceName'
COLUMN_SOURCE_TYPE = 'sourceType'
COLUMN_SOURCE_ATTRIBUTE = 'sourceAttribute'
COLUMN_UNIQUEITEMS = "uniqueItems"

# Define constants for JSON elements
NODE_TITLE = 'title'
NODE_DESCRIPTION = 'description'
NODE_TYPE = 'type'
NODE_ARRAY = 'array'
NODE_ITEMS = 'items'
NODE_PROPERTIES = 'properties'
NODE_X_COLLIBRA = 'x-collibra'
NODE_PRIMARY_KEY = 'primaryKey'
NODE_SOURCES = 'sources'
NODE_SOURCE_NAME = 'sourceName'
NODE_SOURCE_TYPE = 'sourceType'
NODE_SOURCE_ATTRIBUTE = 'sourceAttribute'
NODE_OBJECT = "object"
NODE_UNIQUEITEMS = "uniqueItems"
BASIC_TYPES = ["string", "boolean", "number"]  # other than "array", "object"
COMPLEX_TYPE = ["array", "object"]

WARN_1_ARRAY_NO_SOURCE_DEFINED = []
ERROR_1_ARRAY_NO_TYPE_DEFINED = []
ERRORS_WARNINGS=[{"WARN_1_ARRAY_NO_SOURCE_DEFINED":WARN_1_ARRAY_NO_SOURCE_DEFINED},
                 {"ERROR_1_ARRAY_NO_TYPE_DEFINED": ERROR_1_ARRAY_NO_TYPE_DEFINED}]

def csv_to_json(csv_file_path, json_file_path):
    # Read the CSV file using pandas and specify column types as string
    df = pd.read_csv(csv_file_path, dtype=str)

    # Identify duplicate rows based on selected columns
    duplicates = df[df.duplicated(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], keep=False)]

    # Drop duplicate rows based on selected columns
    df.drop_duplicates(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], inplace=True)

    # Create a dictionary to store the JSON data
    json_data = {}

    records_to_process = int(os.getenv('n', -1))
    if records_to_process > 0:
        df = df.head(records_to_process)

    process_records = os.getenv('process_records')
    if process_records:
        df = df[df[COLUMN_OV_NAME].isin((process_records.split(','))) |
        df[COLUMN_PARENT_TAG].isin(process_records.split(','))]

    df_rows, df_columns = df.shape
    print(f"Input file: {csv_file_path} \nProcessing {df_rows} records: {df[COLUMN_OV_NAME].to_list()}")

    # Iterate over each row in the DataFrame
    for rownum, row in df.iterrows():

        parent_tag = row[COLUMN_PARENT_TAG]
        ov_name = row[COLUMN_OV_NAME]
        description = row[COLUMN_DESCRIPTION].strip() if not pd.isna(row[COLUMN_DESCRIPTION]) else ""
        ov_type = row[COLUMN_TYPE]
        # actual type of the ov node
        ovnt = ov_type.split('.')
        ov_node_type = ovnt[0]
        # for array, subtype can be object or basic types
        ov_node_subtype = ""
        if len(ovnt) == 2:
            ov_node_subtype = ovnt[1]
        if ov_node_type == NODE_ARRAY and ov_node_subtype == "":
            ERROR_1_ARRAY_NO_TYPE_DEFINED.append(f"{rownum}-{ov_name}")
        primary_key = True if not pd.isna(row[COLUMN_PRIMARY_KEY]) and (row[COLUMN_PRIMARY_KEY].strip()).lower() == "true" else False
        source_name = row[COLUMN_SOURCE_NAME].strip() if not pd.isna(row[COLUMN_SOURCE_NAME]) else row[COLUMN_SOURCE_NAME]
        source_type = row[COLUMN_SOURCE_TYPE].strip() if not pd.isna(row[COLUMN_SOURCE_TYPE]) else row[COLUMN_SOURCE_TYPE]
        source_attribute = row[COLUMN_SOURCE_ATTRIBUTE].strip() if not pd.isna(row[COLUMN_SOURCE_ATTRIBUTE]) else row[COLUMN_SOURCE_ATTRIBUTE]
        uniqueItems = True if not pd.isna(row[COLUMN_UNIQUEITEMS]) and row[COLUMN_UNIQUEITEMS].strip().lower()  == "true" else False
        print(f"{rownum}, {ov_name}")
        # no parent_tag --> row is for basic type or first entry for array of object or object
        if pd.isna(parent_tag) :
            if ov_name not in json_data:
                json_data[ov_name] = {
                    NODE_TITLE: ov_name,
                    NODE_DESCRIPTION: description,
                    NODE_TYPE: ov_node_type
                }
                if ov_node_type == NODE_OBJECT:
                    json_data[ov_name][NODE_PROPERTIES] = {}
                if ov_node_type == NODE_ARRAY:
                    json_data[ov_name][NODE_UNIQUEITEMS] = uniqueItems
                    json_data[ov_name][NODE_ITEMS] = {
                        NODE_TITLE : ov_name,
                        NODE_DESCRIPTION: description,
                        NODE_TYPE: ov_node_subtype
                    }
                    if not pd.isna(ov_node_subtype) and ov_node_subtype not in BASIC_TYPES:
                        json_data[ov_name][NODE_ITEMS][NODE_PROPERTIES]= {}
                    else:
                        # add x-collibra node for array of basic type. Only if the source is defined.
                        if not pd.isna(source_name):
                            source = {
                                'sourceName': source_name,
                                'sourceType': source_type,
                                'sourceAttribute': source_attribute
                            }
                            if NODE_X_COLLIBRA not in json_data[ov_name]:
                                json_data[ov_name][NODE_X_COLLIBRA] = {
                                    'primaryKey': primary_key,
                                    'sources': []
                                }
                            if source not in json_data[ov_name][NODE_X_COLLIBRA][NODE_SOURCES]:
                                json_data[ov_name][NODE_X_COLLIBRA][NODE_SOURCES].append(source)
                        else:
                            print(f"Error")
                            WARN_1_ARRAY_NO_SOURCE_DEFINED.append(f"{ov_name}")

                elif ov_node_type in BASIC_TYPES:
                    # add x-collibra only if source is present in csv row
                    if not pd.isna(source_name):
                        source = {
                            'sourceName': source_name,
                            'sourceType': source_type,
                            'sourceAttribute': source_attribute
                        }
                        if NODE_X_COLLIBRA not in json_data[ov_name]:
                            json_data[ov_name][NODE_X_COLLIBRA] = {
                                'primaryKey': primary_key,
                                'sources': []
                             }
                        if source not in json_data[ov_name][NODE_X_COLLIBRA][NODE_SOURCES]:
                            json_data[ov_name][NODE_X_COLLIBRA][NODE_SOURCES].append(source)
            else:
                #print(f" {ov_name} is in {json_data}")
                if ov_node_type in BASIC_TYPES:
                    if not pd.isna(source_name):
                        source = {
                            'sourceName': source_name,
                            'sourceType': source_type,
                            'sourceAttribute': source_attribute
                        }
                    if NODE_X_COLLIBRA not in json_data[ov_name]:
                        json_data[ov_name][NODE_X_COLLIBRA] = {
                            'primaryKey': primary_key,
                            'sources': []
                         }
                    if source not in json_data[ov_name][NODE_X_COLLIBRA][NODE_SOURCES]:
                        json_data[ov_name][NODE_X_COLLIBRA][NODE_SOURCES].append(source)

        else:
            #parent_tag = int(parent_tag)
            parent_tag_str = str(parent_tag)
            if parent_tag_str in json_data:
                parent_node = json_data[parent_tag_str]
                parent_properties = parent_node[NODE_ITEMS][NODE_PROPERTIES] if parent_node[NODE_TYPE] == NODE_ARRAY else parent_node[NODE_PROPERTIES]
                parent_properties[ov_name] = {
                    NODE_TITLE: ov_name,
                    NODE_DESCRIPTION: description,
                    NODE_TYPE: ov_type
                }
                # add x-collibra tag only if source attribute is present in csv
                if not (pd.isna(source_name) or pd.isna(source_type) or pd.isna(source_attribute)):
                    parent_properties[ov_name][NODE_X_COLLIBRA] = {
                        NODE_PRIMARY_KEY: primary_key,
                        NODE_SOURCES: [{
                            NODE_SOURCE_NAME: source_name,
                            NODE_SOURCE_TYPE: source_type,
                            NODE_SOURCE_ATTRIBUTE: source_attribute
                        }]
                    }

    # Create a backup of the existing JSON file
    backup_file_path = add_timestamp_suffix(json_file_path)
    try:
        os.rename(json_file_path, backup_file_path)
        print(f"JSON file renamed from '{json_file_path}' to '{backup_file_path}' successfully.")
    except Exception as e:
        print(f"An error occurred while renaming the CSV file: {e}")

    # Write the JSON data to a file
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=2)

    print(f"CSV to JSON conversion completed. {csv_file_path} --> {json_file_path}")


def add_timestamp_suffix(file_path):
    timestamp = datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
    file_name, extension = file_path.rsplit('.', 1)
    backup_file_path = f"{file_name}_{timestamp}.{extension}"
    return backup_file_path

# Example usage
csv_file_path = 'c2j.csv'
json_file_path = 'c2j.json'
csv_to_json(csv_file_path, json_file_path)
print(f"<{csv_file_path}, {json_file_path}>")
#print(f"ERROR_1_BASIC_ARRAY_NO_SOURCE -> {WARN_1_ARRAY_NO_SOURCE_DEFINED}")
print(f"{ERRORS_WARNINGS}")