import pandas as pd
import json
import os
import time
from datetime import datetime

# Define column names as constants
COLUMN_PARENT_TAG = 'parentTag'
COLUMN_OV_NAME = 'ovName'
COLUMN_DESCRIPTION = 'description'
COLUMN_TYPE = 'type'
COLUMN_PRIMARY_KEY = 'primaryKey'
COLUMN_SOURCE_NAME = 'sourceName'
COLUMN_SOURCE_TYPE = 'sourceType'
COLUMN_SOURCE_ATTRIBUTE = 'sourceAttribute'
COLUMN_UNIQUEITEMS = 'uniqueItems'

# Define index for column names as constants
INDEX_PARENT_TAG = 0 #'parentTag'
INDEX_OV_NAME = 1 #'ovName'
INDEX_DESCRIPTION = 2 #'description'
INDEX_TYPE = 3 #'type'
INDEX_PRIMARY_KEY = 4 #'primaryKey'
INDEX_SOURCE_NAME = 5 #'sourceName'
INDEX_SOURCE_TYPE = 6 #'sourceType'
INDEX_SOURCE_ATTRIBUTE = 7 #'sourceAttribute'
INDEX_UNIQUEITEMS = 8 #'uniqueItems'

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

#ERROR lists

WARN_1_ARRAY_NO_SOURCE_DEFINED = []
ERROR_1_ARRAY_NO_TYPE_DEFINED = []
ERRORS_WARNINGS=[{"WARN_1_ARRAY_NO_SOURCE_DEFINED":WARN_1_ARRAY_NO_SOURCE_DEFINED},
                 {"ERROR_1_ARRAY_NO_TYPE_DEFINED": ERROR_1_ARRAY_NO_TYPE_DEFINED}]

def env_setup(json_data: dict, input_file_path=None) -> dict:
    process_nodes = os.getenv('process_nodes', 'ALL')
    process_nodes = process_nodes.split(',') if process_nodes != 'ALL' else []
    print(f"Processing following nodes: {process_nodes} from {input_file_path}")
    if len(process_nodes) > 0:
        json_data = {key:value for key, value in json_data.items() if key in process_nodes}
    return json_data

def json_to_csv(json_file_path, csv_file_path):
    # Read the JSON file
    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

    json_data = env_setup(json_data,input_file_path=json_file_path)
    # Convert JSON to DataFrame
    rows = []
    for ov_name, data in json_data.items():

        uniqueItems = data.get(COLUMN_UNIQUEITEMS, '')
        description = data.get(COLUMN_DESCRIPTION, '')
        ov_node_type = data.get(COLUMN_TYPE, '')
        x_collibra_node = data.get(NODE_X_COLLIBRA)
        row = ['',ov_name, description, ov_node_type, '', '', '', '',uniqueItems]

        # process x-collibra
        '''
        if NODE_X_COLLIBRA in data:
            row[3] = str(data[NODE_X_COLLIBRA][COLUMN_PRIMARY_KEY])
            for source in data[NODE_X_COLLIBRA]['sources']:
                row[4] = source[COLUMN_SOURCE_NAME]
                row[5] = source[COLUMN_SOURCE_TYPE]
#                row[5] = f"{data[COLUMN_TYPE]}.{source[COLUMN_SOURCE_TYPE]}" if data[COLUMN_TYPE] else source[COLUMN_SOURCE_TYPE]
                row[6] = source[COLUMN_SOURCE_ATTRIBUTE]
                rows.append(row.copy())
        else:
        '''
        #rows.append(row)
        # if node is object type
        if ov_node_type == NODE_OBJECT:
            rows.append(row)
            for prop_name, prop_data in data[NODE_PROPERTIES].items():
                prop_row = [f"{ov_name}",f"{prop_name}", prop_data[COLUMN_DESCRIPTION], prop_data[COLUMN_TYPE], '', '', '', '']
                if NODE_X_COLLIBRA in prop_data:
                    prop_row[3] = str(prop_data[NODE_X_COLLIBRA][COLUMN_PRIMARY_KEY])
                    for source in prop_data[NODE_X_COLLIBRA]['sources']:
                        prop_row[4] = source[COLUMN_SOURCE_NAME]
                        prop_row[5] = source[COLUMN_SOURCE_TYPE]
                        prop_row[6] = source[COLUMN_SOURCE_ATTRIBUTE]
                        rows.append(prop_row.copy())
                else:
                    rows.append(prop_row)
        # if node is array
        if ov_node_type == NODE_ARRAY:
            # array node must have items.
            array_items = data.get(NODE_ITEMS)
            if array_items:
                # update the row for type to include node sub type
                ov_node_sub_type = array_items.get(NODE_TYPE)
                # simple array
                if ov_node_sub_type in BASIC_TYPES:
                    # replace title, description if present
                    row[INDEX_TYPE] = f"{row[INDEX_TYPE]}.{ov_node_sub_type}"
                    # add x-collibra attributes... from x-collibra - primaryKey, sources tag if source is present
                    if NODE_X_COLLIBRA in data:
                        row[INDEX_PRIMARY_KEY] = str(data[NODE_X_COLLIBRA][COLUMN_PRIMARY_KEY])
                        for source in data[NODE_X_COLLIBRA]['sources']:
                            row[INDEX_SOURCE_NAME] = source[COLUMN_SOURCE_NAME]
                            row[INDEX_SOURCE_TYPE] = f"{data[COLUMN_TYPE]}.{source[COLUMN_SOURCE_TYPE]}" if data[COLUMN_TYPE] else \
                            source[COLUMN_SOURCE_TYPE]
                            row[INDEX_SOURCE_ATTRIBUTE] = source[COLUMN_SOURCE_ATTRIBUTE]
                elif ov_node_sub_type in COMPLEX_TYPE:
                    # TODO: array of object
                    print(f"array of object")

            else:
                # if node of `type` `array` doesn't have `type` in `items`, it's an error.
                ERROR_1_ARRAY_NO_TYPE_DEFINED.append(ov_name)

            rows.append(row)






    # Create DataFrame
    df = pd.DataFrame(rows,
                      columns=[COLUMN_PARENT_TAG,COLUMN_OV_NAME, COLUMN_DESCRIPTION, COLUMN_TYPE,
                               COLUMN_PRIMARY_KEY, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE,
                               COLUMN_SOURCE_ATTRIBUTE, COLUMN_UNIQUEITEMS])

    # Create a backup of the existing CSV file
    backup_file_path = add_timestamp_suffix(csv_file_path)
    try:
        os.rename(csv_file_path, backup_file_path)
        #print(f"CSV file renamed from '{csv_file_path}' to '{backup_file_path}' successfully.")
    except Exception as e:
        print(f"An error occurred while renaming the CSV file: {e}")

    # Write DataFrame to CSV file
    df.to_csv(csv_file_path, index=False)
    print(f"CSV file created successfully. {csv_file_path}")

    # Delete backed-up CSV files older than 5 minutes
    current_time = time.time()
    directory = os.path.dirname(os.path.abspath(csv_file_path))
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if filename.startswith('output_') and filename.endswith('.csv'):
            if (current_time - os.path.getmtime(file_path)) > 300:
                os.remove(file_path)
                print(f"Removed old backed-up CSV file: {file_path}")

def add_timestamp_suffix(file_path):
    timestamp = datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
    file_name, extension = file_path.rsplit('.', 1)
    backup_file_path = f"{file_name}_{timestamp}.{extension}"
    return backup_file_path

# Example usage
json_file_path = 'j2c.json'
csv_file_path = 'j2c_t.csv'
print(f"{__file__}")
json_to_csv(json_file_path, csv_file_path)
print(f"{ERRORS_WARNINGS}")

