import pandas as pd
import json

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

def csv_to_json(csv_file_path, json_file_path):
    # Read the CSV file using pandas and specify column types as string
    df = pd.read_csv(csv_file_path, dtype=str)

    # Identify duplicate rows based on selected columns
    duplicates = df[df.duplicated(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], keep=False)]

    # Drop duplicate rows based on selected columns
    df.drop_duplicates(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], inplace=True)

    # Create a dictionary to store the JSON data
    json_data = {}

    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():
        #print(f"{_}")
        parent_tag = row[COLUMN_PARENT_TAG]
        ov_name = row[COLUMN_OV_NAME]
        description = row[COLUMN_DESCRIPTION]
        ov_type = row[COLUMN_TYPE]
        # actual type of the ov node
        ov_node_type = ov_type.split('.')[0]
        primary_key = True if not pd.isna(row[COLUMN_PRIMARY_KEY]) and (row[COLUMN_PRIMARY_KEY].strip()).lower() == "true" else False
        source_name = row[COLUMN_SOURCE_NAME]
        source_type = row[COLUMN_SOURCE_TYPE]
        source_attribute = row[COLUMN_SOURCE_ATTRIBUTE]
        uniqueItems = True if not pd.isna(row[COLUMN_UNIQUEITEMS]) and row[COLUMN_UNIQUEITEMS].strip().lower()  == "true" else False
        #pt = pd.isna(parent_tag)
        #print(f"{parent_tag} pt is na ? {pd.isna(parent_tag)} for {ov_name}, and type: {ov_type}")

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
                        NODE_DESCRIPTION: description,
                        NODE_TYPE: ov_type.split('.')[1],
                        NODE_PROPERTIES: {}
                    }
                elif ov_node_type in BASIC_TYPES:
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
                    NODE_TYPE: ov_type,
                    NODE_X_COLLIBRA: {
                        NODE_PRIMARY_KEY: primary_key,
                        NODE_SOURCES: [{
                            NODE_SOURCE_NAME: source_name,
                            NODE_SOURCE_TYPE: source_type,
                            NODE_SOURCE_ATTRIBUTE: source_attribute
                        }]
                    }
                }

    # Write the JSON data to a file
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

    print(f"CSV to JSON conversion completed. Output file: {json_file_path}")

# Example usage
csv_file_path = 'input.csv'
json_file_path = 'output.json'
csv_to_json(csv_file_path, json_file_path)
print(f"<{csv_file_path}, {json_file_path}>")
