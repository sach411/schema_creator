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

def process_json_node(node, parent_tag="", is_array=False):
    title = node.get(NODE_TITLE, '')
    description = node.get(NODE_DESCRIPTION, '')
    node_type = node.get(NODE_TYPE, '')
    unique_items = node.get(COLUMN_UNIQUEITEMS, '')
    primary_key = node.get(NODE_X_COLLIBRA, {}).get(NODE_PRIMARY_KEY, '')
    sources = node.get(NODE_X_COLLIBRA, {}).get(NODE_SOURCES, [])

    # If it's an object or array node, add it as a row in the CSV
    if not is_array:
        row = {
            COLUMN_PARENT_TAG: parent_tag,
            COLUMN_OV_NAME: title,
            COLUMN_DESCRIPTION: description,
            COLUMN_TYPE: node_type,
            COLUMN_PRIMARY_KEY: primary_key,
            COLUMN_UNIQUEITEMS: unique_items,
            COLUMN_SOURCE_NAME: sources[0][NODE_SOURCE_NAME] if sources else '',
            COLUMN_SOURCE_TYPE: sources[0][NODE_SOURCE_TYPE] if sources else '',
            COLUMN_SOURCE_ATTRIBUTE: sources[0][NODE_SOURCE_ATTRIBUTE] if sources else ''
        }
        csv_rows.append(row)

    # If the node has properties, process them recursively
    if NODE_PROPERTIES in node:
        for prop_name, prop_node in node[NODE_PROPERTIES].items():
            process_json_node(prop_node, title)

    # If it's an array node, process its items recursively
    if NODE_ITEMS in node:
        if node_type == NODE_ARRAY:
            if NODE_PROPERTIES in node[NODE_ITEMS]:  # To handle "distributionPolicy" case
                process_json_node(node[NODE_ITEMS], parent_tag, is_array=True)
            else:
                process_json_node(node[NODE_ITEMS][NODE_PROPERTIES], parent_tag, is_array=True)
        else:
            process_json_node(node[NODE_ITEMS], parent_tag)

# Read the JSON data from the file
with open('output.json', 'r') as json_file:
    json_data = json.load(json_file)

# Initialize an empty list to store the rows for the CSV
csv_rows = []

# Process the JSON data recursively
for node_key in json_data.keys():
    process_json_node(json_data[node_key])

# Convert the list of CSV rows to a DataFrame
csv_df = pd.DataFrame(csv_rows)

# Reorder columns to match the output.csv format
csv_df = csv_df[[
    COLUMN_PARENT_TAG, COLUMN_OV_NAME, COLUMN_DESCRIPTION, COLUMN_TYPE, COLUMN_PRIMARY_KEY, COLUMN_SOURCE_NAME,
    COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE, COLUMN_UNIQUEITEMS
]]

# Write the DataFrame to a CSV file
csv_df.to_csv('output.csv', index=False)
print("JSON to CSV conversion completed.")
