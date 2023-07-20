import pandas as pd
import json

# Constants for column headers in the CSV file
COL_PARENT_TAG = 'parentTag'
COL_OV_NAME = 'ovName'
COL_DESCRIPTION = 'description'
COL_TYPE = 'type'
COL_PRIMARY_KEY = 'primaryKey'
COL_SOURCE_NAME = 'sourceName'
COL_SOURCE_TYPE = 'sourceType'
COL_SOURCE_ATTRIBUTE = 'sourceAttribute'
COL_UNIQUE_ITEMS = 'uniqueItems'

# Constants for JSON nodes
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
NODE_OBJECT = 'object'
NODE_STRING = 'string'
NODE_NUMBER = 'number'

def process_json_node(node, parent_tag=""):
    title = node.get(NODE_TITLE, '')
    description = node.get(NODE_DESCRIPTION, '')
    node_type = node.get(NODE_TYPE, '')
    unique_items = node.get(COL_UNIQUE_ITEMS, '')
    primary_key = node.get(NODE_X_COLLIBRA, {}).get(NODE_PRIMARY_KEY, '')
    sources = node.get(NODE_X_COLLIBRA, {}).get(NODE_SOURCES, [])

    # If it's an object node, add it as a row in the CSV
    row = {
        COL_PARENT_TAG: parent_tag,
        COL_OV_NAME: title,
        COL_DESCRIPTION: description,
        COL_TYPE: node_type,
        COL_PRIMARY_KEY: primary_key,
        COL_UNIQUE_ITEMS: unique_items,
        COL_SOURCE_NAME: sources[0][NODE_SOURCE_NAME] if sources else '',
        COL_SOURCE_TYPE: sources[0][NODE_SOURCE_TYPE] if sources else '',
        COL_SOURCE_ATTRIBUTE: sources[0][NODE_SOURCE_ATTRIBUTE] if sources else ''
    }
    csv_rows.append(row)

    # If the node has properties, process them recursively
    if NODE_PROPERTIES in node:
        for prop_name, prop_node in node[NODE_PROPERTIES].items():
            process_json_node(prop_node, title)

    # If it's an array node, process its items recursively
    if NODE_ITEMS in node:
        if node_type == NODE_ARRAY:
            if NODE_ITEMS in node[NODE_ITEMS]:  # To handle "distributionPolicy" case
                process_json_node(node[NODE_ITEMS], title)
            else:
                process_json_node(node[NODE_ITEMS][NODE_PROPERTIES], title)
        else:
            process_json_node(node[NODE_ITEMS], title)

# Read the JSON data from the file
with open('input.json', 'r') as json_file:
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
    COL_PARENT_TAG, COL_OV_NAME, COL_DESCRIPTION, COL_TYPE, COL_PRIMARY_KEY, COL_SOURCE_NAME,
    COL_SOURCE_TYPE, COL_SOURCE_ATTRIBUTE, COL_UNIQUE_ITEMS
]]

# Write the DataFrame to a CSV file
csv_df.to_csv('output.csv', index=False)
print("JSON to CSV conversion completed.")
