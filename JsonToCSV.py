import pandas as pd
import json

# Constants for column names
COLUMN_PARENT_TAG = 'parentTag'
COLUMN_OV_NAME = 'ovName'
COLUMN_DESCRIPTION = 'description'
COLUMN_TYPE = 'type'
COLUMN_PRIMARY_KEY = 'primaryKey'
COLUMN_SOURCE_NAME = 'sourceName'
COLUMN_SOURCE_TYPE = 'sourceType'
COLUMN_SOURCE_ATTRIBUTE = 'sourceAttribute'
COLUMN_UNIQUE_ITEMS = 'uniqueItems'

# File names
input_file = 'j2c.json'
output_file = 'j2c_temp.csv'

# Function to flatten the nested JSON and extract the required data
def flatten_json(json_obj, parent_tag=''):
    result = []
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_tag = f"{parent_tag},{key}" if parent_tag else key
            if isinstance(value, dict):
                result.extend(flatten_json(value, new_tag))
            elif isinstance(value, list):
                result.append((new_tag, value))
            else:
                result.append((new_tag, value))
    elif isinstance(json_obj, list):
        for item in json_obj:
            result.extend(flatten_json(item, parent_tag))
    return result

# Read the input JSON
with open(input_file, 'r') as file:
    input_data = file.read()

# Load the input JSON
input_json = json.loads(input_data)

# Flatten the JSON and convert to DataFrame
flatten_data = flatten_json(input_json)
df = pd.DataFrame(flatten_data, columns=[COLUMN_OV_NAME, COLUMN_DESCRIPTION, COLUMN_TYPE, COLUMN_PRIMARY_KEY,
                                          COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE,
                                          COLUMN_UNIQUE_ITEMS])

# Convert DataFrame to CSV
df.to_csv(output_file, index=False)
