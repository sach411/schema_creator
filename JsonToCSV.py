# wrong file, not functioning.

#DONT USE

import pandas as pd
import json
import os

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

def env_setup(json_data:dict) -> dict:
    process_nodes = os.getenv('process_nodes', 'ALL')
    process_nodes = process_nodes.split(',') if process_nodes != 'ALL' else []
    if len(process_nodes) > 0:
        json_data = {key:value for key, value in json_data.items() if key in process_nodes}
    return json_data

# Function to flatten the nested JSON and extract the required data
def flatten_json(json_obj, parent_tag='', result=[]):
    #result = []
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            new_tag = f"{parent_tag},{key}" if parent_tag else key
            #print(f"newtag:{new_tag}, value:{value}  ")
            if isinstance(value, dict):
                result.extend(flatten_json(value, new_tag, result=result))
            elif isinstance(value, list):
                result.append((new_tag, value))
            else:
                result.append((new_tag, value))
    elif isinstance(json_obj, list):
        for item in json_obj:
            result.extend(flatten_json(item, parent_tag, result=result))

    #print(f"{result}")
    return result

# Read the input JSON
with open(input_file, 'r') as file:
    input_data = file.read()

# Load the input JSON
input_json = json.loads(input_data)

input_json = env_setup(input_json)

# Flatten the JSON and convert to DataFrame
flatten_data = flatten_json(input_json)

flatten_data = [('distributionPolicy,type', 'array'),
                ('distributionPolicy,uniqueItems', True)]
print(f"flatten_data = {flatten_data}")

"""
flatten_data = [('distributionPolicy,title', 'distributionPolicy'), ('distributionPolicy,description', 'Distribution Policy'),
                ('distributionPolicy,type', 'array'),
                ('distributionPolicy,uniqueItems', True),
                ('distributionPolicy,items,title', 'distributionPolicy'),
                ('distributionPolicy,items,description', 'Distribution Policy'),
                ('distributionPolicy,items,type', 'string'),
                ('distributionPolicy,title', 'distributionPolicy'),
                ('distributionPolicy,description', 'Distribution Policy'),
                ('distributionPolicy,type', 'array'),
                ('distributionPolicy,uniqueItems', True),
                ('distributionPolicy,items,title', 'distributionPolicy'),
                ('distributionPolicy,items,description', 'Distribution Policy'),
                ('distributionPolicy,items,type', 'string'),
                ('distributionPolicy,title', 'distributionPolicy'),
                ('distributionPolicy,description', 'Distribution Policy'),
                ('distributionPolicy,type', 'array'),
                ('distributionPolicy,uniqueItems', True),
                ('distributionPolicy,items,title', 'distributionPolicy'),
                ('distributionPolicy,items,description', 'Distribution Policy'),
                ('distributionPolicy,items,type', 'string'),
                ('distributionPolicy,title', 'distributionPolicy'),
                ('distributionPolicy,description', 'Distribution Policy'),
                ('distributionPolicy,type', 'array'),
                ('distributionPolicy,uniqueItems', True),
                ('distributionPolicy,items,title', 'distributionPolicy'),
                ('distributionPolicy,items,description', 'Distribution Policy'),
                ('distributionPolicy,items,type', 'string')]
"""

df = pd.DataFrame(flatten_data, columns=[COLUMN_PARENT_TAG,COLUMN_OV_NAME])
"""df = pd.DataFrame(flatten_data, columns=[COLUMN_PARENT_TAG,COLUMN_OV_NAME, COLUMN_DESCRIPTION, COLUMN_TYPE, COLUMN_PRIMARY_KEY,
                                          COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE,
                                          COLUMN_UNIQUE_ITEMS])"""

# Convert DataFrame to CSV
df.to_csv(output_file, index=False)
print(f"<{input_file}  -- {output_file}>")
