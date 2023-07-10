import pandas as pd
import json

# Define column names as constants
COLUMN_OV_NAME = 'ovName'
COLUMN_DESCRIPTION = 'description'
COLUMN_TYPE = 'type'
COLUMN_PRIMARY_KEY = 'primaryKey'
COLUMN_SOURCE_NAME = 'sourceName'
COLUMN_SOURCE_TYPE = 'sourceType'
COLUMN_SOURCE_ATTRIBUTE = 'sourceAttribute'

def csv_to_json(csv_file_path, json_file_path):
    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Identify duplicate rows based on selected columns
    duplicates = df[df.duplicated(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], keep=False)]

    # Drop duplicate rows based on selected columns
    df.drop_duplicates(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], inplace=True)

    # Convert DataFrame to JSON structure
    json_data = {}
    for _, row in df.iterrows():
        ov_name = row[COLUMN_OV_NAME]
        description = row[COLUMN_DESCRIPTION]
        ov_type = row[COLUMN_TYPE]
        source_name = row[COLUMN_SOURCE_NAME]
        source_type = row[COLUMN_SOURCE_TYPE]
        source_attribute = row[COLUMN_SOURCE_ATTRIBUTE]

        if '.' not in ov_name:
            if ov_name not in json_data:
                json_data[ov_name] = {
                    COLUMN_DESCRIPTION: description,
                    COLUMN_TYPE: ov_type,
                    'x-collibra': {
                        COLUMN_PRIMARY_KEY: row[COLUMN_PRIMARY_KEY],
                        'sources': []
                    }
                }
            json_data[ov_name]['x-collibra']['sources'].append({
                COLUMN_SOURCE_NAME: source_name,
                COLUMN_SOURCE_TYPE: source_type,
                COLUMN_SOURCE_ATTRIBUTE: source_attribute
            })
        else:
            parent_name, property_name = ov_name.split('.', 1)
            if parent_name not in json_data:
                json_data[parent_name] = {
                    COLUMN_DESCRIPTION: '',
                    COLUMN_TYPE: 'object',
                    'properties': {}
                }
            if 'properties' not in json_data[parent_name]:
                json_data[parent_name]['properties'] = {}
            json_data[parent_name]['properties'][property_name] = {
                COLUMN_DESCRIPTION: description,
                COLUMN_TYPE: ov_type,
                'x-collibra': {
                    COLUMN_PRIMARY_KEY: row[COLUMN_PRIMARY_KEY],
                    'sources': [{
                        COLUMN_SOURCE_NAME: source_name,
                        COLUMN_SOURCE_TYPE: source_type,
                        COLUMN_SOURCE_ATTRIBUTE: source_attribute
                    }]
                }
            }

    # Write the data as JSON to a file
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

    # Print duplicate rows
    if not duplicates.empty:
        print("Duplicate Rows:")
        print(duplicates)

# Example usage
csv_file_path = 'data.csv'
json_file_path = 'data.json'
csv_to_json(csv_file_path, json_file_path)
