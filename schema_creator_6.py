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
    print(f"Input file: {csv_file_path}")
    df = pd.read_csv(csv_file_path)

    # Convert DataFrame to JSON structure
    json_data = {}
    for _, row in df.iterrows():
        ov_name = row[COLUMN_OV_NAME]
        description = row[COLUMN_DESCRIPTION]
        ov_type = row[COLUMN_TYPE]

        if '.' not in ov_name:
            if ov_name not in json_data:
                json_data[ov_name] = {
                    'title': ov_name,
                    'description': description,
                    'type': ov_type,
                    'x-collibra': {
                        'primaryKey': row[COLUMN_PRIMARY_KEY],
                        'sources': []
                    }
                }

            source_name = row[COLUMN_SOURCE_NAME]
            if source_name:
                source = {
                    'sourceName': source_name,
                    'sourceType': row[COLUMN_SOURCE_TYPE],
                    'sourceAttribute': row[COLUMN_SOURCE_ATTRIBUTE]
                }
                if source not in json_data[ov_name]['x-collibra']['sources']:
                    json_data[ov_name]['x-collibra']['sources'].append(source)
        else:
            parent_name, property_name = ov_name.split('.', 1)
            if parent_name not in json_data:
                json_data[parent_name] = {
                    'title': parent_name,
                    'description': '',
                    'type': 'object',
                    'properties': {}
                }

            if 'properties' not in json_data[parent_name]:
                json_data[parent_name]['properties'] = {}

            if property_name not in json_data[parent_name]['properties']:
                json_data[parent_name]['properties'][property_name] = {
                    'title': property_name,
                    'description': description,
                    'type': ov_type
                }

            source_name = row[COLUMN_SOURCE_NAME]
            if source_name:
                source = {
                    'sourceName': source_name,
                    'sourceType': row[COLUMN_SOURCE_TYPE],
                    'sourceAttribute': row[COLUMN_SOURCE_ATTRIBUTE]
                }
                if 'x-collibra' not in json_data[parent_name]['properties'][property_name]:
                    json_data[parent_name]['properties'][property_name]['x-collibra'] = {
                        'primaryKey': row[COLUMN_PRIMARY_KEY],
                        'sources': []
                    }

                if source not in json_data[parent_name]['properties'][property_name]['x-collibra']['sources']:
                    json_data[parent_name]['properties'][property_name]['x-collibra']['sources'].append(source)

    # Write the data as JSON to a file
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)
    print(f"JSON file created successfully. {json_file_path}")

# Example usage
csv_file_path = 'input.csv'
json_file_path = 'output.json'
csv_to_json(csv_file_path, json_file_path)
print(f"Input file: {csv_file_path}, output file: {json_file_path}")
