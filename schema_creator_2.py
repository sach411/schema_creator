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
    duplicates = df[
        df.duplicated(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE],
                      keep=False)]

    # Drop duplicate rows based on selected columns
    df.drop_duplicates(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE],
                       inplace=True)

    # Convert DataFrame to JSON structure
    json_data = {}
    for _, row in df.iterrows():
        ov_name = row[COLUMN_OV_NAME]
        if ov_name not in json_data:
            json_data[ov_name] = {
                COLUMN_DESCRIPTION: row[COLUMN_DESCRIPTION],
                COLUMN_TYPE: row[COLUMN_TYPE],
                'x-collibra': {
                    COLUMN_PRIMARY_KEY: row[COLUMN_PRIMARY_KEY],
                    'sources': []
                }
            }
            if row[COLUMN_TYPE] == 'object':
                json_data[ov_name]['properties'] = {}
        if row[COLUMN_TYPE] != 'object':
            source = {
                COLUMN_SOURCE_NAME: row[COLUMN_SOURCE_NAME],
                COLUMN_SOURCE_TYPE: row[COLUMN_SOURCE_TYPE],
                COLUMN_SOURCE_ATTRIBUTE: row[COLUMN_SOURCE_ATTRIBUTE]
            }
            json_data[ov_name]['x-collibra']['sources'].append(source)
        else:
            property_name = row[COLUMN_SOURCE_NAME]
            property_type = row[COLUMN_TYPE].split('.')[1]
            property_source = {
                COLUMN_SOURCE_NAME: row[COLUMN_SOURCE_NAME],
                COLUMN_SOURCE_TYPE: row[COLUMN_SOURCE_TYPE],
                COLUMN_SOURCE_ATTRIBUTE: row[COLUMN_SOURCE_ATTRIBUTE]
            }
            property_data = {
                COLUMN_DESCRIPTION: row[COLUMN_DESCRIPTION],
                COLUMN_TYPE: property_type,
                'x-collibra': {
                    COLUMN_PRIMARY_KEY: row[COLUMN_PRIMARY_KEY],
                    'sources': [property_source]
                }
            }
            json_data[ov_name]['properties'][property_name] = property_data

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
