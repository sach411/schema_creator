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


def json_to_csv(json_file_path, csv_file_path):
    # Read the JSON file
    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

    # Convert JSON to DataFrame
    rows = []
    for ov_name, data in json_data.items():
        row = [ov_name, data[COLUMN_DESCRIPTION], data[COLUMN_TYPE], '', '', '', '']
        if 'x-collibra' in data:
            row[3] = str(data['x-collibra'][COLUMN_PRIMARY_KEY])
            for source in data['x-collibra']['sources']:
                row[4] = source[COLUMN_SOURCE_NAME]
                row[5] = source[COLUMN_SOURCE_TYPE]
                row[6] = source[COLUMN_SOURCE_ATTRIBUTE]
                rows.append(row.copy())
        else:
            rows.append(row)
            if 'properties' in data:
                for prop_name, prop_data in data['properties'].items():
                    prop_row = [f"{ov_name}.{prop_name}", prop_data[COLUMN_DESCRIPTION], prop_data[COLUMN_TYPE], '', '',
                                '', '']
                    if 'x-collibra' in prop_data:
                        prop_row[3] = str(prop_data['x-collibra'][COLUMN_PRIMARY_KEY])
                        for source in prop_data['x-collibra']['sources']:
                            prop_row[4] = source[COLUMN_SOURCE_NAME]
                            prop_row[5] = source[COLUMN_SOURCE_TYPE]
                            prop_row[6] = source[COLUMN_SOURCE_ATTRIBUTE]
                            rows.append(prop_row.copy())
                    else:
                        rows.append(prop_row)

    # Create DataFrame
    df = pd.DataFrame(rows,
                      columns=[COLUMN_OV_NAME, COLUMN_DESCRIPTION, COLUMN_TYPE, COLUMN_PRIMARY_KEY, COLUMN_SOURCE_NAME,
                               COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE])

    # Write DataFrame to CSV file
    df.to_csv(csv_file_path, index=False)
    print("CSV file created successfully.")


# Example usage
json_file_path = 'input.json'
csv_file_path = 'output.csv'
json_to_csv(json_file_path, csv_file_path)
