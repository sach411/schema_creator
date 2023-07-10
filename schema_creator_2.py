import pandas as pd
import json


def csv_to_json(csv_file_path, json_file_path):
    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Identify duplicate rows based on selected columns
    duplicates = df[df.duplicated(subset=['ovName', 'sourceName', 'sourceType', 'sourceAttribute'], keep=False)]

    # Drop duplicate rows based on selected columns
    df.drop_duplicates(subset=['ovName', 'sourceName', 'sourceType', 'sourceAttribute'], inplace=True)

    # Convert DataFrame to JSON structure
    json_data = {}
    for _, row in df.iterrows():
        ov_name = row['ovName']
        if ov_name not in json_data:
            json_data[ov_name] = {
                'description': row['description'],
                'type': row['type'],
                'x-collibra': {
                    'primaryKey': row['primaryKey'],
                    'sources': []
                }
            }
        source = {
            'sourceName': row['sourceName'],
            'sourceType': row['sourceType'],
            'sourceAttribute': row['sourceAttribute']
        }
        json_data[ov_name]['x-collibra']['sources'].append(source)

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
