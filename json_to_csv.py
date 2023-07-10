import pandas as pd
import json


def json_to_csv(json_file_path, csv_file_path):
    # Read the JSON file
    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

    # Convert JSON to DataFrame
    rows = []
    for ov_name, data in json_data.items():
        row = [ov_name, data['description'], data['type'], '', '', '', '']
        if 'x-collibra' in data:
            row[3] = str(data['x-collibra']['primaryKey'])
            for source in data['x-collibra']['sources']:
                row[4] = source['sourceName']
                row[5] = source['sourceType']
                row[6] = source['sourceAttribute']
                rows.append(row.copy())
        else:
            rows.append(row)
            if 'properties' in data:
                for prop_name, prop_data in data['properties'].items():
                    prop_row = [f"{ov_name}.{prop_name}", prop_data['description'], prop_data['type'], '', '', '', '']
                    if 'x-collibra' in prop_data:
                        prop_row[3] = str(prop_data['x-collibra']['primaryKey'])
                        for source in prop_data['x-collibra']['sources']:
                            prop_row[4] = source['sourceName']
                            prop_row[5] = source['sourceType']
                            prop_row[6] = source['sourceAttribute']
                            rows.append(prop_row.copy())
                    else:
                        rows.append(prop_row)

    # Create DataFrame
    df = pd.DataFrame(rows, columns=['ovName', 'description', 'type', 'primaryKey', 'sourceName', 'sourceType',
                                     'sourceAttribute'])

    # Write DataFrame to CSV file
    df.to_csv(csv_file_path, index=False)
    print("CSV file created successfully.")


# Example usage
json_file_path = 'input.json'
csv_file_path = 'output.csv'
json_to_csv(json_file_path, csv_file_path)
