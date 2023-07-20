import pandas as pd
import json

# Read the CSV file into a pandas dataframe
df = pd.read_csv('complex_data.csv')

# Define a function to convert each row of the dataframe to a JSON object
def row_to_json(row):
    # Convert the row to a dictionary
    row_dict = row.to_dict()
    # Convert any nested fields to a JSON string
    for key, value in row_dict.items():
        if isinstance(value, dict):
            row_dict[key] = json.dumps(value)
    # Convert the dictionary to a JSON object
    json_obj = json.loads(json.dumps(row_dict))
    return json_obj

# Convert each row of the dataframe to a JSON object
json_list = [row_to_json(row) for index, row in df.iterrows()]

# Convert the list of JSON objects to a JSON string
json_str = json.dumps(json_list)

# Print the JSON string
print(json_str)
