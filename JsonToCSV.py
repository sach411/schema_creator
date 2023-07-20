import pandas as pd
import json

# Read the JSON data from the file
with open('output.json', 'r') as json_file:
    json_data = json.load(json_file)

# Initialize an empty list to store the rows for the CSV
csv_rows = []

def process_json_node(node, parent_tag=""):
    title = node.get('title', '')
    description = node.get('description', '')
    node_type = node.get('type', '')

    if parent_tag == "":
        parent_tag_str = ""
    else:
        parent_tag_str = str(parent_tag)

    if 'properties' in node:
        # If it's an object node, process its properties recursively
        for prop_name, prop_node in node['properties'].items():
            process_json_node(prop_node, parent_tag_str + "." + title)

    elif 'items' in node:
        # If it's an array node, process its items recursively
        process_json_node(node['items'], parent_tag_str + "." + title)

    else:
        # If it's a leaf node, add it to the CSV rows
        row = {
            'parentTag': parent_tag_str,
            'ovName': title,
            'description': description,
            'type': node_type
        }
        csv_rows.append(row)

# Process the JSON data recursively
for node_key in json_data.keys():
    process_json_node(json_data[node_key])

# Convert the list of CSV rows to a DataFrame
csv_df = pd.DataFrame(csv_rows)

# Write the DataFrame to a CSV file
csv_df.to_csv('input.csv', index=False)
print("JSON to CSV conversion completed.")
