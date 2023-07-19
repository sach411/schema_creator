import pandas as pd
import json

# Define constants for column names
COLUMN_PARENT_TAG = 'parentTag'
COLUMN_OV_NAME = 'ovName'
COLUMN_DESCRIPTION = 'description'
COLUMN_TYPE = 'type'
COLUMN_PRIMARY_KEY = 'primaryKey'
COLUMN_SOURCE_NAME = 'sourceName'
COLUMN_SOURCE_TYPE = 'sourceType'
COLUMN_SOURCE_ATTRIBUTE = 'sourceAttribute'
COLUMN_UNIQUEITEMS = "uniqueItems"

# Define constants for JSON elements
NODE_TITLE = 'title'
NODE_DESCRIPTION = 'description'
NODE_TYPE = 'type'
NODE_ARRAY = 'array'
NODE_ITEMS = 'items'
NODE_PROPERTIES = 'properties'
NODE_X_COLLIBRA = 'x-collibra'
NODE_PRIMARY_KEY = 'primaryKey'
NODE_SOURCES = 'sources'
NODE_SOURCE_NAME = 'sourceName'
NODE_SOURCE_TYPE = 'sourceType'
NODE_SOURCE_ATTRIBUTE = 'sourceAttribute'
NODE_OBJECT = "object"
NODE_UNIQUEITEMS = "uniqueItems"
BASIC_TYPES = ["string", "boolean", "number"]  # other than "array", "object"
COMPLEX_TYPES = ["array", "object"]

class JsonToCSVConverter:
    def __init__(self, csv_file_path, json_file_path):
        self.csv_file_path = csv_file_path
        self.json_file_path = json_file_path
        self.df = None

    def json_to_csv(self):
        # Read the JSON data from the file
        with open(self.json_file_path, 'r') as json_file:
            json_data = json.load(json_file)

        # Create a list to store CSV rows
        csv_rows = []

        def process_json_node(node, parent_tag=""):
            title = node[NODE_TITLE]
            description = node[NODE_DESCRIPTION]
            node_type = node[NODE_TYPE]

            if parent_tag == "":
                parent_tag_str = ""
            else:
                parent_tag_str = str(parent_tag)

            if NODE_PROPERTIES in node:
                # If it's an object node, process its properties recursively
                row = {
                    COLUMN_PARENT_TAG: parent_tag_str,
                    COLUMN_OV_NAME: title,
                    COLUMN_DESCRIPTION: description,
                    COLUMN_TYPE: node_type
                }
                csv_rows.append(row.copy())
                for prop_name, prop_node in node[NODE_PROPERTIES].items():
                    process_json_node(prop_node,  title)

            elif NODE_ITEMS in node:
                # If it's an array node, process its items recursively
                row = {
                    COLUMN_PARENT_TAG: parent_tag_str,
                    COLUMN_OV_NAME: title,
                    COLUMN_DESCRIPTION: description,
                    COLUMN_TYPE: node_type+"."+node[NODE_ITEMS][NODE_TYPE],
                    COLUMN_PRIMARY_KEY:"",
                    COLUMN_SOURCE_NAME:"",
                    COLUMN_SOURCE_TYPE:"",
                    COLUMN_SOURCE_ATTRIBUTE:"",
                    COLUMN_UNIQUEITEMS:"true" if node[NODE_UNIQUEITEMS] else "false"
                }
                csv_rows.append(row.copy())
                process_json_node(node[NODE_ITEMS], title)

            else:
                # If it's a leaf node, add it to the CSV rows
                row = {
                    COLUMN_PARENT_TAG: parent_tag_str,
                    COLUMN_OV_NAME: title,
                    COLUMN_DESCRIPTION: description,
                    COLUMN_TYPE: node_type
                }

                if NODE_X_COLLIBRA in node:
                    x_collibra = node[NODE_X_COLLIBRA]
                    if NODE_PRIMARY_KEY in x_collibra:
                        row[COLUMN_PRIMARY_KEY] = x_collibra[NODE_PRIMARY_KEY]

                    if NODE_SOURCES in x_collibra:
                        sources = x_collibra[NODE_SOURCES]
                        for source in sources:
                            row[COLUMN_SOURCE_NAME] = source[NODE_SOURCE_NAME]
                            row[COLUMN_SOURCE_TYPE] = source[NODE_SOURCE_TYPE]
                            row[COLUMN_SOURCE_ATTRIBUTE] = source[NODE_SOURCE_ATTRIBUTE]

                            # Add the row to the list of CSV rows
                            csv_rows.append(row.copy())

        # Process the JSON data recursively
        for node_key in json_data.keys():
            process_json_node(json_data[node_key])

        # Convert the list of CSV rows to a DataFrame
        csv_df = pd.DataFrame(csv_rows)

        # Write the DataFrame to a CSV file
        csv_df.to_csv(self.csv_file_path, index=False)
        print(f"JSON to CSV conversion completed. Output file: {self.csv_file_path}")

# Example usage
json_file_path = 'output.json'
csv_file_path = 'output_.csv'
converter = JsonToCSVConverter( csv_file_path, json_file_path)
converter.json_to_csv()
print(f"<{json_file_path}, {csv_file_path}>")
