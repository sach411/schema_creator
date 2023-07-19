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

class CSVToJsonConverter:
    def __init__(self, csv_file_path, json_file_path):
        self.csv_file_path = csv_file_path
        self.json_file_path = json_file_path
        self.df = None

    def read_csv(self):
        # Read the CSV file using pandas and specify column types as string
        self.df = pd.read_csv(self.csv_file_path, dtype=str)

    def process_csv(self):
        # Your existing code for processing the CSV and creating the JSON
        pass

    def write_json(self):
        # Write the JSON data to a file
        with open(self.json_file_path, 'w') as json_file:
            json.dump(self.json_data, json_file, indent=4)

    def convert(self):
        self.read_csv()
        self.process_csv()
        self.write_json()

# Example usage
csv_file_path = 'input.csv'
json_file_path = 'output.json'
converter = CSVToJsonConverter(csv_file_path, json_file_path)
converter.convert()
print(f"<{csv_file_path}, {json_file_path}>")
