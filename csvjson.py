import pandas as pd
import json

def csv_json():
    import csv

    with open('input.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    with open('output.json', 'w') as jsonfile:
        json.dump(rows, jsonfile, indent=1)

def csv_json_using_pandas():
    print(f"Invoking {csv_json_using_pandas.__name__}")
    df = pd.read_csv("input.csv")

    data_dict = df.to_dict(orient='records')

    nested_data = []

    for d in data_dict:
        nested_dict = {}
        nested_dict['description'] = d['description']
        nested_dict['type'] = d['type']
        nested_dict['sources'] = {"sourceName":d['sourceName'],
                                  "sourceType":d['sourceType'],
                                  "sourceAttribute":d['sourceAttribute']}
        nested_data.append(nested_dict)

    json_data = json.dumps(nested_data, indent=1)
    #json_data = df.to_json(orient='records', indent=1)

    with open('output.json', 'w') as json_file:
        json_file.write((json_data))


if __name__=="__main__":
    print(f"Invoking..")
    csv_json_using_pandas()
    print(f"done..")