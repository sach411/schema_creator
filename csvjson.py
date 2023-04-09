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
    print(f'total records: {len(data_dict)}')

    nested_data = []
    tag_set = set()

    for d in data_dict:
        print(f"{d}")
        if d['ovName'] not in tag_set:
            print(f"{d['ovName']} NOT present in tag_set : {tag_set}")
            tag_set.add(d['ovName'])
            nested_dict = {}
            record = {}
            nested_dict[d["ovName"]] = {
                "description" : d['description'],
                "type" : d['type'],
                "sources" : list()
            }
            nested_dict[d["ovName"]]['sources'] = list()
        else:
            print(f"{d['ovName']} present in tag_set : {tag_set}")

            #nested_dict['type'] = d['type']
        nested_dict[d["ovName"]]['sources'].append({"sourceName":d['sourceName'],
                                  "sourceType":d['sourceType'],
                                  "sourceAttribute":d['sourceAttribute']})
        nested_data.append(nested_dict)

    json_data = json.dumps(nested_data, indent=1)
    #json_data = df.to_json(orient='records', indent=1)

    with open('output.json', 'w') as json_file:
        json_file.write((json_data))


if __name__=="__main__":
    print(f"Invoking..")
    csv_json_using_pandas()
    print(f"done..")