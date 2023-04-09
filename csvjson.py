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
        #print(f"{d}")
        ovName = d['ovName']
        description = d['description']
        ovType = d['type']
        primaryKey = d['primaryKey']
        sourceName = d['sourceName']
        sourceType  =d['sourceType']
        sourceAttribute = d['sourceAttribute']

        #if ovType == "object":
            #continue

        if ovName not in tag_set:

            print(f"New Tag: {ovName} -> {ovType}-> {tag_set}")
            tag_set.add(ovName)

            nested_dict = {}

            nested_dict[ovName] = {
                "description" : description,
                "type" : ovType
            }

            # for type object, no need for x-collibra tag.
            if ovType != "object":
                print(f"object type. Adding only needed tags.")
                nested_dict[ovName]["x-collibra"] = {"primaryKey": primaryKey,
                                  "sources": list()}
                nested_dict[ovName]["x-collibra"]['sources'].append({"sourceName":sourceName,
                                          "sourceType":sourceType,
                                          "sourceAttribute":sourceAttribute})


            nested_data.append(nested_dict)

        else:
            print(f"Existing Tag : {ovName} -> {ovType}->  {tag_set}")
            for item in nested_data:
                if ovName in item:
                    #print(f">>>item: {type(item)} --> {item}")
                    item[ovName]["x-collibra"]['sources'].append({"sourceName":sourceName,
                                      "sourceType":sourceType,
                                      "sourceAttribute":sourceAttribute})
                    break


    json_data = json.dumps(nested_data, indent=1)

    with open('output.json', 'w') as json_file:
        json_file.write((json_data))


if __name__=="__main__":
    print(f"Invoking..")
    csv_json_using_pandas()
    print(f"done..")