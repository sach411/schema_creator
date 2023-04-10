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
                if ovType.split(".")[0] == "object":
                    oname = ovName.split(".")[0]
                    oname1= ovName.split(".")[1]
                    print(f"create under object..{oname}")
                    for item in nested_data:
                        if oname in item:
                            nested_dict[ovName]["type"] = ovType.split(".")[1]
                            item[oname]["properties"][oname1] = nested_dict[ovName]

                nested_dict[ovName]["x-collibra"] = {"primaryKey": primaryKey,
                                  "sources": list()}
                nested_dict[ovName]["x-collibra"]['sources'].append({"sourceName":sourceName,
                                          "sourceType":sourceType,
                                          "sourceAttribute":sourceAttribute})
            else:
                # for type "object", include "properties" tag
                print(f"object type. Adding properties tag.")
                nested_dict[ovName]["properties"] = {}

            #if ovType.split(".")[0] != "object":
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
    del_ind = []
    for ind, i in enumerate(nested_data):
        if "." in list(i.keys())[0]:
            print(f"{list(i.keys())[0]}")
            del_ind.append(ind)

    print(f"remove: {del_ind} from {len(nested_data)}, {nested_data}")

    for ele in sorted(del_ind, reverse=True):
        del nested_data[ele]

    json_data = json.dumps(nested_data, indent=1)

    with open('output.json', 'w') as json_file:
        json_file.write((json_data))


if __name__=="__main__":
    print(f"Invoking..")
    csv_json_using_pandas()
    print(f"done..")