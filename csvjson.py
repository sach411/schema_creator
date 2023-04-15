import pandas as pd
import json
import os

def get_param(param_name, default_value):
    return os.getenv(param_name, default_value)

def get_data_dict_from_csv(input_csv=None):
    df = pd.read_csv(input_csv)
    data_dict = df.to_dict(orient='records')
    return data_dict

def process(data_dict=None, x_collibra=None):
    """process list of data_dict to create the nested data dict"""
    nested_data = []
    tag_set = set()

    for index, d in enumerate(data_dict):
        #print(f"{d}")
        ovName = d['ovName']
        description = d['description']
        ovType = d['type']
        print(f"\t{index+1}->{ovName} -> {ovType}-> {description}")

        if x_collibra:
            primaryKey = d['primaryKey']
            sourceName = d['sourceName']
            sourceType  =d['sourceType']
            sourceAttribute = d['sourceAttribute']

        if ovName not in tag_set:
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
                    # print(f"create under object..{oname}")
                    for item in nested_data:
                        if oname in item:
                            nested_dict[ovName]["type"] = ovType.split(".")[1]
                            item[oname]["properties"][oname1] = nested_dict[ovName]

                if x_collibra:
                    nested_dict[ovName]["x-collibra"] = {"primaryKey": primaryKey,
                                      "sources": list()}
                    nested_dict[ovName]["x-collibra"]['sources'].append({"sourceName":sourceName,
                                              "sourceType":sourceType,
                                              "sourceAttribute":sourceAttribute})
            else:
                # for type "object", include "properties" tag
                nested_dict[ovName]["properties"] = {}

            nested_data.append(nested_dict)
        else:
            #print(f"Existing Tag : {ovName} -> {ovType}->  {tag_set}")
            for item in nested_data:
                if ovName in item:
                    if x_collibra:
                        item[ovName]["x-collibra"]['sources'].append({"sourceName":sourceName,
                                          "sourceType":sourceType,
                                          "sourceAttribute":sourceAttribute})
                        break
    del_ind = []
    for ind, i in enumerate(nested_data):
        if "." in list(i.keys())[0]:
            print(f"{list(i.keys())[0]}")
            del_ind.append(ind)

    # print(f"remove: {del_ind} from {len(nested_data)}, {nested_data}")

    for ele in sorted(del_ind, reverse=True):
        del nested_data[ele]

    nested_data = {key: value for dictionary in nested_data for key, value in dictionary.items()}

    return nested_data

def generate_schema_file(output_file=None, json_data=None):
    print(f"****{generate_schema_file.__name__}: {output_file}")
    with open(output_file, 'w') as json_file:
        json_file.write((json_data))

def csv_json_using_pandas():
    """

    :return:
    """
    # pass schema_file, name (optional), description(optional) as env variable
    input_csv = get_param("schema_file","input.csv")
    base_name, extension = os.path.splitext(input_csv)
    schema_name = get_param("name",base_name)
    schema_description = get_param("description", f"{base_name} - schema")
    schema = {"title" : schema_name,
              "description": schema_description,
              "type" : "object", "properties":{}}

    print(f"Invoking {csv_json_using_pandas.__name__} for schema:`{schema_name}`, description:`{schema_description}`")
    data_dict = get_data_dict_from_csv(input_csv="input.csv")
    print(f'total records in input file: `{input_csv}`: {len(data_dict)}')

    # collibra tag included file
    nested_data=process(data_dict=data_dict,x_collibra=True)
    json_data_x_collibra = json.dumps(nested_data, indent=1)
    out_file_collibra = f"{schema_name}-x-collibra.json"
    generate_schema_file(output_file=out_file_collibra, json_data=json_data_x_collibra)

    # no collibra tag file
    nested_data=process(data_dict=data_dict,x_collibra=False)
    json_data = json.dumps(nested_data, indent=1)
    out_file=f"{schema_name}.json"
    generate_schema_file(output_file=out_file, json_data=json_data)

if __name__=="__main__":
    print(f"Invoking..")
    csv_json_using_pandas()
    print(f"done..")