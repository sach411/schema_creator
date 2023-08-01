import os

import pandas as pd
from pandas import DataFrame, Series
import json
from datetime import datetime
from dataclasses import dataclass


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
COMPLEX_TYPE = ["array", "object"]

WARN_1_ARRAY_NO_SOURCE_DEFINED = []
ERROR_1_ARRAY_NO_TYPE_DEFINED = []
ERRORS_WARNINGS=[{"WARN_1_ARRAY_NO_SOURCE_DEFINED":WARN_1_ARRAY_NO_SOURCE_DEFINED},
                 {"ERROR_1_ARRAY_NO_TYPE_DEFINED": ERROR_1_ARRAY_NO_TYPE_DEFINED}]

@dataclass
class Record:
    parent_tag: str
    ov_name: str
    description: str
    ov_type: str
    ov_node_type: str
    ov_node_subtype: str
    primary_key: bool
    source_name: str
    source_type : str
    source_attribute: str
    uniqueItems: bool
    record_num: int






def csv_df(csv_file_path:str) -> DataFrame:
    """
    returns DataFrame from csv
    :param csv_file_path:
    :return:
    """
    # Read the CSV file using pandas and specify column types as string
    df = pd.read_csv(csv_file_path, dtype=str)

    # Identify duplicate rows based on selected columns
    duplicates = df[df.duplicated(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], keep=False)]

    # Drop duplicate rows based on selected columns
    df.drop_duplicates(subset=[COLUMN_OV_NAME, COLUMN_SOURCE_NAME, COLUMN_SOURCE_TYPE, COLUMN_SOURCE_ATTRIBUTE], inplace=True)

    return df


def records_to_process(df:DataFrame) -> DataFrame:
    """
    returns DataFrame based on env variables.
    :param df:
    :return:
    """
    # process only first `n` rows passed as env variable
    # process only records present in `process_records` string passed as env var.
    records_to_process = int(os.getenv('n', -1))
    if records_to_process > 0:
        df = df.head(records_to_process)

    process_records = os.getenv('process_records')
    if process_records:
        prs = process_records.split(',')
        #TEMP: TODO: remove
        prs1 = ['distributionPolicy', 'fundPrimeBroker', 'primeBrokerAccountNumber',
                           'primeBrokerAccountName', 'feesSchedule', 'feesTieredSchedule']
        prs1 = ['feesSchedule', 'feesTieredSchedule']

        df = df[df[COLUMN_OV_NAME].isin((prs)) | df[COLUMN_PARENT_TAG].isin(prs)]

    return df


def get_row(rownum:int, row: Series)-> Record:
    """
    returns Record dataclass
    :param rownum:
    :param row:
    :return:
    """
    parent_tag = row[COLUMN_PARENT_TAG].strip() if not pd.isna(row[COLUMN_PARENT_TAG]) else ""
    ov_name = row[COLUMN_OV_NAME]
    description = row[COLUMN_DESCRIPTION].strip() if not pd.isna(row[COLUMN_DESCRIPTION]) else ""
    ov_type = row[COLUMN_TYPE]
    # actual type of the ov node
    ovnt = ov_type.split('.')
    ov_node_type = ovnt[0]
    # for array, subtype can be object or basic types
    ov_node_subtype = ""
    if len(ovnt) == 2:
        ov_node_subtype = ovnt[1]
    if ov_node_type == NODE_ARRAY and ov_node_subtype == "":
        ERROR_1_ARRAY_NO_TYPE_DEFINED.append(f"{rownum}-{ov_name}")
    primary_key = True if not pd.isna(row[COLUMN_PRIMARY_KEY]) and (
        row[COLUMN_PRIMARY_KEY].strip()).lower() == "true" else False
    source_name = row[COLUMN_SOURCE_NAME].strip() if not pd.isna(row[COLUMN_SOURCE_NAME]) else row[COLUMN_SOURCE_NAME]
    source_type = row[COLUMN_SOURCE_TYPE].strip() if not pd.isna(row[COLUMN_SOURCE_TYPE]) else row[COLUMN_SOURCE_TYPE]
    source_attribute = row[COLUMN_SOURCE_ATTRIBUTE].strip() if not pd.isna(row[COLUMN_SOURCE_ATTRIBUTE]) else row[
        COLUMN_SOURCE_ATTRIBUTE]
    uniqueItems = True if not pd.isna(row[COLUMN_UNIQUEITEMS]) and row[
        COLUMN_UNIQUEITEMS].strip().lower() == "true" else False

    r = Record(record_num=rownum, parent_tag=parent_tag, ov_name=ov_name, description=description, ov_type=ov_type,ov_node_type=ov_node_type,
           ov_node_subtype=ov_node_subtype,primary_key=primary_key,
           source_name=source_name,source_type=source_type, source_attribute=source_attribute, uniqueItems=uniqueItems)

    print(f"{r}")

    return r


def process_record(r:Record, json_data:dict):
    """
    process current record and update main json_data with the node
    :param r:
    :param json_data:
    :return:
    """
    node = { r.ov_name : {
        NODE_TITLE: r.ov_name,
        NODE_DESCRIPTION: r.description,
        NODE_TYPE: r.ov_node_type

    }}
    # identify the record type
    # simple
    if r.ov_node_type in BASIC_TYPES:
        if r.ov_name in json_data:
            pass
    # array
    # - needs items, uniqueItems

    # object
    # - needs properties

    # is this record having parentTag?




def csv_to_json(csv_file_path, json_file_path):
    """

    :param csv_file_path:
    :param json_file_path:
    :return:
    """
    # get DataFrame to work with
    df = csv_df(csv_file_path)

    # filter based on env var.
    df = records_to_process(df)

    df_rows, df_columns = df.shape
    print(f"Input file: {csv_file_path} \nProcessing {df_rows} records: {df[COLUMN_OV_NAME].to_list()}")

    # iterate thr records to build final json
    json_data = {}

    for rownum, row in df.iterrows():
        r = get_row(rownum, row)






# Example usage
csv_file_path = 'c2j.csv'
json_file_path = 'c2j.1.json'
csv_to_json(csv_file_path, json_file_path)
print(f"<{csv_file_path}, {json_file_path}>")
print(f"{ERRORS_WARNINGS}")