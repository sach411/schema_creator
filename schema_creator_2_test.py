import pytest
import os
import json
from schema_creator_2 import csv_to_json

@pytest.fixture(scope="module")
def csv_file_path(tmpdir_factory):
    # Create a temporary CSV file
    csv_data = '''"ovName","description","type","primaryKey","sourceName","sourceType","sourceAttribute"
    "fundSFDCId","Fund Id","string","false","Salesforce Data Dictionary","Service","FM_fund.Fund_id_c"
    "fundSFDCId","Fund Id","string","false","IDR","Database","Fund.FundId"
    "fundName","Fund Name","string","true","Salesforce Data Dictionary","Service","FM_fund.Fund_Legal_Name"
    "fundSFDCId","Fund Id","string","false","IDR","Database","Fund.FundId"
    "dealingTerms","dealing Terms","object","","","",""
    "dealingTerms.redemptionAdvanceNoticedays","redemption Advance Noticedays","object.number","false","Salesforce Data Dictionary","Service","FM_fund.redemptionAdvanceNoticedays"
    "dealingTerms.redemptionAdvanceNoticeDayOfMonth","redemption Advance Notice DayOfMonth","object.string","false","Salesforce Data Dictionary","Service","FM_fund.redemptionAdvanceNoticeDayOfMonth"'''
    csv_file = tmpdir_factory.mktemp("data").join("data.csv")
    csv_file.write(csv_data)
    return str(csv_file)

@pytest.fixture(scope="module")
def expected_json():
    # Define the expected JSON
    expected_json = {
        "fundSFDCId": {
            "description": "Fund Id",
            "type": "string",
            "x-collibra": {
                "primaryKey": False,
                "sources": [
                    {
                        "sourceName": "Salesforce Data Dictionary",
                        "sourceType": "Service",
                        "sourceAttribute": "FM_fund.Fund_id_c"
                    },
                    {
                        "sourceName": "IDR",
                        "sourceType": "Database",
                        "sourceAttribute": "Fund.FundId"
                    }
                ]
            }
        },
        "fundName": {
            "description": "Fund Name",
            "type": "string",
            "x-collibra": {
                "primaryKey": True,
                "sources": [
                    {
                        "sourceName": "Salesforce Data Dictionary",
                        "sourceType": "Service",
                        "sourceAttribute": "FM_fund.Fund_Legal_Name"
                    }
                ]
            }
        },
        "dealingTerms": {
            "description": "dealing Terms",
            "type": "object",
            "properties": {
                "redemptionAdvanceNoticedays": {
                    "description": "redemption Advance Noticedays",
                    "type": "number",
                    "x-collibra": {
                        "primaryKey": False,
                        "sources": [
                            {
                                "sourceName": "Salesforce Data Dictionary",
                                "sourceType": "Service",
                                "sourceAttribute": "FM_fund.redemptionAdvanceNoticedays"
                            }
                        ]
                    }
                },
                "redemptionAdvanceNoticeDayOfMonth": {
                    "description": "redemption Advance Notice DayOfMonth",
                    "type": "string",
                    "x-collibra": {
                        "primaryKey": False,
                        "sources": [
                            {
                                "sourceName": "Salesforce Data Dictionary",
                                "sourceType": "Service",
                                "sourceAttribute": "FM_fund.redemptionAdvanceNoticeDayOfMonth"
                            }
                        ]
                    }
                }
            }
        }
    }
    return expected_json

def test_csv_to_json(csv_file_path, expected_json):
    # Create a temporary JSON file
    json_file_path = os.path.join(os.path.dirname(csv_file_path), "data.json")

    # Convert CSV to JSON
    csv_to_json(csv_file_path, json_file_path)

    # Read the resulting JSON file
    with open(json_file_path, 'r') as json_file:
        result_json = json.load(json_file)

    # Compare the result with the expected JSON
    assert result_json == expected_json

# Example usage
csv_file_path = 'data.csv'
json_file_path = 'data.json'
csv_to_json(csv_file_path, json_file_path)
