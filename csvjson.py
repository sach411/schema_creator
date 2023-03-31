import csv
import json

with open('input.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    rows = list(reader)

with open('output.json', 'w') as jsonfile:
    json.dump(rows, jsonfile, indent=1)

    