"""Decode the disposable InfluxDB's annotated CSV without losing int64 digits."""
import csv
import json
import sys

header, types, result = None, None, []
for row in csv.reader(sys.stdin):
    if not row:
        continue
    if row[0] == '#datatype':
        types = row
    elif row[0].startswith('#'):
        continue
    elif '_field' in row and '_value' in row:
        header = row
    elif header and types:
        assert len(row) == len(header) == len(types), 'invalid fixture CSV'
        result.append({'type': types[header.index('_value')],
                       'data': dict(zip(header, row))})
print(json.dumps(result))
