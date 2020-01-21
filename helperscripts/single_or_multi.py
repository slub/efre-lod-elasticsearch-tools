#!/usr/bin/python3
# determine if the values in the keys of an line-delimited json file are single or multi valued
#
import json
import sys
mapping={}

for line in sys.stdin:
    rec=json.loads(line)
    for k,v in rec.items():
        if k not in mapping:
            mapping[k]="single"
        if isinstance(v,list):
            mapping[k]="multi"

json.dumps(mapping,indent=4)
