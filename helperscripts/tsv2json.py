#!/usr/bin/env python
# -*- coding:utf-8 -*-
#   transform geonames TSV Data to Elasticsearch-conform JSON with geo-information! dont forget to use the geo type mapping on the location field
#
#
def isfloat(num):
    try: 
        float(num)
        return True
    except (ValueError, TypeError):
        return False
    
import csv
import json
import collections
import sys
# aliases
OrderedDict = collections.OrderedDict

header = ["id","name","asciiname","alternateName","latitude","longitude","feature class","feature code","cc2","admin1 code","admin2 code","admin3 code","admin4 code","population","elevation","dem","timezone","modification date"]

data = []
with open(sys.argv[1], 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for row in reader:
        if row[0].strip()[0] == '#':  #
            continue
        if not isfloat(row[4]) and not isfloat(row[5]):
            eprint(row)
        else:
            record=OrderedDict(zip(header, row))
            record["location"]={"lat":record.pop("latitude"),
                                "lon":record.pop("longitude")}
            print(json.dumps(record,indent=None))
