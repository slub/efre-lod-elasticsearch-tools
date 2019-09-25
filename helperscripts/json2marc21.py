#!/usr/bin/python3
# -*- coding:utf-8 -*-
#


import json
import sys
from pymarc import Record, Field
from es2json import isint

def transpose_to_marc21(record):
    Mrecord=Record(force_utf8=True)
    Mrecord.leader=record["_LEADER"]
    for field in record:
        if isint(field):
            if int(field)<10:
                if isinstance(record[field],list):
                    for elem in record[field]:
                        Mrecord.add_field(Field(tag=field,data=elem))
                elif isinstance(record[field],str):
                    Mrecord.add_field(Field(tag=field,data=record[field]))
            else:
                for subfield in record[field]:
                    for ind, values in subfield.items():
                        indicators=[]
                        subfields=[]
                        for elem in values:
                            for k,v in elem.items():
                                if isinstance(v,str):
                                    subfields.append(k)
                                    subfields.append(v)
                                elif isinstance(v,list):
                                    for subfield_elem in v:
                                        subfields.append(k)
                                        subfields.append(subfield_elem)
                        for elem in ind:
                            indicators.append(elem)
                        Mrecord.add_field(Field(tag=str(field),
                                                indicators=indicators,
                                                subfields=subfields))
    return Mrecord.as_marc()

def main():
    try:
        for line in sys.stdin:
            record=json.loads(line,encoding='utf-8')
            transpose_to_marc21(record)
            sys.stdout.buffer.write(transpose_to_marc21(record))
            #sys.stdout.flush()
    except UnicodeDecodeError as e:
        eprint("unicode decode error: {}".format(e))
        eprint(record)

if __name__ == "__main__":
    main()
