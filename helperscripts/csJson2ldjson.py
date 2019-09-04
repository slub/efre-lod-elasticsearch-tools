#!/usr/bin/python3

from json import dumps
import sys
import ijson.backends.yajl2_cffi as ijson

def yield_obj(path,basepath):
    with open(path,"rb") as fin:
        builder=ijson.common.ObjectBuilder()
        for prefix,event,val in ijson.parse(fin):
            try:
                builder.event(event,val)
            except:
                if hasattr(builder,"value"):
                    print(builder.value)
            if prefix==basepath and event=="end_map":
                if hasattr(builder,"value"):
                    yield builder.value
                builder=ijson.common.ObjectBuilder()

for obj in yield_obj(sys.argv[1],"item.item"):
    print(dumps(obj))
