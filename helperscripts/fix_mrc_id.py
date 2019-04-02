#!/usr/bin/python3

import json
import sys
from es2json import eprint, isint

def fix_mrc_id(jline):
    if "001" in jline and isinstance(jline["001"],list):
        _id=jline.pop("001")
        for elem in _id:
            jline["001"]=elem
            if elem=="0021114284":  # this particulary FINC-MARC21 Record is broken and will break the whole toolchain
                return None
    return jline

def valid_mrc_fields(jline):
    if jline:
        for key in jline:
            if isint(key) and len(str(int(key)))>1:
                for elem in jline[key]:
                    if isinstance(elem,str):
                        return None
    return jline



if __name__ == "__main__":
    for line in sys.stdin:
        try:
            jline=json.loads(line)
        except:
            eprint("corrupt json: "+str(line))
            continue
        jline=fix_mrc_id(jline)
        jline=valid_mrc_fields(jline)
        if jline:
            sys.stdout.write(json.dumps(jline)+"\n")
            sys.stdout.flush()

