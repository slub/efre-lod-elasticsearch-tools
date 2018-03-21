#!/usr/bin/python3

import json
import sys
from es2json import eprint

def fix_mrc_id(jline):
    if "001" in jline and isinstance(jline["001"],list):
        _id=jline.pop("001")
        for elem in _id:
            jline["001"]=elem
    return jline

if __name__ == "__main__":
    for line in sys.stdin:
        try:
            jline=json.loads(line)
        except:
            eprint("corrupt json: "+str(line))
            continue
        jline=fix_mrc_id(jline)
        sys.stdout.write(json.dumps(jline)+"\n")
        sys.stdout.flush()

