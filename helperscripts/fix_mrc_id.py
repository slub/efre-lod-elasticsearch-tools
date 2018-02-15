#!/usr/bin/python3

import json
import sys
from es2json import eprint

for line in sys.stdin:
    try:
        jline=json.loads(line)
    except:
        eprint("corrupt json: "+str(line))
        continue
    _id=jline.pop("001")
    for elem in _id:
        jline["001"]=elem
    sys.stdout.write(json.dumps(jline)+"\n")

