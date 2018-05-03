#!/usr/bin/python3
from esmarc import getmarc
import json
import sys

    
if __name__ == "__main__":
    out={}
    for line in sys.stdin:
        snine=getmarc(json.loads(line),"079..b",None)
        if not out.get(snine):
            out[snine]=None
            out[snine]=open(str(snine)+"-out.ldj","w")
        out[snine].write(line)
    for entity in out:
        out[entity].close()
