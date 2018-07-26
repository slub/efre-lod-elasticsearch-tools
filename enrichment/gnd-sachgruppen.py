#!/usr/bin/python3
import sys
import json
import requests

server="http:/127.0.0.1:9200/gnd-records/record/"

def process(record,dnb_uri):
    r = requests.get(server+dnb_uri.replace("/","%2F").replace(":","%3A"))
    if r.ok and r.json().get("_source").get("gndSubjectCategory"):
        for elem in r.json().get("_source").get("gndSubjectCategory"):
            if not record.get("about"):
                record["about"]={"identifier":{"propertyID":"GND-Sachgruppe","@type":"PropertyValue","value": elem}}
            else:
                record["about"]=[record.pop("about")]
                record["about"].append({"identifier":{"propertyID":"GND-Sachgruppe","@type":"PropertyValue","value": elem}})
        return record
        
def find_gnd(jline):
    if isinstance(jline.get("sameAs"),str):
        if jline.get("sameAs").startswith("http://d-nb.info"):
            return process(jline,jline.get("sameAs"))
        else:
            return None
    elif isinstance(jline.get("sameAs"),list):
        for elem in jline.get("sameAs"):
            if isinstance(elem,str) and elem.startswith("http://d-nb.info"):
                return process(jline,elem)
            
if __name__ == "__main__":
    for line in sys.stdin:
        try:
            jline=json.loads(line)
            if jline.get("about"):
                print(jline.get("about"))
        except:
            eprint("corrupt json: "+str(line))
            continue
        newrec=find_gnd(jline)
        if newrec:
            print(json.dumps(newrec,indent=None))
