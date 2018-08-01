#!/usr/bin/python3
import sys
import json
import requests
from es2json import eprint

server="http://127.0.0.1:9200/gnd-records/record/"

def process(record,dnb_uri):
    r = requests.get(server+dnb_uri.replace("/","%2F").replace(":","%3A"))
    if r.ok and r.json().get("_source").get("gndSubjectCategory"):
            for elem in r.json().get("_source").get("gndSubjectCategory"):
                if not record.get("about"):
                    record["about"]={"identifier":{"propertyID":"GND-Sachgruppe","@type":"PropertyValue","value": elem}}
                else:
                    if isinstance(record.get("about"),dict) and elem not in record.get("about").get("identifier").get("value"):
                        record["about"]=[record.pop("about")]
                        record["about"].append({"identifier":{"propertyID":"GND-Sachgruppe","@type":"PropertyValue","value": elem}})
                    elif isinstance(record.get("about"),list):
                        dont_add=False
                        for item in record.get("about"):
                            if elem in item.get("identifier").get("value"):
                                dont_add=True
                        if dont_add==False:
                            record["about"].append({"identifier":{"propertyID":"GND-Sachgruppe","@type":"PropertyValue","value": elem}})
    return record
        
def find_gnd(jline):
    if isinstance(jline.get("sameAs"),str):
        if jline.get("sameAs").startswith("http://d-nb.info"):
            return process(jline,jline.get("sameAs"))
        else:
            return jline
    elif isinstance(jline.get("sameAs"),list):
        for elem in jline.get("sameAs"):
            if isinstance(elem,str) and elem.startswith("http://d-nb.info"):
                jline=process(jline,elem)
        return jline
if __name__ == "__main__":
    for line in sys.stdin:
        try:
            jline=json.loads(line)
        except:
            eprint("corrupt json: "+str(line))
            continue
        newrec=find_gnd(jline)
        if newrec:
            print(json.dumps(newrec,indent=None))
