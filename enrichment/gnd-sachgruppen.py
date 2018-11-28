#!/usr/bin/python3
import sys
import json
import requests
import argparse
from es2json import eprint

map=["gndSubjectCategory","fieldOfStudy","fieldOfActivity","biographicalOrHistoricalInformation"]



def process(record,dnb_uri,server):
    server+="/gnd-records/record/"
    change=False                                    #   [0]   [1] [2]         [3]   [4,-1]
    r = requests.get(server+dnb_uri.split("/")[-1]) #	http: / / d-nb.info / gnd / 102859268X get the GND number 
    if r.ok:
        for gndItem in map:
            if r.json().get("_source").get(gndItem):
                for elem in r.json().get("_source").get(gndItem):
                    if not record.get("about"):
                        record["about"]={"identifier":{"propertyID":gndItem,"@type":"PropertyValue","value": elem}}
                        change=True
                    else:
                        if isinstance(record.get("about"),dict) and elem not in record.get("about").get("identifier").get("value"):
                            record["about"]=[record.pop("about")]
                            record["about"].append({"identifier":{"propertyID":gndItem,"@type":"PropertyValue","value": elem}})
                            change=True
                        elif isinstance(record.get("about"),list):
                            dont_add=False
                            for item in record.get("about"):
                                if elem in item.get("identifier").get("value"):
                                    dont_add=True
                            if dont_add==False:
                                record["about"].append({"identifier":{"propertyID":gndItem,"@type":"PropertyValue","value": elem}})
                                change=True
    return record if change else None
        
def find_gnd(jline,server):
    if isinstance(jline.get("sameAs"),str):
        if jline.get("sameAs").startswith("http://d-nb.info"):
            return process(jline,jline.get("sameAs"),server)
        else:
            return jline
    elif isinstance(jline.get("sameAs"),list):
        for elem in jline.get("sameAs"):
            if isinstance(elem,str) and elem.startswith("http://d-nb.info"):
                newrec=process(jline,elem,server)
                if newrec:
                    jline=newrec
        return jline
    
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich ES by GND Sachgruppen!!')
    parser.add_argument('-pipeline',action="store_true",help="output every record (even if not enriched) to put this script into a pipeline")
    parser.add_argument('-searchserver',type=str,help="use http://host:port/") #the server where to search...
    args=parser.parse_args()
        
    for line in sys.stdin:
        try:
            jline=json.loads(line)
        except:
            eprint("corrupt json: "+str(line))
            continue
        newrec=find_gnd(jline,args.searchserver)
        if newrec:
            print(json.dumps(newrec,indent=None))
        elif not newrec and args.pipeline:
            print(json.dumps(jline,indent=None))
        else:
            continue
