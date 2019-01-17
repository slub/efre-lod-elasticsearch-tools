#!/usr/bin/python3
import sys
import json
import requests
import argparse
from es2json import eprint,litter

map=["gndSubjectCategory","fieldOfStudy","fieldOfActivity","biographicalOrHistoricalInformation"]



def process(record,dnb_uri,server):
    change=False                                    #   [0]   [1] [2]         [3]   [4,-1]
    r = requests.get(server+"/gnd-records/record/"+str(dnb_uri.split("/")[-1])) #	http: / / d-nb.info / gnd / 102859268X get the GND number
    if r.ok:
        for gndItem in map:
            if r.json().get("_source").get(gndItem):
                for elem in r.json().get("_source").get(gndItem):
                    newabout={"identifier":{"propertyID":gndItem,"@type":"PropertyValue","value": elem.split("/")[-1]}}
                    if elem.startswith("http"):
                        newabout["@id"]=elem
                    if gndItem=="fieldOfStudy":
                        fos=requests.get(server+"/gnd-records/record/"+elem.split("/")[-1])
                        if fos.ok and fos.json().get("_source").get("relatedDdcWithDegreeOfDeterminacy3"):
                            newabout["identifier"]=[newabout.pop("identifier")]
                            newabout["identifier"].append({"@type":"PropertyValue","propertyID":"DDC","value":fos.json().get("_source").get("relatedDdcWithDegreeOfDeterminacy3")[0].split("/")[-2][:3]})
                            if fos.json().get("_source").get("preferredNameForTheSubjectHeading"):
                                newabout["name"]=fos.json().get("_source").get("preferredNameForTheSubjectHeading")[0]
                            newabout["@id"]="http://purl.org/NET/decimalised#c"+fos.json().get("_source").get("relatedDdcWithDegreeOfDeterminacy3")[0].split("/")[-2][:3]
                    elif gndItem=="gndSubjectCategory":
                        url=server+"/gnd-subjects/subject/_search"
                        gsc=requests.post(url,json={"query":{"match":{"@id.keyword":elem}}})
                        if gsc.ok and gsc.json().get("hits").get("total")==1:
                            for hit in gsc.json().get("hits").get("hits"):
                                newabout["name"]=" ".join(hit.get("_source").get("skos:prefLabel").get("@value").replace("\n","").split())
                    if not record.get("about"):
                        record["about"]=newabout
                    else:
                        plzAdd=True
                        #print(elem,record.get("about"))
                        if isinstance(record.get("about"),dict) and record.get("about").get("@id") and elem not in record.get("about").get("@id"):
                            record["about"]=[record.pop("about")]
                        elif isinstance(record.get("about"),list):
                            for item in record.get("about"):
                                if item.get("@id") and elem in item.get("@id"):
                                    plzAdd=False
                                elif isinstance(item.get("identifier"),list):
                                    for ident_list_elem in item.get("identifier"):
                                        if ident_list_elem.get("@id") and elem in ident_list_elem.get("@id"):
                                            plzAdd=False
                        if plzAdd:
                            record["about"]=litter(record["about"],newabout)
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
