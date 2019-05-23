#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import sys
import time
from requests import get
from es2json import esgenerator
from es2json import isint
from es2json import litter

def entityfacts(record,gnd,ef_instances):
    try:
        changed = False
        for url in ef_instances:
            r = get(url+str(gnd))
            if r.ok:
                data=r.json()
                if data.get("_source"):
                    data=data.get("_source")
                sameAsses=[] # ba-dum-ts
                if data.get("sameAs") and isinstance(data["sameAs"],list):
                    for sameAs in data.get("sameAs"):
                        if sameAs.get("@id"):
                            if not sameAs.get("@id").startswith("http://d-nb.info"):
                                sameAsses.append(sameAs.get("@id"))
                #print(sameAsses,url)
                if sameAsses:
                    record["sameAs"]=litter(record.get("sameAs"),sameAsses)
                    changed=True
                break
        return record if changed else None
    except Exception as e:
        time.sleep(5)
        return entityfacts(record,gnd,ef_instances)
 
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich ES by EF!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument("-id",type=str,help="retrieve single document (optional)")
    parser.add_argument('-searchserver',type=str,help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty") #no, i don't steal the syntax from esbulk...
    parser.add_argument('-stdin',action="store_true",help="get data from stdin")
    parser.add_argument('-pipeline',action="store_true",help="output every record (even if not enriched) to put this script into a pipeline")
    args=parser.parse_args()
    if args.searchserver:
        slashsplit=args.searchserver.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            args.port=args.searchserver.split(":")[2].split("/")[0]
        if len(slashsplit)>3:    
            args.index=args.searchserver.split("/")[3] 
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.searchserver:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    ef_instances=["http://"+args.host+":"+args.port+"/ef/gnd/","http://hub.culturegraph.org/entityfacts/"]
    if args.stdin:
        for line in sys.stdin:
            rec=json.loads(line)
            gnd=None
            record=None
            if rec.get("sameAs"):
                if isinstance(rec.get("sameAs"),list) and any("http://d-nb.info"in x for x in rec.get("sameAs")):
                    for item in rec.get("sameAs"):
                        if "http://d-nb.info" in item and len(item.split("/"))>4:
                            gnd=item.rstrip().split("/")[4]
                elif isinstance(rec.get("sameAs"),str) and "http://d-nb.info" in rec.get("sameAs"):
                    gnd=rec.get("sameAs").split("/")[4]
            if gnd:
                record=entityfacts(rec,gnd,ef_instances)
                if record:
                    rec=record
            if record or args.pipeline:
                print(json.dumps(rec,indent=None))
    else:
        for rec in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True,body={"query":{"prefix":{"sameAs.keyword":"http://d-nb.info"}}}):
            gnd=None
            if rec.get("sameAs"):
                if isinstance(rec.get("sameAs"),list) and any("http://d-nb.info" in x for x in rec.get("sameAs")):
                    for item in rec.get("sameAs"):
                        if "http://d-nb.info" in item and len(item.split("/"))>4:
                            gnd=item.split("/")[4]
                elif isinstance(rec.get("sameAs"),str) and "http://d-nb.info" in rec.get("sameAs"):
                    gnd=rec.get("sameAs").split("/")[4]
            if gnd:
                record=entityfacts(rec,gnd,ef_instances)
                if record:
                    rec=record
            if record or args.pipeline:
                print(json.dumps(rec,indent=None))
                
