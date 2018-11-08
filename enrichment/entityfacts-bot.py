#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
from elasticsearch import Elasticsearch
import json
import argparse
import sys, time
import os.path
import codecs
import syslog
from requests import get
from es2json import esgenerator
from es2json import isint
from es2json import litter



def entityfacts(record,gnd,ef_instances):
    changed = False
    for url in ef_instances:
        r = get(url+str(gnd))
        if r.ok:
            data=r.json()
            if data.get("_source"):
                data=data.get("_source")
            sameAsses=[] # ba-dum-ts
            for sameAs in data.get("sameAs"):
                if not sameAs.get("@id").startswith("http://d-nb.info"):
                    sameAsses.append(sameAs.get("@id"))
            #print(sameAsses,url)
            if sameAsses:
                record["sameAs"]=litter(record.get("sameAs"),sameAsses)
                changed=True
            break
    return record if changed else None

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich ES by EF!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument("-id",type=str,help="retrieve single document (optional)")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty") #no, i don't steal the syntax from esbulk...
    args=parser.parse_args()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    ef_instances=["http://"+args.host+":"+args.port+"/ef/gnd/","http://hub.culturegraph.org/entityfacts/"]
    for rec in esgenerator(host=args.host,port=args.port,index=args.index,id=args.id,type=args.type,headless=True,body={"query":{"match_phrase":{"sameAs":"http://d-nb.info"}}}):
        gnd=None
        if rec.get("sameAs"):
            if isinstance(rec.get("sameAs"),list) and any("http://d-nb.info" for x in rec.get("sameAs")):
                for item in rec.get("sameAs"):
                    if "http://d-nb.info" in item and len(item.split("/"))>4:
                        gnd=item.rstrip().split("/")[4]
            elif isinstance(rec.get("sameAs"),str) and "http://d-nb.info" in rec.get("sameAs"):
                gnd=rec.get("sameAs").split("/")[4]
        if gnd:
            newrec=entityfacts(rec,gnd,ef_instances)
            if newrec:
                print(json.dumps(newrec,indent=None))
