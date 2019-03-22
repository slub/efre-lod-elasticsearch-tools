#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator,isint,litter,eprint,esidfilegenerator

def enrichrecord(record,host,port):
    confirm=["Zugl:", "Zugl.:", "Teilw. zugl.:", "Vollst. zugl.:"]
    for conf in confirm:
        if isinstance(record.get("Thesis"),str) and record.get("Thesis").startswith(conf):
            location=record.get("Thesis")[len(conf):].split(",")[0].strip()
            wL=requests.get("http://{host}:{port}/fidmove-enriched-aut/schemaorg/_search?q=workLocation.name:{location}&_source=workLocation".format(host=host,port=port,location=location))
            if wL.ok and wL.json()["hits"]["hits"]:
                wLnode=wL.json()["hits"]["hits"][0].get("_source").get("workLocation")
                if isinstance(wLnode,dict):
                    record["location"]=wLnode
                    print(json.dumps(record))
                elif isinstance(wLnode,list):
                    record["location"]=wLnode[0]
                    print(json.dumps(record))
                    
    #print(json.dumps(record))

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich titledata by DNB-Titledata over ISBN!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument("-id",type=str,help="retrieve single document (optional)")
    parser.add_argument('-stdin',action="store_true",help="get data from stdin")
    parser.add_argument('-pipeline',action="store_true",help="output every record (even if not enriched) to put this script into a pipeline")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty") #no, i don't steal the syntax from esbulk...
    parser.add_argument('-searchserver',type=str,help="search instance to use. default is -server e.g. http://127.0.0.1:9200")
    parser.add_argument('-idfile',type=str,help="path to a file with IDs to process")
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
    if args.searchserver:
        slashsplit=args.searchserver.split("/")
        search_host=slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            search_port=args.searchserver.split(":")[2].split("/")[0]
    else:
        search_host=args.host
        search_port=args.port
    if args.stdin:
        for line in sys.stdin:
            isbn10=None
            isbn13=None
            record=None
            rec=json.loads(line)
            if rec.get("isbn"):
                if isinstance(rec.get("isbn"),list):
                    for item in rec.get("isbn"):
                        if len(item)==10:
                            isbn10=item
                        elif len(item)==13:
                            isbn13=item
                elif isinstance(rec.get("isbn"),str):
                    if len(rec.get("isbn"))==10:
                        isbn10=rec.get("isbn")
                    elif len(rec.get("isbn"))==13:
                        isbn13=rec.get("isbn")
            if isbn10 or isbn13:
                record=get_gndbyISBN(rec,search_host,search_port,isbn10,isbn13)
                if record:
                    rec=record
            if record or args.pipeline:
                print(json.dumps(rec,indent=None))
    elif args.idfile:
        for rec in esidfilegenerator(host=args.host,
                       port=args.port,
                       index=args.index,
                       type=args.type,
                       idfile=args.idfile,
                       headless=True,
                       timeout=600
                        ):
            isbn10=None
            isbn13=None
            record=None
            if rec and rec.get("isbn"):
                if isinstance(rec.get("isbn"),list):
                    for item in rec.get("isbn"):
                        if len(item)==10:
                            isbn10=item
                        elif len(item)==13:
                            isbn13=item
                elif isinstance(rec.get("isbn"),str):
                    if len(rec.get("isbn"))==10:
                        isbn10=rec.get("isbn")
                    elif len(rec.get("isbn"))==13:
                        isbn13=rec.get("isbn")
            if isbn10 or isbn13:
                record=get_gndbyISBN(rec,search_host,search_port,isbn10,isbn13)
                if record:
                    rec=record
    for rec in esgenerator(host=args.host,port=args.port,index=args.index,id=args.id,type=args.type,headless=True,body={"query": {"exists":{"field": "Thesis"}}}):
            enrichrecord(rec,args.host,args.port)
            
