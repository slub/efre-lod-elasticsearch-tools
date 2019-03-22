#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator,isint,litter,eprint,esidfilegenerator
from rdflib import Graph

def enrichrecord(record,hit):
    record["sameAs"]=litter(record.get("sameAs"),hit.get("_id"))
    if 'http://www.w3.org/2002/07/owl#sameAs' in hit.get("_source"):
        for sameAs in hit.get("_source").get('http://www.w3.org/2002/07/owl#sameAs'):
            record["sameAs"]=litter(record.get("sameAs"),sameAs.get("@id"))
    if "http://purl.org/dc/terms/subject" in hit.get("_source"):
        for subject in hit.get("_source").get("http://purl.org/dc/terms/subject"):
            if subject.get("@id").startswith("http://d-nb.info/gnd/"):
                sub={"identifier":{"@type":"PropertyValue","propertyID":"gndSubject","value":subject.get("@id")},"@id":subject.get("@id")}
                record["about"]=litter(record.get("about"),sub)
    return record
    
def get_gndbyISBN(record,search_host,search_port,isbn10,isbn13):
    changed = False
    if isbn13 and isbn10:
        for hit in esgenerator(host=search_host,port=search_port,index="dnb-titel",id=args.id,type="resources",body={"query":{"bool":{"must":[{"match":{"http://purl.org/ontology/bibo/isbn13.@value.keyword":str(isbn13)}},{"match":{"http://purl.org/ontology/bibo/isbn10.@value.keyword":str(isbn10)}}]}}},source=["http://purl.org/dc/terms/subject","http://www.w3.org/2002/07/owl#sameAs"],source_exclude=None,source_include=None,headless=False,timeout=60):
            record=enrichrecord(record,hit)
            changed=True
    if isbn13 and changed==False:
        for hit in esgenerator(host=search_host,port=search_port,index="dnb-titel",id=args.id,type="resources",body={"query":{"match":{"http://purl.org/ontology/bibo/isbn13.@value.keyword":str(isbn13)}}},source=["http://purl.org/dc/terms/subject","http://www.w3.org/2002/07/owl#sameAs"],source_exclude=None,source_include=None,headless=False,timeout=60):
            record=enrichrecord(record,hit)
            changed=True
    elif isbn10 and changed==False:
        for hit in esgenerator(host=search_host,port=search_port,index="dnb-titel",id=args.id,type="resources",body={"query":{"match":{"http://purl.org/ontology/bibo/isbn10.@value.keyword":str(isbn10)}}},source=["http://purl.org/dc/terms/subject","http://www.w3.org/2002/07/owl#sameAs"],source_exclude=None,source_include=None,headless=False,timeout=60):
            record=enrichrecord(record,hit)
            changed=True
    if changed:
        return record

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
            if record or args.pipeline:
                print(json.dumps(rec,indent=None))
    else:                                                                                                   
        for rec in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True,body={"query": {"bool": {"filter": {"exists": {"field": "relatedEvent"}},"must":{"exists":{"field": "isbn"}}}}}):
            isbn10=None
            isbn13=None
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
            
