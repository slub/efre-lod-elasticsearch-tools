#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator,isint,litter,eprint,esidfilegenerator


def FuckUpWithRVKs(tree):
    if "notation" in tree:
        yield tree["benennung"], str("https://rvk.uni-regensburg.de/api/json/ancestors/" + tree["notation"])
    if "node" in tree:
        for rvks in FuckUpWithRVKs(tree["node"]):
            yield rvks
    elif "ancestor" in tree:
        for rvks in FuckUpWithRVKs(tree["ancestor"]):
            yield rvks

def enrichrecord(record):
    change=False
    for n,rvk in enumerate(record.get("about")):
        if rvk.get("identifier").get("propertyID")=="RVK":
            rvktree=requests.get(rvk.get("@id"))
            if rvktree.ok:
                change=True
                record["about"][n]["keywords"]=[]
                for name, uri in FuckUpWithRVKs(rvktree.json()):
                    if uri!=rvk.get("@id"):
                        record["about"][n]["keywords"].append({"name":name,
                                     "@id":uri
                                     })
    if change:
        print(json.dumps(record))
                
                    
    
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='expand RVK')
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
    searchbody={
  "query": {
    "match": {
      "about.identifier.propertyID": "RVK"
    }
  }
}
    for rec in esgenerator(host=args.host,port=args.port,index=args.index,body=searchbody,id=args.id,type=args.type,headless=True):
            enrichrecord(rec)
            
