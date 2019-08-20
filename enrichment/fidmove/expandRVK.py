#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator,isint,litter,eprint,esidfileconsumegenerator

def cleanup_rec(rec):
        if "about" in rec:
            if isinstance(rec["about"],dict):
                if "keywords" in rec["about"]:
                    if isinstance(rec["about"]["keywords"],list):
                        for n,elem in enumerate(rec["about"]):
                            if isinstance(elem,str):
                                del rec["about"]["keywords"][n]
                    elif isinstance(rec["about"]["keywords"],dict):
                        continue
                    elif isinstance(rec["about"]["keywords"],str):
                        rec["about"].pop("keywords")
            elif isinstance(rec["about"],list):
                for n,elem in enumerate(rec["about"]):
                    if "keywords" in elem:
                        if isinstance(elem["keywords"],list):
                            for m,elen in enumerate(elem["keywords"]):
                                if isinstance(elen,str):
                                    del rec["about"][n]["keywords"][m]
                        elif isinstance(elem["keywords"],dict):
                            continue
                        elif isinstance(elem["keywords"],str):
                            rec["about"][n].pop("keywords")
        return rec
    
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
    if record.get("about") and isinstance(record["about"],list):
        for n,rvk in enumerate(record.get("about")):
            if rvk and isinstance(rvk,dict) and rvk.get("identifier") and rvk["identifier"].get("propertyID") and rvk["identifier"]["propertyID"]=="RVK":
                rvktree=requests.get("http://194.95.145.44:9200/rvk/tree/"+rvk.get("@id").split("/")[-1])
#                rvktree=requests.get(rvk.get("@id"))
                if rvktree.ok:
                    change=True
                    record["about"][n]["keywords"]=[]
                    for name, uri in FuckUpWithRVKs(rvktree.json().get("_source")):
                        if uri!=rvk.get("@id"):
                            record["about"][n]["keywords"].append({"name":name,
                                        "@id":uri
                                        })
        for n,rvk in enumerate(record.get("about")):
            if rvk and isinstance(rvk,str):
                eprint(record)
                record["about"][n]={}
                record["about"][n]["keywords"]=[rvk]
        for n,rvk in enumerate(record.get("about")):
            if rvk and isinstance(rvk,dict) and rvk.get("keywords"):
                if isinstance(rvk["keywords"],list):
                    for m,elem in enumerate(rvk["keywords"]):
                        if isinstance(elem,str):
                            record["about"][n]["keywords"][m]={"name":elem}
                            change=True
    print(json.dumps(cleanup_rec(record))
                
                    
    
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
    searchbody={"query":{"match":{"about.identifier.propertyID":"RVK"}}}
    #for rec in esidfileconsumegenerator(idfile="fidmove_ids_rvk.txt",host=args.host,port=args.port,index=args.index,body=searchbody,type=args.type,headless=True):
    #        enrichrecord(rec)
    for rec in esgenerator(host=args.host,port=args.port,index=args.index,body=None,id=args.id,type=args.type,headless=True,verbose=True):
        enrichrecord(rec)
