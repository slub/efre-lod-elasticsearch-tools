#!/usr/bin/python3
from elasticsearch import Elasticsearch, exceptions, helpers
import esmarc
import argparse
import json
from es2json import esgenerator, eprint

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-prefix',type=str,default="",help='Prefix to use for output data')
    parser.add_argument('-debug',action="store_true",help='Dump processed Records to stdout (mostly used for debug-purposes)')
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    es=Elasticsearch([{'host':args.host}],port=args.port)
    if args.host: #if inf not set, than try elasticsearch
        for hit in esgenerator(host=args.host,port=args.port,index="queue",type="ppns",headless=False):
            for entity,obj in esmarc.process_line(es.get(args.index,args.type,hit["_id"])["_source"],args).items():
                es.index(entity,"schemaorg",obj,id=obj["identifier"])
                eprint("updated "+hit["_id"]+" "+obj["identifier"])
                es.delete(args.index,args.type,id=hit["_id"])
            #returnobj=process_line(hits,args)
            #if isinstance(returnobj,dict):
            #    for k,v in returnobj.items():
            #        output(k,v,outstream)
