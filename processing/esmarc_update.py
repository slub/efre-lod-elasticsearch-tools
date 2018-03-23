#!/usr/bin/python3
from elasticsearch import Elasticsearch, exceptions, helpers
from multiprocessing import Pool
import esmarc
import argparse
import json
from es2json import esgenerator, eprint
from gnd2swb import resolve_uris


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
        if hit["_source"].get("normalized")=="false":
            for entity,obj in esmarc.process_line(es.get(args.index,args.type,hit["_id"])["_source"],args).items():
                es.index(entity,"schemaorg",obj,id=obj["identifier"])
                hit["_source"]["entity"]=entity
                hit["_source"]["normalized"]="true"
                hit["_source"]["lod_id"]=obj["identifier"]
        if hit["_source"].get("normalized")=="true" and "entity" in hit["_source"] and "lod_id" in hit["_source"] and hit["_source"].get("clean_uri")=="false":
            record=es.get(index=hit["_source"]["entity"],doc_type="schemaorg",id=hit["_source"]["lod_id"])
            if "_source" in record:
                newrecord=resolve_uris(record.get("_source"),args)
                if newrecord:
                    es.index(index=hit["_source"]["entity"],doc_type="schemaorg",id=hit["_source"]["lod_id"],body=newrecord)
                    hit["_source"]["clean_uri"]="true"
        es.index(index="queue",doc_type="ppns",id=hit["_id"],body=hit["_source"])
            
            ###delete! no, dont delete!
            ### set normalized to true!
