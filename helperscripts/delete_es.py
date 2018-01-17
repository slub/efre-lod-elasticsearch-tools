#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
import json
import argparse
import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from multiprocessing import Pool
from es2json import esgenerator
from es2json import eprint

#delete only a specific type inside an existing index

args=None
tes=None

def delete(record_id):
    eprint("deleting: "+record_id)
    tes.delete(index=args.index,doc_type=args.type,id=record_id)


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='simple ES.Getter!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    args=parser.parse_args()
    tes=Elasticsearch([{'host':args.host}],port=args.port)
    bulk = ""
    count=0
    for record in esgenerator(args.host,args.port,args.index,args.type,source=False,headless=False):      
        bulk = bulk + '{ "delete" : { "_index" : "' + str(args.index) + '", "_type" : "' + str(args.type) + '", "_id" : "' + str(record['_id']) + '" } }\n'
        count+=1
        if count==1000:
            count=0
            tes.bulk(body=bulk)
            bulk=""
    if bulk:
        tes.bulk(body=bulk)
