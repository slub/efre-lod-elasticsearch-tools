#!/usr/bin/python3
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch import exceptions
from elasticsearch import helpers
import argparse
from es2json import eprint

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='update raw-data index from sys.stdin')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-prefix',type=str,default="",help='ElasticSearch Search doc_type_prefix to use')
    args=   parser.parse_args()
    es=Elasticsearch(host=args.host)
    actions=[]
    
    try:
        es.get(index=args.index,doc_type=args.type,id="_mapping")
    except exceptions.NotFoundError as e:
        if 'index_not_found' in str(e):
            eprint("Index not found!")
        elif 'type_missing_exception' in str(e):
            eprint("Type not correct!")
        exit(-1)
    for line in sys.stdin:
            update=None 
            jline=json.loads(line)
            doc_es=None
            doc_ts=float(jline["005"][0])
            try:
                doc_es=es.get(index=args.index,doc_type=args.type,id=str(args.prefix+jline["001"][0]),_source="005")
            except exceptions.NotFoundError as e:
                update="create"
            if doc_es:
                if doc_ts>float(doc_es["_source"]["005"][0]):
                    update="update"
            if update:
                actions.append({
                                '_op_type':update,
                                '_index':args.index,
                                '_type':args.type,
                                '_id':str(args.prefix+jline["001"][0]),
                                'doc': jline
                                })
    elasticsearch.helpers.streaming_bulk(es,actions,chunk_size=500)
            
