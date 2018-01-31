#!/usr/bin/python3
import sys
import json
from elasticsearch import Elasticsearch
import argparse

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='update raw-data index from sys.stdin')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-prefix',type=str,help='ElasticSearch Search doc_type_prefix to use')
    args=   parser.parse_args()
    es=Elasticsearch(host=args.host)
    
    for line in sys.stdin:
            jline=json.loads(line)
            doc_ts=float(jline["005"][0])
            doc_es=es.get(index=args.index,doc_type=args.type,id=str(args.prefix+jline["001"][0]),_source="005")
            if doc_es["found"]:
                if doc_ts>float(doc_es["_source"]["005"][0]):
                    print("update")
