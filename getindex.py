#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
from elasticsearch import Elasticsearch
import json
from pprint import pprint
import argparse
import sys

def esgenerator(host,port,index,typ,body):
    es=Elasticsearch([{'host':host}],port=port)  
    page = es.search(
      index = index,
      doc_type = typ,
      scroll = '2m',
      size = 1000,
      body = body,
      _source=True)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    for hits in page['hits']['hits']:
      yield hits['_source']
    while (scroll_size > 0):
      pages = es.scroll(scroll_id = sid, scroll='2m')
      sid = pages['_scroll_id']
      scroll_size = len(pages['hits']['hits'])
      for hits in pages['hits']['hits']:
        yield hits['_source']

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='simple ES.Getter!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-body',type=str,help='Searchbody')
    args=parser.parse_args()
    
    for json_record in esgenerator(args.host,args.port,args.index,args.type,args.body):
        sys.stdout.write(json.dumps(json_record)+"\n")
