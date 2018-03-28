#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
import json
from pprint import pprint
from elasticsearch import Elasticsearch
import argparse
import sys

class simplebar():
    count=0
    def __init__(self):
        self.count=0
        
    def reset(self):
        self.count=0
        
    def update(self,num=None):
        if num:
            self.count+=num
        else:
            self.count+=1
        sys.stderr.write(str(self.count)+"\n"+"\033[F")
        sys.stderr.flush()
        
def ArrayOrSingleValue(array):
    if array:
        length=len(array)
        if length>1 or isinstance(array,dict):
            return array
        elif length==1:
            for elem in array:
                 return elem
        elif length==0:
            return None
        
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)   
    
def esgenerator(host=None,port=9200,index=None,type=None,body=None,source=True,headless=False):
    if not source:
        source=True
    es=Elasticsearch([{'host':host}],port=port)
    try:
        page = es.search(
            index = index,
            doc_type = type,
            scroll = '2m',
            size = 1000,
            body = body,
            _source=source)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("not found: "+host+":"+port+"/"+index+"/"+type+"/_search\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    for hits in page['hits']['hits']:
        if headless:
            yield hits['_source']
        else:
            yield hits
    while (scroll_size > 0):
        pages = es.scroll(scroll_id = sid, scroll='2d')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        for hits in pages['hits']['hits']:
            if headless:
                yield hits['_source']
            else:
                yield hits

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='simple ES.Getter!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-source',type=str,help='just return this field(s)')
    parser.add_argument('-body',type=str,help='Searchbody')
    args=parser.parse_args()
    
    for json_record in esgenerator(args.host,args.port,args.index,args.type,args.body,args.source,headless=True):
        sys.stdout.write(json.dumps(json_record)+"\n")
