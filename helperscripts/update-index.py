#!/usr/bin/python3
# -*- coding: utf-8 -*-
import elasticsearch
from multiprocessing import Pool
from elasticsearch import Elasticsearch, exceptions, helpers
from es2json import eprint
import io
import os
import json
import signal
from pprint import pprint
import argparse
import sys
import itertools
from gnd2swb import simplebar
from fix_mrc_id import fix_mrc_id

sb=None
args=None
es=None
def merge_lists(a,b):
    c=[]
    if isinstance(a,list):
        for item in itertools.chain(a,b):
            if item not in c:
                c.append(item)
                return b
    else:
        if a not in b:
            b.append(a)
            return b
        else:
            return a

def merge_dicts(a,b):
    change=False
    for k,v in a:
        if k in b:
            if isinstance(b[k],list):
                value=merge_lists(a[k],b[k])
                if value:
                    b[k]=value
                change=True
            elif isinstance(b[k],dict) and isinstance(a[k],dict):
                value=merge_dicts(a[k],b[k])
                if value:
                    b[k]=value
                change=True
            elif isinstance(b[k],str) and isinstance(a[k],str):
                if len(b[k])<len(a[k]):
                    b[k]=a[k]
                    change=True
        else:
                b[k]=a[k]
                change=True
    if change:
        return b
    else:
        return None
    
                    
    
def update_index(jline,args):
    tes=Elasticsearch(host=args.host)
    es_doc=tes.get(index=args.index,doc_type=args.type,id=args.id)
    if "_source" in es_doc:
        body=merge_dicts(jline,es_doc["_source"])
        if body:
            tes.update(index=args.index,doc_type=args.type,id=args.id,body=body)
        else:
            return
        
actions=[]
def update_marc_index(jline):
    global actions
    if isinstance(jline,str):
       jline=json.loads(jline) 
    PPN=None
    ts=None
    jline=fix_mrc_id(jline)
    if "001" in jline:
        PPN=jline["001"]
    if "005" in jline:
        ts=jline["005"][0]
    if PPN and ts:
        try:
            esdata=es.get(index=args.index,id=PPN,doc_type=args.type,_source="005")
            if "_source" in esdata and "005" in esdata["_source"]:
                if float(esdata["_source"]["005"][0])<float(ts):
                    action={'_op_type':"index",
                                    '_index':args.index,
                                    '_type':args.type,
                                    '_id':str(PPN),
                                    '_source':{}}
                    for k in jline:
                        action['_source'][k]=jline[k]
                    actions.append(action)
                    sb.update()
                    actions.append({'_op_type':"create",
                                    '_index': "queue",
                                    '_type':"ppns",
                                    '_id':str(PPN),
                                    '_source': {
                                    'normalized':"false",
                                    'clean_uri':"false",
                                    'enriched':"false"}
                                    })
        except exceptions.NotFoundError:
            #es.index(index=args.index,doc_type=args.type,body=jline,id=str(PPN))
            action={'_op_type':"index",
                   '_index':args.index,
                    '_type':args.type,
                    '_id':str(PPN),
                    '_source':{}}
            for k in jline:
                action['_source'][k]=jline[k]
            actions.append(action)
            actions.append({'_op_type':"create",
                            '_index': "queue",
                            '_type':"ppns",
                            '_id':PPN,
                           '_source': {
                            'normalized':"false",
                            'clean_uri':"false",
                            'enriched':"false"}})
            sb.update()
    if len(actions)>=1000:
        helpers.bulk(es,actions,stats_only=True)
        actions.clear()
        

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='update an index (instead of re-indexing)')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-id',type=str,help='_id field to use to identify the unique documents')
    parser.add_argument('-i',type=str,required=True,help='path to input file')
    args=parser.parse_args()    
    es=Elasticsearch([{'host':args.host}],port=args.port)
    with open(args.i,"r") as fd:
        sb=simplebar()
        for line in fd:
            update_marc_index(line)
        #pool = Pool()
        #pool.map(update_marc_index,fd)
    if len(actions)>0:
        eprint("Lets go")
        helpers.bulk(es,actions,stats_only=True)
        actions.clear()
        
        
    
