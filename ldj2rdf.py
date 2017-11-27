#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import urllib
import sys
import os
from rdflib import ConjunctiveGraph,Graph, URIRef, Namespace, Literal
from pprint import pprint
from elasticsearch import Elasticsearch
from datetime import datetime
from multiprocessing import Pool

global args
global es
global out

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)    
    
def esgenerator():
    try:
        page = es.search(
            index = args.index,
            doc_type = args.type,
            scroll = '14d',
            size = 1000)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("not found: "+args.host+":"+args.port+"/"+args.index+"/"+args.type+"/_search\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    stats = {}
    for hits in page['hits']['hits']:
        yield hits
    while (scroll_size > 0):
        pages = es.scroll(scroll_id = sid, scroll='14d')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        for hits in pages['hits']['hits']:
            yield hits

def process_stuff(doc):
    if isinstance(doc,str):
        ldj=json.loads(doc)
    if '_source' in doc:
        ldj=doc['_source']
    #if isinstance(ldj['@id'],list):
        #ppn=ldj['@id'][0]
    #else:
        #ppn=ldj['@id']
    #ldj['@id']="http://data.slub-dresden.de/person/"+str(ppn)
    #add context:
    ldj.pop("identifier")
    try:
        g = Graph().parse(data=json.dumps(ldj), format='json-ld')
        triple=str(g.serialize(format='nt').decode('utf-8').rstrip())
        print(triple,file=out)
    except:
        eprint(ldj)
        return
            
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='ElasticSearch/ld-json to RDF!!')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-debug',action="store_true",help='debug')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-doc',type=str,help='id of the document to serialize to RDF')
    parser.add_argument('-ent',type=str,default="resources",help='entity type of the doc. possible values: resources (default) persons')
    parser.add_argument('-scroll',action="store_true",help="print out the whole index as RDF instead getting a single doc")
    parser.add_argument('-inp',type=str,help="generate RDF out of LDJ")
    args=parser.parse_args()
    out=open("persons.nt","w",encoding='utf8')
    
    if args.inp:
        inp=open(args.inp,"r")
        pool = Pool(256)
        pool.map(process_stuff,inp)
        inp.close()
    elif args.scroll:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        if not args.debug:
            pool = Pool(256)
            pool.map(process_stuff, esgenerator())
        else:
            for doc in esgenerator():
                process_stuff(doc)
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        process_stuff(es.get(index=args.index,doc_type=args.type,id=args.doc))
    else:
        print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    out.close()
  
