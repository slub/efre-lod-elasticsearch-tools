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

sys.path.append('~/slub-lod-elasticsearch-tools/')
from getindex import esgenerator

global args
global es
global out

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)    


def process_stuff(doc):
    if isinstance(doc,str):
        ldj=json.loads(doc)
    elif isinstance(doc,dict):
        ldj=doc
    #if isinstance(ldj['@id'],list):
        #ppn=ldj['@id'][0]
    #else:
        #ppn=ldj['@id']
    #ldj['@id']="http://data.slub-dresden.de/person/"+str(ppn)
    #add context:
    ldj.pop("identifier")
    if "@context" not in ldj:
        ldj["@context"]="http://schema.org"
    try:
        g = Graph().parse(data=json.dumps(ldj), format='json-ld')
        triple=str(g.serialize(format='nt').decode('utf-8').rstrip())
        print(triple,file=out)
        print(triple)
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
        if not args.debug:
            pool = Pool(256)
            pool.map(process_stuff, esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True))
        else:
            for doc in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True):
                process_stuff()
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        process_stuff(es.get(index=args.index,doc_type=args.type,id=args.doc))
    else:
        print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    out.close()
  
