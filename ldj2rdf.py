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
from multiprocessing import Pool, Manager
from functools import partial

sys.path.append('~/slub-lod-elasticsearch-tools/')
from es2json import eprint
from es2json import esgenerator

global args
global es
global out

def init(l):
    global lock
    lock = l

def wrap_mp(l,doc):
    get_rdf(doc,True)


def get_rdf(doc,mp):
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
    if "identifier" in ldj:
        ldj.pop("identifier")
    if "@context" not in ldj:
        ldj["@context"]="http://schema.org"
    #try:
    try:
        g = Graph().parse(data=json.dumps(ldj), format='json-ld')
        triple=str(g.serialize(format='nt').decode('utf-8').rstrip())
    except:
        eprint("couldn't serialize json! "+str(ldj))
        return
    if mp:
        lock.acquire()
    print(triple)
    if mp:
        lock.release()
    #except:
        #eprint(ldj)
        
            
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
        m = Manager()
        l = m.Lock()
        pool = Pool(initializer=init,initargs=(l,))
        func = partial(wrap_mp,l)
        pool.map(func,inp)
        pool.close()
        pool.join()
        inp.close()
    elif args.scroll:
        if not args.debug:
            m = Manager()
            l = m.Lock()
            pool = Pool(128,initializer=init,initargs=(l,))
            func = partial(wrap_mp,l)
            pool.map(func, esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True))
            pool.close()
            pool.join()
            
        else:
            for doc in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True):
                wrap_mp(doc,True)
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        process_stuff(es.get(index=args.index,doc_type=args.type,id=args.doc))
    else:
        print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    out.close()
  
