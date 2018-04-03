#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import urllib
import sys
import os
import requests
from time import time
from rdflib import ConjunctiveGraph,Graph, URIRef, Namespace, Literal
from rdflib.store import Store
from rdflib.plugin import get as plugin
from pprint import pprint
from elasticsearch import Elasticsearch
from datetime import datetime
from multiprocessing import Pool, Manager,current_process,Process

from es2json import eprint
from es2json import esgenerator
from es2json import esfatgenerator
from es2json import litter

global args
global es

listcontexts={"http://schema.org":"http://schema.org/docs/jsonldcontext.json"}


def run_mp(l,c,m,i,doc):
    init(l,c,m,i)
    get_rdf(doc)

def init(l,c,m,i):
    global lock
    global con
    global mp
    global name
    name=str("-".join(["triples",i["host"],i["index"],i["type"],str(current_process().name)]))+".n3"
    mp=m
    con = c
    lock = l
    
def get_rdf(doc):
    text=None
    for n,elem in enumerate(doc):
        if isinstance(elem,dict):
            toRemove=[]
            for key in elem:
                if key.startswith("_") and key!="_source":
                    toRemove.append(key)
            for key in toRemove:
                doc[n].pop(key)
            toRemove.clear()
            doc[n]=elem.pop("_source")
    for n,elem in enumerate(doc):
        if isinstance(elem,dict):
            toRemoveVal=["http://www.biographien.ac.at"]
            for item in toRemoveVal:
                if "sameAs" in elem:
                    if isinstance(elem["sameAs"],dict):
                        toremove=[]
                        for k,v in elem["sameAs"].items():
                            if item in v:
                                toremove.append(k)
                        for item in toremove:
                            doc[n]["sameAs"].pop(item)
            if not text or elem.get("@context")==text:
                text=doc[n].pop("@context")
    g=ConjunctiveGraph()
    if text not in con:
        if mp:
            lock.acquire()
        if text not in con:
            if text in listcontexts:
                r=requests.get(listcontexts[text])
                if r.ok:
                    con[text]=r.json()
                    eprint("got context from "+listcontexts[text])
                else:
                    eprint("Error, could not get context from "+text)
                    quit(-1)
            else:
                eprint("Error, context unknown :( "+str(text))
                quit(-1)
        if mp:
            lock.release()
    if not args.debug:
        with open(name,"a") as fd:
            g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
            fd.write(str(g.serialize(format='nt').decode('utf-8').rstrip()))
            fd.write("\n")
            fd.flush()
    else:
        g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
        sys.stdout.write(str(g.serialize(format='nt').decode('utf-8').rstrip()))
        sys.stdout.write("\n")
        sys.stdout.flush()
        
            
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='ElasticSearch/ld-json to RDF/Virtuoso')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-debug',action="store_true",help='debug')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-doc',type=str,help='id of the document to serialize to RDF')
    parser.add_argument('-scroll',action="store_true",help="print out the whole index as RDF instead getting a single doc")
    parser.add_argument('-inp',type=str,help="generate RDF out of LDJ")
    args=   parser.parse_args()
    #con =    Isql('DSN=Local Virtuoso;UID=dba;PWD=dba')
    if args.inp:
        inp=open(args.inp,"r")
        for line in inp:
            get_rdf(json.loads(line),False)
        inp.close()
        
    elif args.scroll:
        if not args.debug:
            m = Manager()
            l = m.Lock()
            c = m.dict()
            i = m.dict({"host":args.host+":"+str(args.port),
                        "type":args.type,
                        "index":args.index})
            for fatload in esfatgenerator(host=args.host,port=args.port,type=args.type,index=args.index,source_exclude="_isil,_recorddate,identifier"):
                Process(target=run_mp,args=(l,c,True,i,fatload)).start()
        else:
            global con
            global mp
            mp=False
            global name
            con={}
            for doc in esfatgenerator(host=args.host,port=args.port,type=args.type,index=args.index,source_exclude="_isil,_recorddate,identifier"):
                get_rdf(doc)
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        process_stuff(es.get(index=args.index,doc_type=args.type,id=args.doc))
    else:
        for line in sys.stdin:
            get_rdf(json.loads(line),False)
        #print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    #out.close()
  
