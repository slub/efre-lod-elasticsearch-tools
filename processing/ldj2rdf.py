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
from multiprocessing import Pool, Manager,current_process,Process,cpu_count

from es2json import eprint
from es2json import esgenerator
from es2json import esfatgenerator
from es2json import litter

global args
global es

listcontexts={"http://schema.org":"http://schema.org/docs/jsonldcontext.json",
              "http://schema.org/":"http://schema.org/docs/jsonldcontext.json"}



def init(l,c,m,i):
    global lock
    global con
    global mp
    global name
    name=str("-".join(["triples",i["host"],i["index"],i["type"],str(current_process().name)]))+".n3"
    mp=m
    con = c
    lock = l
    
def get_bulkrdf(doc):
    text=None
    #print(doc)
    for n,elem in enumerate(doc):
        print(elem)
        if isinstance(elem,dict):
            toRemove=[]
            for key in elem:
                if key.startswith("_") and key!="_source":
                    toRemove.append(key)
            for key in toRemove:
                doc[n].pop(key)
            toRemove.clear()
            doc[n]=elem.pop("_source")
        if "sameAs" in elem:
            if isinstance(elem.get("sameAs"),list):
                for m,elen in elem.get("sameAs"):
                    if not elen.startswith("http"):
                        del doc[n]["sameAs"][m]
            elif isinstance(elem.get("sameAs"),str):
                if not elem.get("sameAs").startswith("http"):
                    doc[n].pop("sameAs")
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
    if doc:
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
                        return
                else:
                    eprint("Error, context unknown :( "+str(text),doc)
                    return
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
        return
    
def get_rdf(doc):
    text=""
    if isinstance(doc,dict):
            toRemove=[]
            for key in doc:
                if key.startswith("_") and key!="_source":
                    toRemove.append(key)
            for key in toRemove:
                doc.pop(key)
            toRemove.clear()
    if not text or doc.get("@context")==text:
        text=doc.pop("@context")
    if doc:
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
                        return
                else:
                    eprint("Error, context unknown :( "+str(text),doc)
                    return
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
        return
    
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
        with open(args.inp,"r") as inp:
            m = Manager()
            l = m.Lock()
            c = m.dict()
            i = m.dict({"host":"",
                        "type":"",
                        "index":""})
            pool = Pool(processes=cpu_count()*2,initializer=init,initargs=(l,c,True,i,))
            for line in inp:
                pool.apply_async(get_rdf,args=(json.loads(line),))
        
    elif args.scroll:
        if not args.debug:
            m = Manager()
            l = m.Lock()
            c = m.dict()
            i = m.dict({"host":args.host+":"+str(args.port),
                        "type":args.type,
                        "index":args.index})
            pool = Pool(processes=cpu_count()*2,initializer=init,initargs=(l,c,True,i,))
            for fatload in esfatgenerator(host=args.host,port=args.port,type=args.type,index=args.index,source_exclude="_isil,_recorddate,identifier"):
                pool.apply_async(get_bulkrdf,args=(fatload,))
            pool.close()
            pool.join()
                    
        else:
            global con
            global mp
            mp=False
            global name
            con={}
            for doc in esfatgenerator(host=args.host,port=args.port,type=args.type,index=args.index,source_exclude="_isil,_recorddate,identifier"):
                get_bulkrdf(doc)
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        process_stuff(es.get(index=args.index,doc_type=args.type,id=args.doc))
    else:
        for line in sys.stdin:
            get_rdf(json.loads(line))
        #print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    #out.close()
  
