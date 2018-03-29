#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import urllib
import sys
import os
import requests
import pyodbc
from rdflib import ConjunctiveGraph,Graph, URIRef, Namespace, Literal
from rdflib.store import Store
from rdflib.plugin import get as plugin
from pprint import pprint
from elasticsearch import Elasticsearch
from datetime import datetime
from multiprocessing import Pool, Manager
from functools import partial
from es2json import Daemon
from isql import Isql

from es2json import eprint
from es2json import esgenerator

global args
global es
global out

listcontexts={"http://schema.org":"http://schema.org/docs/jsonldcontext.json"}


def init(l,c,m):
    global lock
    global con
    global mp
    mp=m
    con = c
    lock = l

def get_rdf(doc):
    g=ConjunctiveGraph()
    text = doc.pop("@context")
    if text not in con:
        if mp:
            lock.acquire()
        if text not in con:
            eprint("nope!")
            if text in listcontexts:
                r=requests.get(listcontexts[text])
                if r.ok:
                    con[text]=r.json()
                else:
                    eprint("Error, could not get context from "+text)
                    quit(-1)
            else:
                eprint("Error, context unknown :(")
                quit(-1)
        if mp:
            lock.release()
    if "@id" not in doc and "identifier" in doc:        ##what if a @type got Orga AND Place? #boogus
        doc["@id"]="http://data.slub-dresden.de/"
        if any("Orga" in s for s in doc["@type"]):
            doc["@id"]+="orga/"
        elif any("Place" in s for s in doc["@type"]):
            doc["@id"]+="geo/"
        elif any("Person" in s for s in doc["@type"]):
            doc["@id"]+="persons/"
        doc["@id"]+=doc["identifier"]
    toRemove=["@context","identifier"]
    toRemoveVal=["http://www.biographien.ac.at"]
    for item in toRemoveVal:
        if "sameAs" in doc:
            if isinstance(doc["sameAs"],dict):
                toremove=[]
                for k,v in doc["sameAs"].items():
                    if item in v:
                        toremove.append(k)
                for item in toremove:
                    doc["sameAs"].pop(item)
    for key in doc:
        if key.startswith("_"):
            toRemove.append(key)
    for cul in toRemove:
        if cul in doc:
            doc.pop(cul)
    triple=""
    try:
        g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
        triple=str(g.serialize(format='nt').decode('utf-8').rstrip())
    except:
        eprint(doc)
    triples=[]
    for line in triple.split('\n'):
        triples.append(line)
    if mp:
        lock.acquire()
    for triple in triples:
        sys.stdout.write(str(triple)+"\n")
    if mp:
        lock.release()
    #for s,p,o in g:
    #    print(s,p,o)
            #con.insert_triple
        
    #except:
        #eprint("couldn't serialize json! "+str(ldj))
        #return
            
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
            pool = Pool(initializer=init,initargs=(l,c,True,))
            pool.map(get_rdf, esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True))
            pool.close()
            pool.join()
            
        else:
            global con
            con={}
            for doc in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True):
                get_rdf(doc,False)
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        process_stuff(es.get(index=args.index,doc_type=args.type,id=args.doc))
    else:
        for line in sys.stdin:
            get_rdf(json.loads(line),False)
        #print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    #out.close()
  
