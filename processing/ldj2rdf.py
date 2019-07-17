#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import urllib
import sys
import os
import requests
import bz2
import traceback
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
from es2json import isint
from es2json import litter

global args
global es

listcontexts={"http://schema.org":"http://schema.org/docs/jsonldcontext.json",
              "http://schema.org/":"http://schema.org/docs/jsonldcontext.json",
              "http://lobid.org/gnd/context.jsonld":"http://lobid.org/gnd/context.jsonld"}

def get_context(con_dict,con_url):
    if con_url not in con_dict:
        if con_url in listcontexts:
            r=requests.get(listcontexts[con_url])
            if r.ok:
                con_dict[text]=r.json()
                eprint("got context from "+listcontexts[con_url])
            else:
                eprint("Error, could not get context from "+con_url)
                exit(-1)
        else:
            r=requests.get(con_url)
            if r.ok:
                con_dict[text]=r.json()
                eprint("got context from "+con_url)
                return
            eprint("Error, context unknown :( "+str(con_url),doc)
            exit(-1)

def init(l,c,m,i,f):
    global lock
    global con
    global mp
    global name
    global format
    if len(i["host"])<0:
        name=str("-".join(["triples",i["host"],i["index"],i["type"],str(current_process().name)]))+".n3"
    else:
        name=str(current_process().name)+".n3"
    if i["compression"]:
        name+=".bz2"
    mp=m
    con = c
    lock = l
    format = f
    
def adjust_or_get_context_elasticsearchScroll(doc):
    context_included=False
    global text
    text=""
    try:
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
            if "sameAs" in elem:
                if isinstance(elem.get("sameAs"),str):
                    elem["sameAs"]=[elem.get("sameAs")]
                if isinstance(elem.get("sameAs"),list):
                    for m,elen in elem.get("sameAs"):
                        if not elen.startswith("http"):
                            del doc[n]["sameAs"][m]
        for n,elem in enumerate(doc):
            if isinstance(elem,dict):
                toRemoveVal=["http://www.biographien.ac.at"]
                for item in toRemoveVal:
                    if isinstance(elem["sameAs"],dict):
                        toremove=[]
                        for k,v in elem["sameAs"].items():
                            if item in v:
                                toremove.append(k)
                        for item in toremove:
                            doc[n]["sameAs"].pop(item)
                if (not text or elem.get("@context")==text ) and elem.get("@context") and isinstance(elem.get("@context"),str):
                    text=doc[n].pop("@context")
                else:
                    context_included=True
                if isinstance(elem.get("about"),dict):
                    elem["about"]=[elem.get("about")]
                if isinstance(elem.get("about"),list):
                    for m,rvkl in enumerate(elem.get("about")):
                        if isinstance(rvkl.get("identifier"),dict) and rvkl.get("identifier").get("propertyID") and rvkl.get("identifier").get("propertyID")=="RVK":
                            doc[n]["about"][m]["@id"]=doc[n]["about"][m]["@id"].replace(" ","+")
        if doc:
            transpose_to_rdf(doc,con,text,context_included,name,format)
    except Exception as e:
        with open("errors.txt",'a') as f:
            traceback.print_exc(file=f)

def adjust_or_get_context_singledoc(doc):
    try:
        global text
        text=""
        context_included=False
        if isinstance(doc,dict):
                toRemove=[]
                for key in doc:
                    if key.startswith("_") and key!="_source":
                        toRemove.append(key)
                for key in toRemove:
                    doc.pop(key)
                toRemove.clear()
        if (not text or doc.get("@context")==text ) and doc.get("@context") and isinstance(doc.get("@context"),str):
            text=doc.pop("@context")
        elif isinstance(doc.get("@context"),dict):
            context_included=True
        if doc:
            transpose_to_rdf(doc,con,text,context_included,name,format)
    except Exception as e:
        with open("errors.txt",'a') as f:
            traceback.print_exc(file=f)
            
def transpose_to_rdf(doc,con,text,context_included,name,f):
    g=ConjunctiveGraph()
    if text not in con:
        if not context_included:
            if mp:
                with lock:
                    get_context(con,text)
            else:
                get_context(con,text)
    if not args.debug:
        opener=open
        if ".bz" in name:
            opener=bz2.open
        if context_included:
            g.parse(data=json.dumps(doc), format='json-ld')
        else:
            g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
        with opener(name,"at") as fd:
            print(str(g.serialize(format='nt').decode('utf-8').rstrip()),file=fd)
    else:
        if context_included:
            g.parse(data=json.dumps(doc), format='json-ld')
        else:
            g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
        print(str(g.serialize(format=f).decode('utf-8').rstrip()))

if  __name__ == "__main__":
    parser=argparse.ArgumentParser(description='ElasticSearch/ld-json-stdin to ntriple-RDF')
    parser.add_argument('-host',type=str,default="localhost",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-debug',action="store_true",help='debug')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-w',type=int,default=8,help='How many Workers to use!')
    parser.add_argument('-index',default="resources",type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-type',default="schemaorg",type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-doc',type=str,help='id of the document to serialize to RDF')
    parser.add_argument('-scroll',action="store_true",help="print out the whole index as RDF instead getting a single doc")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id")
    parser.add_argument('-context',type=str,help="deliver a url to the context if there is no @context field in the data")
    parser.add_argument('-compress',action="store_true",help="use this flag to enable bzip2 compression on the fly")
    parser.add_argument('-format',type=str,default="nt",help="rdflib format to use for output. default=nt (N-Triple), allowed values: nquads,turtle,application/rdf+xml")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.scroll=True
            args.type=slashsplit[4]
        if len(slashsplit)>5 and slashsplit[5]:
            args.scroll=False
            if "?pretty" in args.server:
                args.doc=slashsplit[5].rsplit("?")[0]
            else:
                args.doc=slashsplit[5]
                
    m = Manager()
    l = m.Lock()
    c = m.dict()
    i = m.dict({"host":args.host+":"+str(args.port),
                "type":args.type,
                "index":args.index,
                "compression":args.compress})
    f = args.format
    if not args.doc or not args.debug:
        pool = Pool(processes=args.w,initializer=init,initargs=(l,c,True,i,f,))
    if args.scroll and not args.debug:
        for fatload in esfatgenerator(host=args.host,port=args.port,type=args.type,index=args.index,source_exclude="_isil,_recorddate,identifier"):
            pool.apply_async(adjust_or_get_context_elasticsearchScroll,args=(fatload,))
    elif args.scroll and args.debug:
        init(l,c,True,i,f,)
        for fatload in esfatgenerator(host=args.host,port=args.port,type=args.type,index=args.index,source_exclude="_isil,_recorddate,identifier"):
            adjust_or_get_context_elasticsearchScroll(fatload)
    elif args.doc:
        es=Elasticsearch([{'host':args.host}],port=args.port)  
        record=es.get(index=args.index,doc_type=args.type,id=args.doc,_source_exclude="_isil,_recorddate,identifier")
        init(l,c,True,i,f,)
        record["_source"]["@id"]="http://d-nb.info/gnd/"+record["_source"].pop("id")
        adjust_or_get_context_singledoc(record.get("_source"))
    elif args.debug:
        init(l,c,True,i,f,)
        for line in sys.stdin:
            adjust_or_get_context_singledoc(json.loads(line))
    else:
        init(l,c,True,i,f,)
        for line in sys.stdin:
            pool.apply_async(adjust_or_get_context_singledoc,args=(json.loads(line),))
    if not args.doc or not args.debug:
        pool.close() 
        pool.join()
