#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import urllib
import sys
import os
import requests
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
    context_included=False
    try:
        global text
        text=""
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
                if (not text or elem.get("@context")==text ) and elem.get("@context") and isinstance(elem.get("@context"),str):
                    text=doc[n].pop("@context")
                else:
                    context_included=True
                if isinstance(elem.get("about"),list):
                    for m,rvkl in enumerate(elem.get("about")):
                        if isinstance(rvkl.get("identifier"),dict) and rvkl.get("identifier").get("propertyID") and rvkl.get("identifier").get("propertyID")=="RVK":
                            doc[n]["about"][m]["@id"]=doc[n]["about"][m]["@id"].replace(" ","+")
                    #    elif isinstance(rvkl.get("identifier"),list):
                    #        for l,ids in enumerate(rvkl.get("identifier")):
                    #            if ids.get("propertyID") and ids.get("propertyID")=="RVK":
                    #                doc[n]["about"][m]
                elif isinstance(elem.get("about"),dict):
                    if elem.get("about").get("identifier") and elem.get("about").get("identifier").get("propertyID") and elem.get("about").get("identifier").get("propertyID")=="RVK":
                        doc[n]["about"]["@id"]=doc[n]["about"]["@id"].replace(" ","+")
                            
        if doc:
            g=ConjunctiveGraph()
            if text not in con:
                if mp:
                    lock.acquire()
                if not context_included:
                    get_context(con,text)
                if mp:
                    lock.release()
            if not args.debug:
                with open(name,"a") as fd:
                    if context_included:
                        g.parse(data=json.dumps(doc), format='json-ld')
                    else:
                        g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
                    fd.write(str(g.serialize(format='nt').decode('utf-8').rstrip()))
                    fd.write("\n")
                    fd.flush()
            else:
                if context_included:
                    g.parse(data=json.dumps(doc), format='json-ld')
                else:
                    g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
                sys.stdout.write(str(g.serialize(format='nt').decode('utf-8').rstrip()))
                sys.stdout.write("\n")
                sys.stdout.flush()
            return
    except Exception as e:
        with open("errors.txt",'a') as f:
            traceback.print_exc(file=f)
    
def get_rdf(doc):
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
        g=ConjunctiveGraph()
        if text not in con and not context_included:
            if mp:
                lock.acquire()
            get_context()
            if mp:
                lock.release()
        if not args.debug:
            with open(name,"a") as fd:
                if context_included:
                    g.parse(data=json.dumps(doc), format='json-ld')
                else:
                    g.parse(data=json.dumps(doc), format='json-ld',context=con[text])
                fd.write(str(g.serialize(format='nt').decode('utf-8').rstrip()))
                fd.write("\n")
                fd.flush()
        else:
            if context_included:
                g.parse(data=json.dumps(doc), format='json-ld')
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
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-doc',type=str,help='id of the document to serialize to RDF')
    parser.add_argument('-scroll',action="store_true",help="print out the whole index as RDF instead getting a single doc")
    parser.add_argument('-inp',type=str,help="generate RDF out of LDJ")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id")
    parser.add_argument('-context',type=str,help="deliver a url to the context if there is no @context field in the data")
    args=parser.parse_args()
    
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
    if args.help:
        parser.print_help(sys.stderr)
        exit()        
        
    elif args.inp:
        if not args.debug:
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
        else:
            m = Manager()
            l = m.Lock()
            c = m.dict()
            i = m.dict({"host":"",
                        "type":"",
                        "index":""})
            init(l,c,m,i)
            with open(args.inp,"r") as inp:
                for line in inp:
                    get_rdf(json.loads(line))
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
        record=es.get(index=args.index,doc_type=args.type,id=args.doc,_source_exclude="_isil,_recorddate,identifier")
        m = Manager()
        l = m.Lock()
        c = m.dict()
        i = m.dict({"host":args.host,
                    "type":args.type,
                    "index":args.index})
        init(l,c,True,i,)
        record["_source"]["@id"]="http://d-nb.info/gnd/"+record["_source"].pop("id")
        get_rdf(record.get("_source"))
    else:
        m = Manager()
        l = m.Lock()
        c = m.dict()
        i = m.dict({"host":"",
                    "type":"",
                    "index":""})
        pool = Pool(processes=cpu_count()*2,initializer=init,initargs=(l,c,True,i,))
        for line in sys.stdin:
            pool.apply_async(get_rdf,args=(json.loads(line),))
        #print("neither given the -scroll optarg or given a -doc id or even an -inp file. nothing to do her. exiting")
    #out.close()
  
