#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
import elasticsearch
from elasticsearch import Elasticsearch
import json
from pprint import pprint
import argparse
import sys
import pickle
import os.path
import signal
import urllib3.request
from multiprocessing import Pool


es=None
args=None

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)   
    
schema2entity = {
    "forename":"givenName",
    "surname":"familyName",
    "professionorOccupation":"hasOccupation",
    "dateOfDeath":"deathDate",
    "dateOfBirth":"birthDate",
    "placeOfBirth":"birthPlace",
    "sameAs":"sameAs"
}
    
def entityfacts(record):
    changed=False
    if 'sameAs' in record:
        if "http://d-nb.info/gnd/" in record['sameAs']:
            http = urllib3.PoolManager()
            url="http://hub.culturegraph.org/entityfacts/"+str(record['sameAs'].split('/')[-1])
            r=http.request('GET',url)
            try:
                data = json.loads(r.data.decode('utf-8'))
            except:
                print(url)
                return [0,0]
            for k,v in schema2entity.items():
                    if k=="sameAs":
                        if isinstance(record['sameAs'],str):
                            dnb=record['sameAs']
                            record['sameAs']=[dnb]
                        if k in data:
                            if v not in record:
                                record[v]=data[k]["@id"]
                                changed=True
                            else:
                                for sameAs in data[k]:
                                    record[v].append(sameAs["@id"])
                                    changed=True
                    if k in data and v not in record:
                        record[v]=data[k]
                        changed=True
                    elif k in data and v in record and k!="sameAs":
                        if k=="dateOfDeath" or k=="dateOfBirth":
                            if data[k]!=record[v] and record[v] and data[k]:
                                if record[v] in data[k]:
                                    record[v]=data[k]
                                elif k in record:
                                    print("Error! "+record["@id"]+" birthDate in SWB differs from entityFacts! SWB: "+record["birthDate"]+" EF: "+data["dateOfBirth"])
    if changed:
        return record
    else:
        return [0,0]

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
            
def process_stuff(jline):
    length=len(jline['_source'].items())
    if "_source" in jline:
        body=entityfacts(jline['_source'])
        if (len(body) > length):
            fes=Elasticsearch([{'host':args.host}],port=args.port)  
            fes.index(index=args.index,doc_type=args.type,body=body,id=body["identifier"])
            print("updated: "+str(jline['_id']))
            return

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich your ElasticSearch Search Index with data from entityfacts!')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-pickle',type=str,help="use pickle for store/retrieve")
    args=parser.parse_args()
    es=Elasticsearch([{'host':args.host}],port=args.port)  
    
    #for page in esgenerator():
    #    process_stuff(page)
    pool = Pool(16)
    pool.map(process_stuff, esgenerator())
    
