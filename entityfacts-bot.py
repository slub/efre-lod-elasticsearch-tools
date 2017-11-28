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
sys.path.append('~/slub-lod-elasticsearch-tools/')
from getindex import esgenerator


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

def process_stuff(jline):
    length=len(jline.items())
    body=entityfacts(jline)
    if (len(body) > length):
        fes=Elasticsearch([{'host':args.host}],port=args.port)  
        fes.index(index=args.index,doc_type=args.type,body=body,id=body["identifier"])
        print("updated: "+str(jline['identifier']))
        return

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich your ElasticSearch Search Index with data from entityfacts!')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    args=parser.parse_args()
    pool = Pool(16)
    pool.map(process_stuff, esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True))
    
