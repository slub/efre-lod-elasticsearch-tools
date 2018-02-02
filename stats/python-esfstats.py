#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
from datetime import datetime
from elasticsearch import Elasticsearch
import collections
import json
from pprint import pprint
import argparse
import sys

stats = dict()


def traverse(dict_or_list, path=[]):
    if isinstance(dict_or_list, dict):
        if "properties" in dict_or_list:
            dict_or_list=dict_or_list["properties"]
        iterator = dict_or_list.iteritems()
    else:
        iterator = enumerate(dict_or_list)
    for k, v in iterator:
        yield path + [k], v
        if isinstance(v, (dict, list)):
            if "fields" not in v and "type" not in v:
                for k, v in traverse(v, path + [k]):
                    yield k, v
        
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='return field statistics of an ElasticSearch Search Index')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-marc',action="store_true",help='Ignore Marc Indicator')
    parser.add_argument('-delimiter',type=str,help='delimiter to use')
    args=parser.parse_args()
    if args.host is None:
        args.host='localhost'
    if args.port is None:
        args.port=9200
    if args.delimiter is None:
        delim='|'
        
    es=Elasticsearch([{'host':args.host}],port=args.port)  
    mapping = es.indices.get_mapping(index=args.index,doc_type=args.type)[args.index]["mappings"][args.type]
    for path, node in traverse(mapping):
        fullpath=str()
        for field in path:
            fullpath=fullpath+"."+field
        fullpath=fullpath[1:]
        if args.marc==True:
            fullpath=fullpath[:3]+".*."+fullpath[-1:]
        page = es.search(
            index = args.index,
            doc_type = args.type,
            body = {"query":{"bool":{"must":[{"exists": {"field": fullpath}}]}}},
            size=0
            )
        stats[fullpath]=page['hits']['total']
    hitcount=es.search(
            index = args.index,
            doc_type = args.type,
            body = {},
            size=0
            )['hits']['total']
    print('{:11s}|{:3s}|{:11s}|{:40s}'.format("existing","%","notexisting","field name"))
    print("-----------|---|-----------|----------------------------------------")
    sortedstats=collections.OrderedDict(sorted(stats.items()))
    for key, value in sortedstats.iteritems():
        print('{:>11s}|{:>3s}|{:>11s}| {:40s}'.format(str(value),str(int((float(value)/float(hitcount))*100)),str(hitcount-int(value)),str(key).replace("."," > ")))
