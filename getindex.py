#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
import json
from pprint import pprint
import argparse
import sys
from es2json import esgenerator


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='simple ES.Getter!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-body',type=str,help='Searchbody')
    args=parser.parse_args()
    
    for json_record in esgenerator(args.host,args.port,args.index,args.type,args.body,headless=True):
        sys.stdout.write(json.dumps(json_record)+"\n")
