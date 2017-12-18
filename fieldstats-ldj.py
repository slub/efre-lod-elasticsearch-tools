#!/usr/bin/python3 
# -*- coding: utf-8 -*-
from datetime import datetime
import collections
import json
from pprint import pprint
import numpy as np
import argparse
import sys

stats = dict()

def traverse(dict_or_list, path):
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list,list):
        iterator = enumerate(dict_or_list)
    for k, v in iterator:
        yield path + str([k]), v
        if isinstance(v, (dict, list)):
            for k, v in traverse(v, path + str([k])):
                yield k, v

def removebraces(string):
    if string[-1]==']':
        string=string[:-1]
    if string[0]=='[':
        string=string[1:]
    return string


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='return single field statistics of an line-delimited JSON Input-Stream')
    parser.add_argument('-help',action="store_true",help='print more help')
    parser.add_argument('-marc',action="store_true",help='switch for marc21-data')
    parser.add_argument('-path',type=str,help='which path to examine!')
    args=parser.parse_args()
    if args.help:
        print("fieldstats-ldj.py\n"\
"        -help      print this help\n"\
"        -marc      switch for marc21-data\n"\
"        -path      which JSON Path to examine!\n")
    if '.' in args.path:
        plen = len(args.path.split('.'))
        parr = args.path.split('.')
    else:
        parr = args.path
        plen=0
    for line in sys.stdin:
        jline=json.loads(line)
        if args.marc:
            if parr[0] in jline:
                jline=jline[parr[0]]
                for k,v in traverse(jline,""):
                    try:
                        if isinstance(v,dict):
                            if parr[-1] in v:
                                if(v[parr[-1]]) not in stats:
                                    stats[v[parr[-1]]]=1
                                elif v[parr[-1]] in stats:
                                    stats[v[parr[-1]]]+=1
                    except TypeError:
                        continue
        else: #ok, analyzing some flat schema like lido or finc
            for i in range(0,plen):
                print(i)
                if parr[i] in jline:
                    jline=jline[parr[i]]
            if parr in jline:
                if isinstance(jline[parr],str):
                    if jline[parr] not in stats:
                        stats[jline[parr]]=1
                    elif jline[parr] in stats:
                        stats[jline[parr]]+=1
                elif isinstance(jline[parr],list):
                    for elem in jline[parr]:
                        if elem not in stats:
                            stats[elem]=1
                        elif elem in stats:
                            stats[elem]+=1
                        
    for w in sorted(stats, key=stats.get, reverse=True):
      sys.stdout.write("\""+str(w)+"\";"+str(stats[w])+"\n")
        
        
