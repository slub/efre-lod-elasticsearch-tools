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


def travpath(dol, path):
    if path and path[0]=="*":
        if isinstance(dol,dict):
            for k,v in dol.items():
                for i in travpath(v,path[1:]):
                    yield i
        elif isinstance(dol,list):
            for elem in dol:
                for i in travpath(elem,path[:]):
                    yield i
    if len(path)==1:
        if isinstance(dol,dict):
            if path[0] in dol:
                yield dol[path[0]]
        elif isinstance(dol,list):
            for elem in dol:
                if path[0] in elem:
                    yield elem[path[0]]
    elif isinstance(dol,dict):
        if path[0] in dol:
            for i in travpath(dol[path[0]],path[1:]):
                yield i
    elif isinstance(dol,list):
        for elem in dol:
            if path[0] in elem:
                for i in travpath(elem[path[0]],path[1:]):
                    yield i


def removebraces(string):
    if string[-1]==']':
        string=string[:-1]
    if string[0]=='[':
        string=string[1:]
    return string


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='return single field statistics of an line-delimited JSON Input-Stream.\nOutput is a 2 coloumn CSV-Sheet.\nNavigate into nested fields via dots (.) wildcard operator is: *..')
    parser.add_argument('-help',action="store_true",help='print more help')
    parser.add_argument('-path',type=str,help='which path to examine!')
    args=parser.parse_args()
    if args.help:
        print("fieldstats-ldj.py\n"\
"        -help      print this help\n"\
"        -path      which JSON Path to examine!\n")
    parr = args.path.split('.')
    for line in sys.stdin:
        jline=json.loads(line)
        for v in travpath(jline,parr):
            if isinstance(v,str):
                if v not in stats:
                    stats[v]=1
                elif v in stats:
                    stats[v]+=1
            elif isinstance(v,list):
                for elem in v:
                    if elem not in stats:
                        stats[elem]=1
                    elif elem in stats:
                        stats[elem]+=1
    for w in sorted(stats, key=stats.get, reverse=True):
      sys.stdout.write("\""+str(w)+"\";"+str(stats[w])+"\n")
        
        
