#!/usr/bin/python3 
# -*- coding: utf-8 -*-
import json
import argparse
import sys
from es2json import eprint

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

def addtostats(obj,stats):
    if isinstance(obj,str):
        if obj not in stats:
            stats[obj]=1
        elif obj in stats:
            stats[obj]+=1
    elif isinstance(obj,list):
        for elem in obj:
            addtostats(elem,stats)
    elif isinstance(obj,dict):
        for k,v in obj.items():
            addtostats(v,stats)

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='return single field statistics of an line-delimited JSON Input-Stream.\nOutput is a 2 coloumn CSV-Sheet.\nNavigate into nested fields via dots (.) wildcard operator is: *..')
    parser.add_argument('-help',action="store_true",help='print more help')
    parser.add_argument('-path',type=str,help='which path to examine!')
    args=parser.parse_args()
    if args.help:
        print("fieldstats-ldj.py\n"\
"return single field statistics of an line-delimited JSON Input-Stream.\n"\
"Output is a 2 coloumn CSV-Sheet.\nNavigate into nested fields via dots (.) wildcard operator is: *\n\n"\
"        -help      print this help\n"\
"        -path      which JSON Path to examine!\n")
    parr = args.path.split('.')
    stats={}
    for line in sys.stdin:
        jline=json.loads(line)
        for v in travpath(jline,parr):
            addtostats(v,stats)
    for w in sorted(stats, key=stats.get, reverse=True):
      sys.stdout.write("\""+str(w)+"\";"+str(stats[w])+"\n")
