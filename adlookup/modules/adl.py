#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import json
import urllib3.request

def loadjson(path):
    try:
        config=json.load(open(path))
        return config
    except:
        return None



def getDataByID(typ,num,feld,index):
    uri=None
    path="/etc/adlookup.json"
    config=loadjson(path)
    if not config:
        yield "No config defined in "+str(path)
    elif typ in config["types"]:
        uri=str(config["types"][typ])+str(num)        
    if "http" in num:
        uri=num #shortcut
    if not feld:
        feld="sameAs"
    if uri and config:
        for elastic in config["indices"]:
            if not index.startswith("Search") and index not in elastic["index"]:
                continue
            http = urllib3.PoolManager()
            url="http://"+elastic["host"]+":"+str(elastic["port"])+"/"+elastic["index"]+"/"+elastic["type"]+"/_search?q="+feld+":\""+uri+"\""
            try:
                r=http.request('GET',url)
                data = json.loads(r.data.decode('utf-8'))
                if 'Error' in data or not 'hits' in data:
                    continue
            except:
                continue
            for hit in data["hits"]["hits"]:
                if "_id" in hit:
                    yield hit["_source"]
