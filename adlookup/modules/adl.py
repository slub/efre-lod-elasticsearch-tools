#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import json
import urllib3.request

def getDataByID(typ,num):
    config=json.load(open('/etc/adlookup.json'))
    if "http" in num:
        uri=num #shortcut
    elif typ in config["types"]:
        uri=config["types"][typ]+num
    else:
        uri=None
    
    if uri:
        for elastic in config["indices"]:
            http = urllib3.PoolManager()
            url="http://"+elastic["host"]+":"+str(elastic["port"])+"/"+elastic["index"]+"/"+elastic["type"]+"/_search?q=sameAs:\""+uri+"\""
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
