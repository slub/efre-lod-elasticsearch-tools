# usage for debug:
# PYTHONPATH="$PYTHONPATH:." luigi --module update_lod LODUpdate --local-scheduler

import json
import sys
from datetime import datetime,date,timedelta
from dateutil import parser
from requests import get,head,put
from time import sleep
import os
import shutil
from httplib2 import Http
from gzip import decompress
import subprocess
import argparse

import elasticsearch
from multiprocessing import Pool,current_process
from pyld import jsonld
import ijson.backends.yajl2_cffi as ijson

import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout

def put_dict(url, dictionary):
    '''
    Pass the whole dictionary as a json body to the url.
    Make sure to use a new Http object each time for thread safety.
    '''
    http_obj = Http()
    resp, content = http_obj.request(
        uri=url,
        method='PUT',
        headers={'Content-Type': 'application/json'},
        body=json.dumps(dictionary),
    )

class LODTask(BaseTask):
    """
    Just a base class for LOD
    """
    with open('lod_config.json') as data_file:    
        config = json.load(data_file)
    r=get("{host}/date/actual/1".format(**config))
    lu=r.json().get("_source").get("date")
    config["lastupdate"]=date(int(lu.split('-')[0]),int(lu.split('-')[1]),int(lu.split('-')[2]))
    PPNs=[]
    TAG = 'lod'
    yesterday = date.today() - timedelta(1)
    span = yesterday-config.get("lastupdate")
    for i in range(span.days+1):
        date=(config.get("lastupdate")+timedelta(days=i)).strftime("%y%m%d")
        if os.path.isfile("TA-MARC-norm-{date}.tar.gz".format(date=date)):
            continue
        config["dates"].append(date)
        
    def closest(self):
        return daily(date=self.date)

class LODDownload(LODTask):

    def run(self):
        for n,dat in enumerate(self.config.get("dates")):
            cmdstring="wget --user {username} --password {password} {url}TA-MARC-norm-{date}.tar.gz".format(**self.config,date=dat)
            output = shellout(cmdstring)
        return 0

    def output(self):
        ret=[]
        for n,date in enumerate(self.config.get("dates")):
            ret.append(luigi.LocalTarget("TA-MARC-norm-{date}.tar.gz".format(**self.config,date=date)))
        return ret

class LODExtract(LODTask):
    
    def requires(self):
        return LODDownload()
    
    def run(self):
        for date in self.config.get("dates"):
            cmdstring="tar xvzf TA-MARC-norm-{date}.tar.gz && cat norm-aut.mrc >> {yesterday}-norm.mrc && rm norm-*.mrc".format(**self.config,date=date,yesterday=self.yesterday.strftime("%y%m%d"))
            output = shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-norm.mrc".format(date=self.yesterday.strftime("%y%m%d")))

class LODTransform2ldj(LODTask):
    
    def requires(self):
        return LODExtract()

    def run(self):
        cmdstring="marc2jsonl < {date}-norm.mrc | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py > {date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d")))


class LODFillRawdataIndex(LODTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """
    
    def requires(self):
        return LODTransform2ldj()
    
    def run(self):
        put_dict("{host}/swb-aut-{date}".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
        put_dict("{host}/swb-aut-{date}/_settings".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"index.mapping.total_fields.limit":5000})
        
        cmd="esbulk -verbose -server {host} -w {workers} -index swb-aut-{date} -type mrc -id 001 {date}-norm-aut.ldj""".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)

    def complete(self):
        fail=0
        cmd="{host}/swb-aut-{date}/mrc/_search?size=0".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        i=0
        r = get(cmd)
        try:
            with open("{date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"r") as fd:
                ids=set()
                for line in fd:
                    jline=json.loads(line)
                    ids.add(jline.get("001"))
            i=len(ids)
        except FileNotFoundError:
            fail+=1
            i=-100
        if r.ok:
            if i!=r.json().get("hits").get("total"):
                fail+=1
        else:
            fail+=1
        if fail==0:
            return True
        else:
            return False

class LODProcessFromRdi(LODTask):
    def requires(self):
        return LODFillRawdataIndex()
    
    def run(self):
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -server {host}/swb-aut-{date}/mrc -prefix {date}-aut-data".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)
        sleep(5)
        
    def complete(self):
        returnarray=[]
        path="{date}-aut-data".format(date=self.yesterday.strftime("%y%m%d"))
        try:
            for index in os.listdir(path):
                for f in os.listdir(path+"/"+index):
                    if not os.path.isfile(path+"/"+index+"/"+f):
                        return False
        except FileNotFoundError:
            return False
        return True
    
class LODUpdate(LODTask):
    def requires(self):
        return LODProcessFromRdi()
    
    def run(self):
        path="{date}-aut-data".format(date=self.yesterday.strftime("%y%m%d"))
        enrichmentstr=[]
        for index in os.listdir(path):
            for f in os.listdir(path+"/"+index):
                cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && "
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/sameAs2id.py         -pipeline -stdin -searchserver {host} | ".format(**self.config)
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/entityfacts-bot.py   -pipeline -stdin -searchserver {host} | ".format(**self.config)
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/gnd-sachgruppen.py   -pipeline -searchserver {host} < {fd} | ".format(**self.config,fd=path+"/"+index+"/"+f)
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/wikidata.py          -pipeline -stdin | "
                if index=="geo":
                    cmd+="~git/efre-lod-elasticsearch-tools/enrichment/geonames.py       -pipeline -stdin -searchserver {geonames_host} | ".format(**self.config)
                cmd+="esbulk -verbose -server {host} -w 1 -size 20 -index {index} -type schemaorg -id identifier".format(**self.config,index=index)
                output=shellout(cmd)
        put_dict("{host}/date/actual/1".format(**self.config),{"date":str(self.yesterday.strftime("%Y-%m-%d"))})
    
    def output(self):
        return luigi.LocalTarget(path=self.path())


    def complete(self):
        yesterday = date.today() - timedelta(1)
        now=yesterday.strftime("%Y-%m-%d")
        r=get("{host}/date/actual/1".format(**self.config))
        lu=r.json().get("_source").get("date")
        if lu==now:
            return True
        else:
            return False
        
