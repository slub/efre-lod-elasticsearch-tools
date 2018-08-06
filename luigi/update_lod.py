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
    Just a base class for GND
    """
    PPNs=[]
    TAG = 'lod'
    yesterday = date.today() - timedelta(1)
    config={
        "dates":[],
        "url":"ftp://vftp.bsz-bw.de/006/lod/",
        "username":"nix",
        "password":"da",
        "host":"http://127.0.0.1:9200",
        "workers":8
        }

    def closest(self):
        return daily(date=self.date)

class LODDownload(LODTask):

    def run(self):
        r=get("{host}/date/actual/1".format(**self.config))
        lu=r.json().get("_source").get("date")
        lastupdate=date(int(lu.split('-')[0]),int(lu.split('-')[1]),int(lu.split('-')[2]))
        span=self.yesterday-lastupdate
        for i in range(span.days+1):
            self.config["dates"].append((lastupdate+timedelta(days=i)).strftime("%y%m%d"))
        
        for dat in self.config.get("dates"):
            cmdstring="wget --user {username} --password {password} {url}TA-MARC-norm-{date}.tar.gz".format(**self.config,date=dat)
            output = shellout(cmdstring)
        return 0

    def output(self):
        ret=[]
        for date in self.config.get("dates"):
            ret.append(luigi.LocalTarget("TA-MARC-norm-{date}.tar.gz".format(**self.config,date=date)))
        return ret

class LODExtract(LODTask):
    
    def requires(self):
        return LODDownload()
    
    def run(self):
        for date in self.config.get("dates"):
            cmdstring="tar xvzf TA-MARC-norm-{date}.tar.gz && cat norm-aut.mrc >> norm.mrc && rm norm-*.mrc".format(**self.config,date=date)
            output = shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("norm.mrc")

class LODTransform2ldj(LODTask):
    
    def requires(self):
        return LODExtract()

    def run(self):
        cmdstring="marc2jsonl < norm.mrc | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py > norm-aut.ldj".format(**self.config)
        output=shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("norm-aut.ldj".format(**self.config))


class LODFillRawdataIndex(LODTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """
    
    def requires(self):
        return LODTransform2ldj()
    
    def run(self):
        put_dict("{host}/swb-aut-{date}".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
        put_dict("{host}/swb-aut-{date}/_settings".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"index.mapping.total_fields.limit":5000})
        
        cmd="esbulk -verbose -server {host} -w {workers} -index swb-aut-{date} -type mrc -id 001 norm-aut.ldj""".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)

    def complete(self):
        fail=0
        cmd="{host}/swb-aut-{date}/mrc/_search?size=0".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        i=0
        r = get(cmd)
        try:
            with open("norm-aut.ldj".format(**self.config),"r") as fd:
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
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -server {host}/swb-aut-{date}/mrc".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)
        sleep(5)
        
    def complete(self):
        returnarray=[]
        try:
            for index in os.listdir("ldj"):
                for f in os.listdir("ldj/"+index):
                    if not os.path.isfile("ldj/"+index+"/"+f):
                        return False
        except FileNotFoundError:
            return False
        return True
    
class LODFillLODIndex(LODTask):
    def requires(self):
        return LODProcessFromRdi()
    
    def run(self):
        for index in os.listdir("ldj"):
            for f in os.listdir("ldj/"+index):
                cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/enrichment/gnd-sachgruppen.py < {fd} | esbulk -verbose -server {host} -w {workers} -index {index} -type schemaorg -id identifier".format(**self.config,index=index,fd="ldj/"+index+"/"+f)
                output=shellout(cmd)
                luigi.LocalTarget(output).move(self.output().path)
        put_dict("{host}/date/actual/1".format(**self.config),{"date":str(self.yesterday.strftime("%Y-%m-%d"))})
    
    def output(self):
        return luigi.LocalTarget(path=self.path())


class LODUpdate(LODTask, luigi.WrapperTask):

    def requires(self):
        return [LODFillLODIndex()]
    
    def run(self):
        pass

