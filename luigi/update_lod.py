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
    TAG = 'gnd'
    yesterday = date.today() - timedelta(1)
    config={
        "date":int(yesterday.strftime("%y%m%d")),
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
        cmdstring="wget --user {username} --password {password} {url}TA-MARC-norm-{date}.tar.gz".format(**self.config)
        output = shellout(cmdstring)
        return 0

    def output(self):
        return luigi.LocalTarget("TA-MARC-norm-{date}.tar.gz".format(**self.config))

class LODExtract(LODTask):
    
    def requires(self):
        return LODDownload()
    
    def run(self):
        cmdstring="tar xvzf TA-MARC-norm-{date}.tar.gz".format(**self.config)
        output = shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("norm-aut.mrc")

class LODTransform2ldj(LODTask):
    
    def _requires(self):
        return LODExtract()

    def run(self):
        cmdstring="marc2jsonl < norm-aut.mrc | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py > norm-aut.ldj"
        output=shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("norm-aut.ldj")


class LODFillRawdataIndex(LODTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """
    date = datetime.today()
    es = None

    files=None
    def requires(self):
        return LODTransform2ldj()
    def run(self):
        put_dict("{host}/swb-aut-{date}".format(**self.config),{"mappings":{"mrc":{"date_detection":False}}})
        put_dict("{host}/swb-aut-{date}/_settings".format(**self.config),{"index.mapping.total_fields.limit":5000})
        
        cmd="esbulk -verbose -server {host} -w {workers} -index swb-aut-{date} -type mrc -id 001 norm-aut.ldj""".format(**self.config)
        output=shellout(cmd)

    def complete(self):
        fail=0
        cmd="{host}/swb-aut-{date}/mrc/_search?size=0".format(**self.config)
        i=0
        r = get(cmd)
        try:
            with open("norm-aut.ldj","r") as f:
                for i,l in enumerate(f):
                    pass
        except FileNotFoundError:
            fail+=1
            i=-100
        i+=1
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
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -server {host}/swb-aut-{date}/mrc".format(**self.config)
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
    
    def output(self):
        return luigi.LocalTarget(path=self.path())


class LODUpdate(LODTask, luigi.WrapperTask):

    def requires(self):
        return [LODFillLODIndex()]
    
    def run(self):
        pass

