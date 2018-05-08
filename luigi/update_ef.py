import json
import sys
from datetime import datetime
from dateutil import parser
from requests import get,head
import os
import shutil
from gzip import decompress
import subprocess
import argparse

import elasticsearch
from multiprocessing import Pool,current_process
from pyld import jsonld
import ijson.backends.yajl2_cffi as ijson


import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask
from gluish.utils import shellout

class EFTask(BaseTask):
    """
    Just a base class for EF
    """
    TAG = 'gnd'

    config={
    #    "url":"https://data.dnb.de/Adressdatei.jsonld.gz",
        "url":"https://data.dnb.de/opendata/20180312-EFDump-de-DE.json.gz",
        "username":"opendata",
        "password":"opendata",
        "file":"ef-dump.ldj",
        "fixfile":"ef-dump-fixed.ldj",
        "host":"localhost",
        "index":"ef",
        "type":"gnd",
        "port":9200,
        "workers":8,
        }

    def closest(self):
        return daily(date=self.date)

class EFDownload(EFTask):

    #downloads, uncompresses and transforms the record. from this special DNB flavoured json to line-delimited json.
    #e.g.
    #[{<record/>}
    #,{<record/>}
    #]
    #if you delete the first character on the line by cut -c2-, you already got line-delimited json.
    def run(self):
        cmdstring="wget --user {username} --password {password} -O - {url} | gunzip -c | cut -c2- > {file} ".format(**self.config)
        output = shellout(cmdstring)
        return 0

    def complete(self):
        r=head(self.config["url"],auth=(self.config["username"],self.config["username"]))
        remote=None
        if r.headers["Last-Modified"]:
            datetime_object=parser.parse(r.headers["Last-Modified"])
            remote=float(datetime_object.timestamp())
        if os.path.isfile(self.config["file"]):
            statbuf=os.stat(self.config["file"])
            here=float(statbuf.st_mtime)
        else:
            return False
        if here>remote:
            return True
        else:
            return False

    def output(self):
        return luigi.LocalTarget(self.config.get("file"))

class EFFixIDs(EFTask):
    def requires(self):
        return EFDownload()
    
    def run(self):
        with open(self.config.get("fixfile"),"r") as f:
            with open(self.config.get("fixfile"),"w") as out:
                for line in f:
                    try:
                        record=json.loads(line)
                        record["@id"]=record["@id"].split("http://d-nb.info/gnd/")[1] #make "d-nb.info/gnd/081547-11" to "081547-11"
                    except: #bad json or missing "@id"
                        continue
                    print(json.dumps(record,indent=None),file=out)

    def output(self):
        return luigi.LocalTarget(self.config.get("fixfile"))

class EFFillEsIndex(EFTask):
    """
    Loads processed EF data into a given ElasticSearch index (with help of esbulk)
    """
    date = datetime.today()
    es = None

    files=None
    def requires(self):
        return EFFixIDs()

    def run(self):
        cmd="esbulk -verbose -host {host} -port {port} -index {index} -w {workers} -type {type} -id id {fixfile}""".format(**self.config)
        out = shellout(cmd)
        pass

    def complete(self):
        self.es=elasticsearch.Elasticsearch([{'host':self.config.get("host")}],port=self.config.get("port"))
        fail=0
        cmd="http://{host}:{port}/{index}/{type}/_search?size=0".format(**self.config)
        fail=0
        uniq=set()
        r = get(cmd)
        #result=self.es.search(index=self.config["index"],doc_type=typ,size=0)
        if os.path.exists(self.config["file"]):
            with open(self.config["file"],"r") as f:
                for line in f:
                    try:
                        record=json.loads(line)
                        uniq.add(record["@id"])
                    except:
                        continue
            if len(uniq)==r.json().get("hits").get("total"):
                    return True
        return False


class EFUpdate(EFTask, luigi.WrapperTask):
    def requires(self):
        return [EFFillEsIndex()]
    
    def run(self):
        pass
