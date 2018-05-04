import json
import sys
from datetime import datetime
from dateutil import parser
from jsonld2compactjsonldldj import process
from requests import get,head
import os
import shutil
from gzip import decompress
import subprocess
import argparse

import elasticsearch
from multiprocessing import Pool,current_process
from pyld import jsonld

import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout


class GNDTask(BaseTask):
    """
    Just a base class for GND
    """
    TAG = 'gnd'

    config={
    #    "url":"https://data.dnb.de/Adressdatei.jsonld.gz",
        "url":"https://data.dnb.de/GND.jsonld.gz",
        "context":"https://raw.githubusercontent.com/hbz/lobid-gnd/master/conf/context.jsonld",
        "username":"opendata",
        "password":"opendata",
        "file":"data",
        "host":"localhost",
        "index":"gnd",
        "port":9200,
        "workers":8,
        "doc_types":["bnodes","records"]
        }

    def closest(self):
        return daily(date=self.date)

class GNDDownload(GNDTask):

    def run(self):
        cmdstring="wget --user {username} --password {password} -O - {url} | gunzip -c > {file} ".format(**self.config)
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

class CleanWorkspace(GNDTask):
    
    def complete(self):
        if os.path.exists("chunks") and os.listdir("chunks")==[]:
            return True
        else:
            return False
    def run(self):
        if os.path.exists("chunks"):
            shutil.rmtree("chunks")
        os.mkdir("chunks")
        

class GNDcompactedJSONdata(GNDTask):
    
    def requires(self):
        return [GNDDownload(),
                CleanWorkspace()]

    def run(self):
        process(self.config.get("file"),None,self.config.get("context"),"chunks/",True,28)

    def output(self):
        return [luigi.LocalTarget("chunks")]

class GNDconcatChunks(GNDTask):
    def requires(self):
        return [ GNDcompactedJSONdata()]
    
    def run(self):
        directory="chunks/"
        records=open("records.ldj","w")
        bnodes=open("bnodes.ldj","w")
        for f in os.listdir(directory):
            if "bnode" in f:
                with open(directory + f,"r") as chunk:
                    for line in chunk:
                        print(line,file=bnodes)
            else:
                with open(directory + f,"r") as chunk:
                    for line in chunk:
                        print(line,file=records)
        records.close()
        bnodes.close()
        
    def output(self):
        return [luigi.LocalTarget("bnodes.ldj"),luigi.LocalTarget("records.ldj")]
    
class GNDFillEsIndex(GNDTask):
    """
    Loads processed GND data into a given ElasticSearch index (with help of esbulk)
    """
    date = datetime.today()
    es = None

    files=None
    def requires(self):
        return GNDconcatChunks()

    def run(self):
        for typ in self.config["doc_types"]:
            cmd="esbulk -verbose -host {host} -port {port} -index {index} -w {workers} -type {type} -id id {type}.ldj""".format(**self.config,type=typ)
            out = shellout(cmd)

    def complete(self):
        self.es=elasticsearch.Elasticsearch([{'host':self.config.get("host")}],port=self.config.get("port"))
        fail=0
        for typ in self.config["doc_types"]:
            cmd="http://{host}:{port}/{index}/{type}/_search?size=0".format(**self.config,type=typ)
            r = get(cmd)
            #result=self.es.search(index=self.config["index"],doc_type=typ,size=0)
            try:
                with open(str(typ)+".ldj","r") as f:
                    for i,l in enumerate(f):
                        pass
            except FileNotFoundError:
                fail+=1
                i=-100
            i+=1
            if i!=r.json().get("hits").get("total"):
                fail+=1
        if fail==0:
            print(True)
            return True
        else:
            return False


class GNDUpdate(GNDTask, luigi.WrapperTask):

    date =datetime.today()

    def requires(self):
        return [GNDFillEsIndex()]
    
    def run(self):
        pass



if __name__ == "__main__":
    date1 = datetime.today()
    
    GNDDownload().complete()
    #CleanWorkspace().run()
    #GNDcompactedJSONdata().run()
    #GNDFillEsIndex().run()
    
    #GNDFillEsIndex().complete()
    #GNDUpdate().run()
