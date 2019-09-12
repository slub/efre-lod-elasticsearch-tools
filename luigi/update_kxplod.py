# usage for debug:
# PYTHONPATH="$PYTHONPATH:." luigi --module update_lod LODUpdate --local-scheduler

import json
import sys
from datetime import datetime,date,timedelta
from dateutil import parser
from requests import get,head,put,delete
from time import sleep
import os
import shutil
import gzip
import subprocess
import argparse

import elasticsearch
from multiprocessing import Pool,current_process
from pyld import jsonld
import ijson.backends.yajl2_cffi as ijson
from es2json import put_dict, esidfilegenerator

import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout


class LODKXPTask(BaseTask):
    """
    Just a base class for LOD
    """
    with open('lodkxp_config.json') as data_file:    
        config = json.load(data_file)
    PPNs=[]
    TAG = 'lodkxp'
    yesterday = date.today() - timedelta(1)
    lastupdate = datetime.strptime(config.get("lastupdate"),"%y%m%d")
    span = yesterday-lastupdate.date()
    for i in range(span.days+1):
        date=(lastupdate+timedelta(days=i)).strftime("%y%m%d")
        if os.path.isfile("{path}/TA-MARCVBFL-006-{date}.tar.gz".format(path=config.get("path"),date=date)):
            config["dates"].append(date)
        
    def closest(self):
        return daily(date=self.date)

class LODKXPCopy(LODKXPTask):

    def run(self):
        for n,dat in enumerate(self.config.get("dates")):
            cmdstring="cp {path}/TA-MARCVBFL-006-{date}.tar.gz ./".format(**self.config,date=dat)
            try:
                output = shellout(cmdstring)
            except Exception as e:
                continue
        return 0

    def complete(self):
        for n,date in enumerate(self.config.get("dates")):
            if os.path.exists("TA-MARCVBFL-006-{date}.tar.gz".format(**self.config,date=date)):
                return True
        return False

class LODKXPExtract(LODKXPTask):
    
    def requires(self):
        return LODKXPCopy()
    
    def run(self):
        for date in self.config.get("dates"):
            if os.path.exists("TA-MARCVBFL-006-{date}.tar.gz".format(**self.config,date=date)):
                cmdstring="tar xvzf TA-MARCVBFL-006-{date}.tar.gz && gzip < 006-tit.mrc >> {yesterday}-tit.mrc.gz  && gzip < 006-lok.mrc >> {yesterday}-lok.mrc.gz && rm 006-tit.mrc 006-lok.mrc".format(**self.config,date=date,yesterday=self.yesterday.strftime("%y%m%d"))
                output = shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{yesterday}-tit.mrc.gz".format(yesterday=self.yesterday.strftime("%y%m%d")))

class LODKXPTransform2ldj(LODKXPTask):
    
    def requires(self):
        return LODKXPExtract()

    def run(self):
        for typ in ["tit","lok"]:
            cmdstring="zcat {date}-{typ}.mrc.gz | ~/git/efre-lod-elasticsearch-tools/helperscripts/marc2jsonl.py  | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py | gzip > {date}-{typ}.ldj.gz".format(**self.config,typ=typ,date=self.yesterday.strftime("%y%m%d"))
            output=shellout(cmdstring)
        with open("{date}-lok-ppns.txt".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"w") as outp,gzip.open("{date}-lok.ldj.gz".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"rt") as inp:
            for rec in inp:
                print(json.loads(rec).get("001"),file=outp)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-lok-ppns.txt".format(**self.config,date=self.yesterday.strftime("%y%m%d")))


class LODKXPFillRawdataIndex(LODKXPTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """
    
    def requires(self):
        return LODKXPTransform2ldj()
    
    def run(self):
        for typ in ["tit","lok"]:
            put_dict("{rawdata_host}/kxp-{typ}-{date}".format(**self.config,typ=typ,date=self.yesterday.strftime("%y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
            put_dict("{rawdata_host}/kxp-{typ}-{date}/_settings".format(**self.config,typ=typ,date=self.yesterday.strftime("%y%m%d")),{"index.mapping.total_fields.limit":5000})
            cmd="esbulk -z -verbose -server {rawdata_host} -w {workers} -index kxp-{typ}-{date} -type mrc -id 001 {date}-{typ}.ldj.gz""".format(**self.config,typ=typ,date=self.yesterday.strftime("%y%m%d"))
            output=shellout(cmd)

    def complete(self):
        fail=0
        es_recordcount=0
        file_recordcount=0
        es_ids=set()
        for record in esidfilegenerator(host="{rawdata_host}".format(**self.config).rsplit("/")[-1].rsplit(":")[0],
                                        port="{rawdata_host}".format(**self.config).rsplit("/")[-1].rsplit(":")[1],
                                        index="kxp-lok-{date}".format(date=self.yesterday.strftime("%y%m%d")),
                                        type="mrc",idfile="{date}-lok-ppns.txt".format(**self.config,date=self.yesterday.strftime("%y%m%d")),
                                        source="False"):
            es_ids.add(record.get("_id"))
        es_recordcount=len(es_ids)
        
        try:
            with gzip.open("{date}-lok.ldj.gz".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"rt") as fd:
                ids=set()
                for line in fd:
                    jline=json.loads(line)
                    ids.add(jline.get("001"))
            file_recordcount=len(ids)
            print(file_recordcount)
        except FileNotFoundError:
            return False
        
        if es_recordcount==file_recordcount and es_recordcount>0:
            return True
        return False

class LODKXPMerge(LODKXPTask):
    def requires(self):
        return LODKXPFillRawdataIndex()
    
    def run(self):
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/helperscripts/merge_lok_with_tit.py -title_server {rawdata_host}/kxp-tit-{date}/mrc -local_server {rawdata_host}/kxp-lok-{date}/mrc | tee data.ldj | esbulk -server {rawdata_host} -index kxp-de14 -type mrc -id 001 -w 1 -verbose && jq -rc \'.\"001\"' data.ldj > ids.txt && rm data.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)
    
    def complete(self):
        ids=set()
        es_ids=set()
        with open("ids.txt") as inp:
            for line in inp:
                ids.add(line.strip())
        for record in esidfilegenerator(host="{rawdata_host}".format(**self.config).rsplit("/")[-1].rsplit(":")[0],
                                        port="{rawdata_host}".format(**self.config).rsplit("/")[-1].rsplit(":")[1],
                                        index="kxp-de14",type="mrc",idfile="ids.txt",source=False):
            es_ids.add(record.get("_id"))
        if len(es_ids)==len(ids) and len(es_ids)>0:
            return True
        return False

class LODKXPProcessFromRdi(LODKXPTask):
    def requires(self):
        return LODKXPFillRawdataIndex()
    
    def run(self):
        delete("{rawdata_host}/kxp-tit-{date}".format(**self.config,date=self.yesterday.strftime("%y%m%d")))
        delete("{rawdata_host}/kxp-lok-{date}".format(**self.config,date=self.yesterday.strftime("%y%m%d")))
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -z -server {rawdata_host}/kxp-de14/mrc -idfile ids.txt -prefix {date}-kxp".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)
        sleep(5)
        
    def complete(self):
        returnarray=[]
        path="{date}-kxp".format(date=self.yesterday.strftime("%y%m%d"))
        try:
            for index in os.listdir(path):
                for f in os.listdir(path+"/"+index):
                    if not os.path.isfile(path+"/"+index+"/"+f):
                        return False
        except FileNotFoundError:
            return False
        return True
    
class LODKXPUpdate(LODKXPTask):
    def requires(self):
        return LODKXPProcessFromRdi()
    
    def run(self):
        path="{date}-kxp".format(date=self.yesterday.strftime("%y%m%d"))
        enrichmentstr=[]
        for index in os.listdir(path):
            for f in os.listdir(path+"/"+index):                                        ### doing several enrichment things before indexing the data
                    cmd="esbulk -z -verbose -server {host} -w {workers} -index slub-{index} -type schemaorg -id identifier {fd}".format(**self.config,index=index,fd=path+"/"+index+"/"+f)
                    output=shellout(cmd)
        newconfig=None
        with open('lodkxp_config.json') as data_file:    
            newconfig = json.load(data_file)
        newconfig["lastupdate"]=str(self.yesterday.strftime("%Y%m%d"))
        with open('lodkxp_config.json','w') as data_file:
            json.dump(newconfig,data_file)
    
    def output(self):
        return luigi.LocalTarget(path=self.path())

    def complete(self):
        yesterday = date.today() - timedelta(1)
        now=yesterday.strftime("%Y-%m-%d")
        with open('lodkxp_config.json') as inp:
            lu=json.load(inp).get("lastupdate")
        if not lu==now:
            return False
        map_entities={
            "p":"persons",      #Personen, individualisiert
            "n":"persons",      #Personen, namen, nicht individualisiert
            "s":"topics",        #Schlagwörter/Berufe
            "b":"organizations",         #Organisationen
            "g":"geo",          #Geographika
            "u":"works",     #Werktiteldaten
            "f":"events"
            }   

        person_count_raw=0
        person_count_map=0
        for k,v in map_entities.items():
            result_raw=es.count(index="kxp-de14",doc_type="mrc",body={"query":{"match":{"079.__.b.keyword":k}}})
            result_map=es.count(index=v,doc_type="schemaorg")
            if v=="persons":
                person_count_raw+=result_raw.get("count")
                person_count_map=result_map.get("count")
            else:
                if result_raw.get("count") != result_map.get("count") or result_map.get("count")==0:
                    return False
        if person_count_raw == 0 or person_count_raw != person_count_map:
            return False
        return True
