
from requests import head,get
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout

import luigi
import os
from dateutil import parser

class GNTask(BaseTask):
    TAG = 'gnd'

    config={
        "url":"http://download.geonames.org/export/dump/",
        "file":"allCountries",
        "server":"http://127.0.0.1:9200",
        "index":"geonames",
        "type":"record",
        "workers":8
        }

    def closest(self):
        return daily(date=self.date)
    ldj=0
    
class GNDownload(GNTask):
    def run(self):
        cmdstring="wget {url}{file}.zip".format(**self.config)
        output = shellout(cmdstring)
        return 0

    def complete(self):
        r=head(str(self.config.get("url")+self.config.get("file")+".zip"))
        remote=None
        if r.headers.get("Last-Modified"):
            datetime_object=parser.parse(r.headers["Last-Modified"])
            remote=float(datetime_object.timestamp())
        if os.path.isfile(self.config.get("file")+".zip"):
            statbuf=os.stat(self.config.get("file")+".zip")
            here=float(statbuf.st_mtime)
        else:
            return False
        if here<remote:
            return False
        return True
    
    def output(self):
        return luigi.LocalTarget(self.config.get("file")+".zip")

class GNUnzip(GNTask):
    def requires(self):
        return [GNDownload()]
    
    def run(self):
        cmd="unzip -o {file}.zip".format(**self.config)
        output=shellout(cmd)
    
    def output(self):
        return luigi.LocalTarget(self.config.get("file")+".txt")
    
class GNtsv2json(GNTask):
    def requires(self):
        return [GNUnzip()]
    
    def run(self):
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/helperscripts/tsv2json.py {file}.txt > {file}.ldj".format(**self.config)
        output=shellout(cmd)
    
    def complete(self):
        try:
            txt=0
            i=0
            with open(self.config.get("file")+".txt") as f:
                for txt,l in enumerate(f):
                    pass
            with open(self.config.get("file")+".ldj") as f:
                for i,l in enumerate(f):
                    pass
            if txt==i:
                self.ldj=i
                return True
        except FileNotFoundError:
                return False
        return False

class ldj2ES(GNTask):
    def requires(self):
        return [GNtsv2json()]

    def run(self):
        cmd="esbulk -server {server} -index {index} -type {type} -w {workers} -id id -verbose {file}.ldj".format(**self.config)
        output=shellout(cmd)
    # TODO: put this into the ES Index so we can search by coordinats and create some fancy stuff in kibana
    ######{
    ######"mappings": {
        ######"record": {
            ######"properties": {
                ######"location": {
                    ######"type": "geo_point"
                ######}
            ######}
        ######}
    ######}
######}
    def complete(self):     
        r = get("{server}/{index}/{type}/_search?size=0".format(**self.config))
        if r.ok:
            if self.ldj==r.json().get("hits").get("total") and self.ldj>0:
                return True
        return False
