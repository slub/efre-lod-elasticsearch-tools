
from requests import head,get,delete
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout
from es2json import put_dict
import luigi
import gzip
import os
from dateutil import parser

class GNTask(BaseTask):
    TAG = 'gn'

    config={
        "url":"http://download.geonames.org/export/dump/",
        "file":"allCountries",
        "server":"http://194.95.145.24:9201",
        "index":"geonames",
        "type":"record",
        "workers":8
        }

    ldj=0
    txt=0
    
    def closest(self):
        return daily(date=self.date)
    
    
class GNDownload(GNTask):
    def run(self):
        cmdstring="wget {url}{file}.zip -O {file}.zip".format(**self.config)
        output = shellout(cmdstring)
        cmd="unzip -o {file}.zip".format(**self.config)
        output=shellout(cmd)
        with open(self.config.get("file")+".txt") as f:
            l=0
            for i,line in enumerate(f):
                pass
            GNTask.txt=i
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
        if self.txt>0:
            return True
        else:
            return False
    
    def output(self):
        return luigi.LocalTarget(self.config.get("file")+".txt")

class GNtsv2json(GNTask):   # we inherit from GNDownload instead of the super-parent GNTask so we preserve the change of self.txt in the GNDownload class
    def requires(self):
        return [GNDownload()]
    
    def run(self):
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/helperscripts/tsv2json.py {file}.txt | gzip > {file}.ldj.gz".format(**self.config)
        output=shellout(cmd)
    
    def complete(self):
        try:
            i=0
            with gzip.open(self.config.get("file")+".ldj.gz") as f:
                for i,l in enumerate(f):
                    pass
            if i==GNTask.txt:
                GNTask.ldj=i
                return True
        except FileNotFoundError:
                return False
        return False

class GNIngest(GNTask):
    def requires(self):
        return [GNtsv2json()]

    def run(self):
        r=delete("{server}/{index}".format(**self.config))
        put_dict("{server}/{index}".format(**self.config),{"mappings":{"{type}".format(**self.config):{"properties":{"location":{"type":"geo_point"}}}}})
        cmd="esbulk -z -server {server} -index {index} -type {type} -w {workers} -id id -verbose {file}.ldj.gz".format(**self.config)
        output=shellout(cmd)
        
    def complete(self):     
        r = get("{server}/{index}/{type}/_search?size=0".format(**self.config))
        if r.ok:
            if GNTask.ldj==r.json().get("hits").get("total") and GNTask.ldj>0:
                return True
        return False
