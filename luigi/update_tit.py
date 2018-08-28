import json
import sys
from datetime import datetime,date,timedelta
from dateutil import parser
from requests import get,head,put
from time import sleep
import os
from httplib2 import Http
import subprocess
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

class LODTITTask(BaseTask):
    """
    Just a base class for LOD-FINC
    """
    
    with open('lodtit_config.json') as data_file:    
        config = json.load(data_file)
    TAG = 'lodtit'
    yesterday = date.today() - timedelta(1)
    now=yesterday.strftime("%Y-%m-%d")+"T23:59:59.999Z"
    #
    
class LODTITSolrHarvesterMakeConfig(LODTITTask):
    def run(self):
        r=get("{host}/date/actual/2".format(**self.config))
        lu=r.json().get("_source").get("date")
        with open(self.now+".conf","w") as fd:
            print("---\nsolr_endpoint: '{host}'\nsolr_parameters:\n    fq: last_indexed:[{last} TO {now}]\nrows_size: 100\nchunk_size: 1000\nfullrecord_field: 'fullrecord'\nfullrecord_format: 'marc'\nreplace_method: 'decimal'\noutput_directory: './'\noutput_prefix: 'finc_'\noutput_format: 'marc'".format(last=lu,now=self.now,host=self.config.get("url")),file=fd)
    
    def output(self):
        return luigi.LocalTarget(self.now+".conf")

class LODTITDownload(LODTITTask):
    def requires(self):
        return LODTITSolrHarvesterMakeConfig()
    
    def run(self):
        cmdstring="~/git/solr_harvester-master/solr_harvester.php --conf ./"+self.now+".conf"
        output = shellout(cmdstring)
        return 0

    def output(self):
        ret=[]
        return luigi.LocalTarget(datetime.today().strftime("%Y%m%d"))

class LODTITTransform2ldj(LODTITTask):
    
    def requires(self):
        return LODTITDownload()

    def run(self):
        cmdstring="cat {name}/*.mrc | marc2jsonl | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py >> {date}.ldj".format(name=datetime.today().strftime("%Y%m%d"),date=datetime.today().strftime("%Y%m%d"))
        output=shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}.ldj".format(date=datetime.today().strftime("%Y%m%d")))


class LODTITFillRawdataIndex(LODTITTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """
    
    def requires(self):
        return LODTITTransform2ldj()
    
    def run(self):
        put_dict("{host}/finc-main-{date}".format(**self.config,date=datetime.today().strftime("%Y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
        put_dict("{host}/finc-main-{date}/_settings".format(**self.config,date=datetime.today().strftime("%Y%m%d")),{"index.mapping.total_fields.limit":5000})
        
        cmd="esbulk -verbose -server {host} -w {workers} -index finc-main-{date} -type mrc -id 001 {date}.ldj""".format(**self.config,date=datetime.today().strftime("%Y%m%d"))
        output=shellout(cmd)

    def complete(self):
        fail=0
        cmd="{host}/finc-main-{date}/mrc/_search?size=0".format(**self.config,date=datetime.today().strftime("%Y%m%d"))
        i=0
        r = get(cmd)
        try:
            with open("{date}.ldj".format(**self.config,date=datetime.today().strftime("%Y%m%d")),"r") as fd:
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

class LODTITProcessFromRdi(LODTITTask):
    def requires(self):
        return LODTITFillRawdataIndex()
    
    def run(self):
        cmd="rm -rf ldj/ && . ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -server {host}/finc-main-{date}/mrc".format(**self.config,date=datetime.today().strftime("%Y%m%d"))
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
    
class LODTITFillLODIndex(LODTITTask):
    def requires(self):
        return LODTITProcessFromRdi()
    
    def run(self):
        enrichmentstr=[]
        for index in os.listdir("ldj"):
            for f in os.listdir("ldj/"+index):
                cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && pv {fd} | esbulk -verbose -server {host} -w {workers} -index {index} -type schemaorg -id identifier".format(**self.config,index="resources-finc"+str(datetime.today().strftime("%y%m%d")),fd="ldj/"+index+"/"+f)
                output=shellout(cmd)
                with open("{fd}".format(fd="ldj/"+index+"/"+f)) as fdd:
                    for line in fdd:
                        rec=json.loads(line)
                        enrichmentstr.append((self.config.get("host").split("/")[2].split(":")[0],self.config.get("host").split("/")[2].split(":")[1],index,"schemaorg",rec.get("identifier")))
                        
        yesterday = date.today() - timedelta(1)
        now=yesterday.strftime("%Y-%m-%d")+"T23:59:59.999Z"
        put_dict("{host}/date/actual/2".format(**self.config),{"date":str(now)})
        #for host,port,index,type,id in enrichmentstr:
            #rec=sameAs2id.run(host,port,index,type,id,False)
            #if rec:
                #with open(index+"-toReIndex.ldj","a") as fd:
                    #json.dump(rec,fd,indent=None)
        #for index in os.listdir("ldj"):
            #if os.path.isfile(str(index)+"-toReIndex.ldj"):
                #cmd="esbulk -verbose -server {host} -index {index}-finc -type schemaorg -id identifier -w {workers} < {index}-toReIndex.ldj".format(**self.config,index=index)
                #output=shellout(cmd)
                #os.remove(str(index)+"-toReIndex.ldj")
        luigi.LocalTarget(output).move(self.output().path)
    
    def output(self):
        return luigi.LocalTarget(path=self.path())

class LODTITUpdate(LODTITTask, luigi.WrapperTask):

    def requires(self):
        return [LODTITFillLODIndex()]
    
    def run(self):
        pass
