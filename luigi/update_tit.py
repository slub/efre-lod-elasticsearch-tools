# usage for debug:
# PYTHONPATH="$PYTHONPATH:." luigi --module update_tit LODTITUpdate --local-scheduler

import json
from datetime import datetime, timedelta
from requests import get
from time import sleep
import os
import gzip
import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask
from gluish.utils import shellout
from es2json import put_dict, esidfilegenerator, esgenerator


def get_bzipper():
    """Check whether we can parallize bzip2"""
    from distutils.spawn import find_executable
    return "pbzip2" if find_executable("pbzip2") else "bzip2"


class LODTITTask(BaseTask):
    """
    Just a base class for LOD-FINC
    """

    date = None

    now = None

    date = datetime.today().strftime("%Y%m%d")
    now = (datetime.today() - timedelta(1)
           ).strftime("%Y-%m-%d")+"T23:59:59.999Z"
    with open('lodtit_config.json') as data_file:
        config = json.load(data_file)

    TAG = 'lodtit'
# deprecated


class LODTITSolrHarvesterMakeConfig(LODTITTask):

    def run(self):
        r = get("{host}/date/actual/4".format(**self.config))
        lu = r.json().get("_source").get("date")
        with open(self.now+".conf", "w") as fd:
            print("---\nsolr_endpoint: '{host}'\nsolr_parameters:\n    fq: last_indexed:[{last} TO {now}]\nrows_size: 1000\nchunk_size: 10000\nfullrecord_field: 'fullrecord'\nfullrecord_format: 'marc'\nreplace_method: 'decimal'\noutput_directory: './'\noutput_prefix: 'finc_'\noutput_format: 'marc'\noutput_validation: True".format(
                last=lu, now=self.now, host=self.config.get("url")), file=fd)

    def output(self):
        return luigi.LocalTarget(self.now+".conf")

# deprecated


class LODTITDownload(LODTITTask):
    def requires(self):
        return LODTITSolrHarvesterMakeConfig()

    def run(self):
        cmdstring = "~/solr-harvester.py/solr-harvester.py -conf ./"+self.now+".conf"
        # cmdstring="~/git/bhering/solr_harvester/solr_harvester.php --conf ./"+self.now+".conf"
        shellout(cmdstring)
        return 0

    def output(self):
        return luigi.LocalTarget(self.date)

    def complete(self):
        if os.path.exists("{date}".format(date=self.date)):
            try:
                os.listdir("{date}".format(date=self.date))
                return True
            except:
                return False
        return False


class LODTITDownloadSolrHarvester(LODTITTask):
    def run(self):
        r = get("{host}/date/actual/4".format(**self.config))
        lu = r.json().get("_source").get("date")
        cmdstring = "solrdump -verbose -server {host} -fl 'fullrecord,id,recordtype' -q 'last_indexed:[{last} TO {now}]' | ~/git/efre-lod-elasticsearch-tools/helperscripts/fincsolr2marc.py -valid | {bzip} > {date}.mrc.bz2".format(
            last=lu, now=self.now, host=self.config.get("url"), bzip=get_bzipper(), date=self.date)
        print(cmdstring)
        shellout(cmdstring)

    def output(self):
        return luigi.LocalTarget(self.date+".mrc.bz2")


class LODTITTransform2ldj(LODTITTask):

    def requires(self):
        # return LODTITDownload()
        return LODTITDownloadSolrHarvester()

    def run(self):
        if os.stat("{date}.mrc.bz2".format(date=self.date)).st_size > 0:
            cmdstring = "{bzip} -dc {date}.mrc.bz2 | ~/git/efre-lod-elasticsearch-tools/helperscripts/marc2jsonl.py | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py | gzip >> {date}.ldj.gz".format(
                bzip=get_bzipper(), date=self.date)
            # cmdstring="cat {date}/*.mrc | marc2jsonl | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py >> {date}.ldj".format(date=self.date)
            shellout(cmdstring)
            cmdstring = "zcat {date}.ldj.gz | jq -r '.[\"001\"]' > {date}-ppns.txt".format(
                date=self.date)
            shellout(cmdstring)
        #    with open("{date}-ppns.txt".format(**self.config,date=datetime.today().strftime("%Y%m%d")),"w") as ppns, \
        #         open("{date}.ldj".format(**self.config,date=self.date),"r") as inp:
        #        for rec in inp:
        #            record=json.loads(rec)
        #            if "001" in record:
        #                print(record["001"],file=ppns)
        # shutil.rmtree(str(self.date))
        return 0

    def output(self):
        return luigi.LocalTarget("{date}-ppns.txt".format(date=self.date))

    def complete(self):
        try:
            filesize = os.stat("{date}.mrc.bz2".format(date=self.date)).st_size
        except FileNotFoundError:
            return False
        if os.path.exists("{date}-ppns.txt".format(date=self.date)):
            try:
                os.listdir("{date}".format(date=self.date))
                return False
            except:
                return True
        elif filesize == 0:
            return True
        else:
            return False


class LODTITFillRawdataIndex(LODTITTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """

    def requires(self):
        return LODTITTransform2ldj()

    def run(self):
        # put_dict("{host}/finc-main-{date}".format(**self.config,date=datetime.today().strftime("%Y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
        # put_dict("{host}/finc-main-{date}/_settings".format(**self.config,date=datetime.today().strftime("%Y%m%d")),{"index.mapping.total_fields.limit":20000})

        if os.stat("{date}.mrc.bz2".format(date=self.date)).st_size > 0:
            cmd = "esbulk -z -verbose -server {rawdata_host} -w {workers} -index finc-main-k10plus -type mrc -id 001 {date}.ldj.gz""".format(
                **self.config, date=self.date)
            shellout(cmd)

    def complete(self):
        fail = 0
        es_recordcount = 0
        file_recordcount = 0
        es_ids = set()

        try:
            filesize = os.stat("{date}.mrc.bz2".format(date=self.date)).st_size
        except FileNotFoundError:
            return False
        if filesize > 0:
            try:
                for record in esidfilegenerator(host="{rawdata_host}".format(**self.config).rsplit("/")[2].rsplit(":")[0], port="{rawdata_host}".format(**self.config).rsplit("/")[2].rsplit(":")[1], index="finc-main-k10plus", type="mrc", idfile="{date}-ppns.txt".format(**self.config, date=self.date), source="False"):
                    es_ids.add(record.get("_id"))
                    es_recordcount = len(es_ids)

                with gzip.open("{date}.ldj.gz".format(**self.config, date=self.date), "rt") as fd:
                    ids = set()
                    for line in fd:
                        jline = json.loads(line)
                        ids.add(jline.get("001"))
                file_recordcount = len(ids)
                print(file_recordcount)
                if es_recordcount == file_recordcount and es_recordcount > 0:
                    return True
            except FileNotFoundError:
                if os.path.exists("{date}".format(date=self.date)):
                    try:
                        os.listdir("{date}".format(date=self.date))
                        return False
                    except:
                        return True
                return False
            return False
        else:
            return True


class LODTITProcessFromRdi(LODTITTask):

    def requires(self):
        return LODTITFillRawdataIndex()

    def run(self):
        if os.stat("{date}.mrc.bz2".format(date=self.date)).st_size > 0:
            cmd = ". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -z -server {rawdata_host}/finc-main-k10plus/mrc -idfile {date}-ppns.txt -prefix {date}-data".format(
                **self.config, date=self.date)
            shellout(cmd)
            sleep(5)

    def complete(self):
        try:
            filesize = os.stat("{date}.mrc.bz2".format(date=self.date)).st_size
        except FileNotFoundError:
            return False
        if filesize > 0:
            path = "{date}-data".format(date=self.date)
            try:
                for index in os.listdir(path):
                    for f in os.listdir(path+"/"+index):
                        if not os.path.isfile(path+"/"+index+"/"+f):
                            return True if os.path.exists("{date}".format(date=self.date)) and not os.listdir("{date}".format(date=self.date)) else False
            except FileNotFoundError:
                return True if os.path.exists("{date}".format(date=self.date)) and not os.listdir("{date}".format(date=self.date)) else False
        return True


class LODTITUpdate(LODTITTask):

    def requires(self):
        return LODTITProcessFromRdi()

    def run(self):

        if os.stat("{date}.mrc.bz2".format(date=self.date)).st_size > 0:
            path = "{date}-data".format(date=self.date)
            for index in os.listdir(path):
                for f in os.listdir(path+"/"+index):
                    cmd = "esbulk -z -verbose -server {host} -w {workers} -index {index} -type schemaorg -id identifier {fd}".format(
                        **self.config, index=index, fd=path+"/"+index+"/"+f)
                    shellout(cmd)
        # for f in os.listdir(path+"/resources"):
        #    cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && "
        #    cmd+="~/git/efre-lod-elasticsearch-tools/processing/merge2move.py -server {host} -stdin < {fd} | ".format(**self.config,fd=path+"/resources/"+f)
        #    cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/sameAs2id.py  -searchserver {host} -stdin  | ".format(**self.config,fd=path+"/resources/"+f)
        #    cmd+="esbulk -verbose -server {rawdata_host} -w {workers} -index {index} -type schemaorg -id identifier".format(**self.config,index="resources-fidmove")
        #    output=shellout(cmd)
        put_dict("{host}/date/actual/4".format(**self.config),
                 {"date": str(self.now)})
        with gzip.open("slub_resources_sourceid0.ldj", "wt") as outp:
            for record in esgenerator(host="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[0], port="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[1], index="resources", type="schemaorg", body={"query": {"bool": {"must": [{"match": {"offers.offeredBy.branchCode.keyword": "DE-14"}}, {"match": {"_sourceID.keyword": "0"}}]}}}, headless=True):
                print(json.dumps(record), file=outp)
        # delete("{host}/slub-resources/schemaorg".format(**self.config))
        # put_dict("{host}/slub-resources".format(**self.config),{"mappings":{"schemaorg":{"date_detection":False}}})
        # cmd="esbulk -z -verbose -server {host} -w {workers} -index slub-resources -type schemaorg -id identifier slub_resources_sourceid0.ldj".format(**self.config)
        # output=shellout(cmd)

    def complete(self):
        try:
            lastUpdateRequest = get(
                "{host}/date/actual/4".format(**self.config))
            if lastUpdateRequest.ok:
                lu = lastUpdateRequest.json().get("_source").get("date")
            r = get("{server}/select?fl=fullrecord%2Cid%2Crecordtype&q=last_indexed:[{last}%20TO%20{now}]&wt=json&fl=id".format(
                server=self.config.get("url"), last=lu, now=self.now))
            if r.ok and r.json().get("response") and r.json().get("response").get("numFound") == 0 or (lu == self.now and os.stat("{date}.mrc.bz2".format(date=self.date)).st_size > 0):
                return True
            if os.stat("{date}.mrc.bz2".format(date=self.date)).st_size > 0:
                if lu == self.now:
                    return True
                return True if os.path.exists("{date}".format(date=self.date)) and not os.listdir("{date}".format(date=self.date)) else False
            else:
                return True
        except FileNotFoundError:
            return False
