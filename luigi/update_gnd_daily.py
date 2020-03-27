# usage for debug:
# PYTHONPATH="." luigi --module update_gnd_daily LODGNDDailyFillRawDataIndex --local-scheduler

import json
from datetime import datetime, timedelta
import os
import luigi
import gzip
import requests
from gluish.task import BaseTask
from gluish.utils import shellout
from es2json import put_dict
from es2json import esidfilegenerator
from es2json import esgenerator


class LODGNDDaily(BaseTask):
    """
    Just a base class for GND Daily
    """

    with open('lodgnd_daily_conf.json') as data_file:
        config = json.load(data_file)
    TAG = 'gnd_daily'
    data_directory = None

    def get_days(self):
        days = []
        lastupdate = datetime.strptime(self.config["lastupdate"], "%Y-%m-%d")
        date = datetime.today()
        yesterday = date.today() - timedelta(1)
        span = yesterday-lastupdate
        for i in range(span.days+1):
            days.append(lastupdate+timedelta(days=i))
        return days


class LODGNDDailyDownloadRawData(LODGNDDaily):
    def run(self):
        """
        downloads the delta from the last update until now
        maps it via finc2rdf.py to RDF
        """
        metha_cmd = "/usr/sbin/metha-sync -base-dir {base-dir}/oai -format {format} -set {set} -from {lastupdate} -daily -log {log} {access}".format(**self.config)
        metha_dir_cmd = "/usr/sbin/metha-sync -dir -base-dir {base-dir}/oai -format {format} -set {set} -from {lastupdate} -daily -log {log} {access} > dir.txt".format(**self.config)

        shellout(metha_cmd)
        shellout(metha_dir_cmd)
        with open("dir.txt", "r") as inp:
            self.data_directory = inp.read().strip()

    def complete(self):
        if not self.data_directory:
            return False
        for f in os.listdir(self.data_directory):
            if f.startswith(self.config["lastupdate"]):
                return True
        return False


class LODGNDDailyGenerateDailyDeltas(LODGNDDaily):
    def requires(self):
        """
        requires LODGNDDailyDownloadRawData
        """
        return LODGNDDailyDownloadRawData()

    def run(self):
        days = self.get_days()
        for n, day in enumerate(days):
            if n < (len(days)-1):
                fr = days[n].strftime("%Y-%m-%d")
                to = days[n+1].strftime("%Y-%m-%d")
                yyyy = days[n].strftime("%Y")
                targetdate = days[n].strftime("%Y%m%d")
                targetfile = "{base-dir}/{YYYY}/TA-MARC-GND-{targetdate}.mrc.gz".format(**self.config, YYYY=yyyy, targetdate=targetdate)
                metha_cat_cmd = "metha-cat -from {fr} -until {to} -base-dir {base-dir}/oai -format {format} -set {set} {access} | ".format(**self.config, fr=fr, to=to)
                metha_cat_cmd += "xmlcutty -root collection -path /Records/Record/metadata/record | yaz-marcdump -i marcxml -o marc -f UTF-8 - | gzip > {fd}".format(fd=targetfile)
                print("processing {date}".format(date=targetdate))
                shellout(metha_cat_cmd)

    def output(self):
        days = self.get_days()
        retlist = []
        for n, day in enumerate(days):
            if n < (len(days)-1):
                yyyy = days[n].strftime("%Y")
                targetdate = days[n].strftime("%Y%m%d")
                targetfile = "{base-dir}/{YYYY}/TA-MARC-GND-{targetdate}.mrc.gz".format(**self.config, YYYY=yyyy, targetdate=targetdate)
                retlist.append(luigi.LocalTarget(targetfile))
        return retlist


class LODGNDDailyTransform2ldj(LODGNDDaily):
    def requires(self):
        """
        requires LODGNDDailyGenerateDailyDeltas
        """
        return LODGNDDailyGenerateDailyDeltas()

    def run(self):
        days = self.get_days()
        idfile = days[-1].strftime("%Y%m%d")+".ids"
        targetfile = days[-1].strftime("%Y%m%d")+".ldj.gz"
        for n, day in enumerate(days):
            if n < (len(days)-1):
                yyyy = days[n].strftime("%Y")
                targetdate = days[n].strftime("%Y%m%d")
                sourcefile = "{base-dir}/{YYYY}/TA-MARC-GND-{targetdate}.mrc.gz".format(**self.config, YYYY=yyyy, targetdate=targetdate)
                transformation = "zcat {sfd} | ~/git/efre-lod-elasticsearch-tools/helperscripts/marc2jsonl.py | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_gnd_id.py | uconv -x any-nfc | gzip >> {tfd}".format(sfd=sourcefile, tfd=targetfile)
                shellout(transformation)
        id_cmd = "zcat {tfd} | jq -rc _id >> {idf}".format(tfd=targetfile, idf=idfile)
        shellout(id_cmd)

    def output(self):
        days = self.get_days()
        idfile = days[-1].strftime("%Y%m%d")+".ids"
        targetfile = days[-1].strftime("%Y%m%d")+".ldj.gz"
        return [luigi.LocalTarget(idfile), luigi.LocalTarget(targetfile)]


class LODGNDDailyFillRawDataIndex(LODGNDDaily):
    def requires(self):
        """
        requires LODGNDDailyTransform2ldj
        """
        return LODGNDDailyTransform2ldj()

    def run(self):
        days = self.get_days()
        targetfile = days[-1].strftime("%Y%m%d")+".ldj.gz"
        ingest_cmd = "esbulk -server {host} -index {index} -type {type} -id _id -verbose -w 1 -z {fd}".format(**self.config, fd=targetfile)
        shellout(ingest_cmd)
        self.config["lastupdate"] = days[-1].strftime("%Y-%m-%d")
        with open('lodgnd_daily_conf.json', 'w') as data_file:
            print(json.dumps(self.config, indent=4), file=data_file)

    def complete(self):
        days = self.get_days()
        if days[-1].strftime("%Y-%m-%d") == self.config["lastupdate"]:
            return True
        idfile = days[-1].strftime("%Y%m%d")+".ids"
        targetfile = days[-1].strftime("%Y%m%d")+".ldj.gz"
        if not os.path.isfile(idfile):
            return False
        if not os.path.isfile(targetfile):
            return False
        es_ids_ts = set()
        for record in esidfilegenerator(host="{host}".format(**self.config).rsplit("/")[-1].rsplit(":")[0],
                                        port="{host}".format(**self.config).rsplit("/")[-1].rsplit(":")[1],
                                        index="{index}".format(**self.config),
                                        type="{type}".format(**self.config),
                                        idfile=idfile,
                                        source="005"):
            if not record["found"]:
                return False
            es_ids_ts.add(json.dumps({record["_id"]: record["_source"]["005"][0]}))
        with gzip.open(targetfile, "rt") as fd:
            for line in fd:
                rec = json.loads(line)
                if json.dumps({rec["_id"]: rec["005"][0]}) not in es_ids_ts:
                    return False
        return True


class LODGNDDailyDeleteRecords(LODGNDDaily):
    query = {"query": {"bool": {"must": [{"regexp": {"_LEADER.keyword": ".{5}[d].{18}"}}, {"match": {"682.__.i": "Loeschung"}}]}}}

    def requires(self):
        """
        requires LODGNDDailyFillRawDataIndex
        """
        return LODGNDDailyFillRawDataIndex()

    def run(self):
        bulk = ""
        ndjson_header = {"Content-type": "application/x-ndjson"}
        successfull_deletions = set()
        for delPPN in esgenerator(host="{host}".format(**self.config).rsplit("/")[-1].rsplit(":")[0],
                                  port="{host}".format(**self.config).rsplit("/")[-1].rsplit(":")[1],
                                  index="{index}".format(**self.config),
                                  type="{type}".format(**self.config),
                                  source="False",
                                  body=self.query):
            bulk += json.dumps({"delete": {"_index": self.config["index"], "_type": self.config["type"], "_id": delPPN["_id"]}}) + "\n"
        if bulk:
            url = "{host}/{index}/_bulk".format(**self.config)
            response = requests.post(url, data=bulk, headers=ndjson_header)

    def complete(self):
        url = "{host}/{index}/{type}/_search".format(**self.config)
        r = requests.post(url, json=self.query, params={"size":0}, headers={"Content-type": "application/json"})
        return not r.json()["hits"]["total"]
