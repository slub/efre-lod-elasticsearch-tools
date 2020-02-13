# usage for debug:
# PYTHONPATH="$PYTHONPATH:." luigi --module update_lod LODUpdate --local-scheduler

import json
from datetime import date, timedelta
from requests import get
from time import sleep
import os
import gzip

import elasticsearch
from es2json import put_dict, esidfilegenerator

import luigi
from gluish.task import BaseTask
from gluish.utils import shellout


class LODTask(BaseTask):
    """
    Just a base class for LOD
    -initializes the Base for all Luigi Task
    -loads configuration
    -gets the timestamp from {host}/date/actual/1
    -calculates which days are missing and should be updated from the SWB FTP Server
    """
    with open('lod_config.json') as data_file:
        config = json.load(data_file)
    r = get("{host}/date/actual/1".format(**config))
    lu = r.json().get("_source").get("date")
    config["lastupdate"] = date(
        int(lu.split('-')[0]), int(lu.split('-')[1]), int(lu.split('-')[2]))
    PPNs = []
    TAG = 'lod'
    yesterday = date.today() - timedelta(1)
    span = yesterday-config.get("lastupdate")
    for i in range(span.days+1):
        date = (config.get("lastupdate")+timedelta(days=i)).strftime("%y%m%d")
        if os.path.isfile("TA-MARC-norm-{date}.tar.gz".format(date=date)):
            continue
        config["dates"].append(date)

    def closest(self):
        return daily(date=self.date)


class LODDownload(LODTask):

    def run(self):
        """
        iterates over the in LODTask defined days (in self.config["dates"]) and downloads those day-deltas from the SWB FTP
        """
        for n, dat in enumerate(self.config.get("dates")):
            cmdstring = "wget --user {username} --password {password} {url}TA-MARC-norm-{date}.tar.gz".format(
                **self.config, date=dat)
            try:
                shellout(cmdstring)
            except Exception as e:
                continue
        return 0

    def complete(self):
        """
        checks if the download is complete
        """
        for n, day in enumerate(self.config.get("dates")):
            if os.path.exists("TA-MARC-norm-{date}.tar.gz".format(**self.config, date=day)):
                return True
        return False


class LODExtract(LODTask):

    def requires(self):
        """
        requires LODDownload
        """
        return LODDownload()

    def run(self):
        """
        iterates over the in LODTask defined days (in self.config["dates"]) and extract those day-deltas
        concatenates the extracted data and gzips them to {yesterday}-norm.mrc.gz
        """
        for day in self.config.get("dates"):
            if os.path.exists("TA-MARC-norm-{date}.tar.gz".format(**self.config, date=day)):
                cmdstring = "tar xvzf TA-MARC-norm-{date}.tar.gz && gzip < norm-aut.mrc >> {yesterday}-norm.mrc.gz && rm norm-*.mrc".format(
                    **self.config, date=day, yesterday=self.yesterday.strftime("%y%m%d"))
                shellout(cmdstring)
        return 0

    def output(self):
        return luigi.LocalTarget("{date}-norm.mrc.gz".format(date=self.yesterday.strftime("%y%m%d")))


class LODTransform2ldj(LODTask):

    def requires(self):
        """
        requires LODExtract
        """
        return LODExtract()

    def run(self):
        """
        extracts the {yesterday}-norm.mrc.gz, transforms it to line-delimited-json
        fixes the MARC PPN  from e.g. 001: ["123ImAMarcID"] to 001:"123ImAMarcID"
        gzips that stuff to {date}-norm-aut.ldj.gz
        also prints out every MARC PPN from that file to an TXT file for further reference
        """
        cmdstring = "zcat {date}-norm.mrc.gz | ~/git/efre-lod-elasticsearch-tools/helperscripts/marc2jsonl.py  | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py | gzip > {date}-norm-aut.ldj.gz".format(
            **self.config, date=self.yesterday.strftime("%y%m%d"))
        shellout(cmdstring)
        with open("{date}-norm-aut-ppns.txt".format(**self.config, date=self.yesterday.strftime("%y%m%d")), "w") as outp, gzip.open("{date}-norm-aut.ldj.gz".format(**self.config, date=self.yesterday.strftime("%y%m%d")), "rt") as inp:
            for rec in inp:
                print(json.loads(rec).get("001"), file=outp)
        return 0

    def output(self):
        return luigi.LocalTarget("{date}-norm-aut.ldj.gz".format(**self.config, date=self.yesterday.strftime("%y%m%d")))


class LODFillRawdataIndex(LODTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """

    def requires(self):
        """
        requires LODTransform2ldj
        """
        return LODTransform2ldj()

    def run(self):
        """
        uploads the {date}-norm-aut.ldj.gz to an elasticsearch node via esbulk
        """
        
        # put_dict("{host}/swb-aut".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
        # put_dict("{host}/swb-aut/_settings".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"index.mapping.total_fields.limit":5000})

        cmd = "esbulk -z -verbose -server {host} -w {workers} -index swb-aut -type mrc -id 001 {date}-norm-aut.ldj.gz""".format(
            **self.config, date=self.yesterday.strftime("%y%m%d"))
        shellout(cmd)

    def complete(self):
        """
        takes all the IDS from the in LODTransform2ldj created TXT file and checks whether those are in the elasticsearch node  
        """
        es_recordcount = 0
        file_recordcount = 0
        es_ids = set()
        for record in esidfilegenerator(host="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[0], port="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[1], index="swb-aut", type="mrc", idfile="{date}-norm-aut-ppns.txt".format(**self.config, date=self.yesterday.strftime("%y%m%d")), source="False"):
            es_ids.add(record.get("_id"))
        es_recordcount = len(es_ids)

        try:
            with gzip.open("{date}-norm-aut.ldj.gz".format(**self.config, date=self.yesterday.strftime("%y%m%d")), "rt") as fd:
                ids = set()
                for line in fd:
                    jline = json.loads(line)
                    ids.add(jline.get("001"))
            file_recordcount = len(ids)
            print(file_recordcount)
        except FileNotFoundError:
            return False

        if es_recordcount == file_recordcount and es_recordcount > 0:
            return True
        return False


class LODProcessFromRdi(LODTask):
    def requires(self):
        """
        requires LODFillRawdataIndex
        """
        return LODFillRawdataIndex()

    def run(self):
        """
        takes the in LODTransform2ldj created TXT file and gets those records from the elasticsearch node
        transforms them to JSON-Linked Data with esmarc.py, gzips the files
        """
        cmd = ". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -z -server {host}/swb-aut/mrc -idfile {date}-norm-aut-ppns.txt -prefix {date}-aut-data".format(
            **self.config, date=self.yesterday.strftime("%y%m%d"))
        shellout(cmd)
        sleep(5)

    def complete(self):
        """
        checks whether all the files are there
        complete test for consistency in LODUpdate.complete()
        """
        path = "{date}-aut-data".format(date=self.yesterday.strftime("%y%m%d"))
        try:
            for index in os.listdir(path):
                for f in os.listdir(path+"/"+index):
                    if not os.path.isfile(path+"/"+index+"/"+f):
                        return False
        except FileNotFoundError:
            return False
        return True


class LODUpdate(LODTask):
    def requires(self):
        """
        requires LODProcessFromRdi
        """
        return LODProcessFromRdi()

    def run(self):
        """
        itereates over the in LODProcessFromRdi generated JSON-Linked-Data
        enriches them with identifier from entityfacts
        enriches them with subjects from the GND
        enriches them with identifier from wikidata
        enriches them with identifier from geonames, if its a geographic Place
        ingests them into a elasticsearch node
        """
        path = "{date}-aut-data".format(date=self.yesterday.strftime("%y%m%d"))
        for index in os.listdir(path):
            # doing several enrichment things before indexing the data
            for f in os.listdir(path+"/"+index):
                cmd = "zcat {fd} | ".format(fd=path+"/"+index+"/"+f)  # with -pipeline, all the data get's thru, not only enriched docs
                cmd += "entityfacts.py   -pipeline -stdin -searchserver {host}/ef/gnd/ | ".format(**self.config)
                cmd += "gnd_sachgruppen.py   -pipeline -stdin -searchserver {host} | ".format(**self.config)
                cmd += "wikidata.py          -pipeline -stdin | "
                if index == "geo":
                    cmd += "geonames.py       -pipeline -stdin -searchserver {geonames_host} | ".format(**self.config)
                cmd += "esbulk -verbose -server {host} -w 1 -size 20 -index {index} -type schemaorg -id identifier".format(**self.config, index=index)
                shellout(cmd)
        put_dict("{host}/date/actual/1".format(**self.config),
                 {"date": str(self.yesterday.strftime("%Y-%m-%d"))})

    def complete(self):
        """
        compares the count of entity-types in the source-data elasticsearch and the target-data elasticsearch
        """
        es = elasticsearch.Elasticsearch([{"host": self.config.get("host").split(
            ":")[1][2:]}], port=int(self.config.get("host").split(":")[2]))
        yesterday = date.today() - timedelta(1)
        now = yesterday.strftime("%Y-%m-%d")
        r = es.get("date", "actual", "1")
        lu = r.get("_source").get("date")
        if not lu == now:
            return False
        map_entities = {
            "p": "persons",  # Personen, individualisiert
            "n": "persons",  # Personen, namen, nicht individualisiert
            "s": "topics",  # Schlagwörter/Berufe
            "b": "organizations",  # Organisationen
            "g": "geo",  # Geographika
            "u": "works",  # Werktiteldaten
            "f": "events"
        }
        """
        TODO: 
        change to one single search. query body = {
                                                    "aggs" : {
                                                            "genres" : {
                                                                    "terms" : { "field" : "079.__.b.keyword" } 
                                                                        }
                                                            }
                                                    }
        """
        person_count_raw = 0
        person_count_map = 0
        for k, v in map_entities.items():
            result_raw = es.count(index="swb-aut", doc_type="mrc",
                                  body={"query": {"match": {"079.__.b.keyword": k}}})
            result_map = es.count(index=v, doc_type="schemaorg")
            if v == "persons":
                person_count_raw += result_raw.get("count")
                person_count_map = result_map.get("count")
            else:
                if result_raw.get("count") != result_map.get("count") or result_map.get("count") == 0:
                    return False
        if person_count_raw == 0 or person_count_raw != person_count_map:
            return False
        return True
