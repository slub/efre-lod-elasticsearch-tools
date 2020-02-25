# usage for debug:
# PYTHONPATH="$PYTHONPATH:." luigi --module update_finctit LODTITFillFINCIndex --local-scheduler

import json
from datetime import datetime, timedelta
from requests import get
import os
import gzip
import bz2
import luigi
from finc2rdf import gen_solrdump_cmd
from update_tit import get_bzipper
from gluish.task import BaseTask
from gluish.utils import shellout
from es2json import put_dict, esidfilegenerator


class LODFINCTITTask(BaseTask):
    """
    Just a base class for LOD-FINC
    """
    date = datetime.today().strftime("%Y%m%d")
    now = (datetime.today() - timedelta(1)
           ).strftime("%Y-%m-%d")+"T23:59:59.999Z"
    with open('lodfinctit_config.json') as data_file:
        config = json.load(data_file)
    TAG = 'lodfinctit'


class LODFINCTITDownloadRawData(LODFINCTITTask):
    def run(self):
        """
        downloads the delta from the last update until now
        maps it via finc2rdf.py to RDF
        """
        r = get("{host}/date/actual/5".format(**self.config))
        lu = r.json().get("_source").get("date")
        solrdump_cmd = gen_solrdump_cmd(self.config.get("url"))
        solrdump_cmd += "  -q 'institution:DE-15 last_indexed:[{last} TO {now}]' | ~/git/efre-lod-elasticsearch-tools/processing/finc2rdf.py | {bzip} > {date}-finc.ldj.bz2".format(
            last=lu, now=self.now, host=self.config.get("url"), bzip=get_bzipper(), date=self.date)
        print(solrdump_cmd)
        output = shellout(solrdump_cmd)

    def output(self):
        "returns the mapped file"
        return luigi.LocalTarget(self.date+"-finc.ldj.bz2")


class LODFINCTITAddIDField(LODFINCTITTask):
    def requires(self):
        """
        requires LODFINCTITDownloadRawData
        """
        return LODFINCTITDownloadRawData()

    def run(self):
        """
        the records only contain URIs, for ingest we need an Identifier
        Identifier = URI . split( "/" ) [-1], so to speak: the element behind the last /
        also we need the IDs in an extra File
        """
        with bz2.open(self.date+"-finc.ldj.bz2", "rt") as inp, gzip.open(self.date+"-finc-fixed.ldj.gz", "wt") as out, open(self.date+"-finc-ppns.txt", "wt") as ppns:
            for line in inp:
                record = json.loads(line)
                record["_id"] = record["@id"].split("/")[-1]
                print(json.dumps(record), file=out)
                print(record["@id"].split("/")[-1], file=ppns)

    def complete(self):
        try:
            filesize = os.stat(self.date+"-finc-fixed.ldj.gz").st_size
        except FileNotFoundError:
            return False
        if os.path.exists(self.date+"-finc-ppns.txt"):
            return True
        elif filesize == 0:
            return True
        else:
            return False


class LODTITFillFINCIndex(LODFINCTITTask):
    def requires(self):
        """
        requires LODFINCTITAddIDField
        """
        return LODFINCTITAddIDField()

    def run(self):
        """
        Loads mapped data into a given ElasticSearch index (with help of esbulk)
        """
        if os.stat("{date}-finc-fixed.ldj.gz".format(date=self.date)).st_size > 0:
            cmd = "esbulk -z -verbose -server {host} -w {workers} -index {index} -type {type} -id _id {date}-finc-fixed.ldj.gz""".format(
                **self.config, date=self.date)
            output = shellout(cmd)
            put_dict("{host}/date/actual/5".format(**self.config),
                     {"date": str(self.now)})

    def complete(self):
        """
        checks if the IDs from the file created in LODTITFillFINCIndex are existend in the server
        """
        es_recordcount = 0
        file_recordcount = 0
        es_ids = set()

        try:
            filesize = os.stat(
                "{date}-finc-fixed.ldj.gz".format(date=self.date)).st_size
        except FileNotFoundError:
            return False
        if filesize > 0:
            try:
                for record in esidfilegenerator(host="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[0], port="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[1], index="finc-resources", type="schemaorg", idfile="{date}-finc-ppns.txt".format(**self.config, date=self.date), source="False"):
                    es_ids.add(record.get("_id"))
                    es_recordcount = len(es_ids)

                with open("{date}-finc-ppns.txt".format(**self.config, date=self.date), "rt") as fd:
                    ids = set()
                    for line in fd:
                        ids.add(line)
                file_recordcount = len(ids)
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
