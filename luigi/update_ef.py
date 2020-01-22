import json
from datetime import datetime
from dateutil import parser
from requests import get, head
import os
import gzip
import elasticsearch

import luigi
from gluish.task import BaseTask
from gluish.utils import shellout


class EFTask(BaseTask):
    """
    Just a base class for EF
    """
    TAG = 'gnd'

    with open('ef_config.json') as data_file:
        config = json.load(data_file)


class EFDownload(EFTask):
    """
    downloads, uncompresses and transforms the record.
    from this special DNB flavoured json to line-delimited json.
    e.g.
    [{<record/>}
    ,{<record/>}
    ]
    if you delete the first character on the line by cut -c2-, you get ldj
    """

    def run(self):
        cmdstring = "wget --user {username} --password {password} -O - {url} | gunzip -c | cut -c2- | gzip > {file} ".format(
            **self.config)
        shellout(cmdstring)
        return 0

    def complete(self):
        r = head(self.config["url"], auth=(
            self.config["username"], self.config["username"]))
        remote = None
        if r.headers["Last-Modified"]:
            datetime_object = parser.parse(r.headers["Last-Modified"])
            remote = float(datetime_object.timestamp())
        if os.path.isfile(self.config["file"]):
            statbuf = os.stat(self.config["file"])
            here = float(statbuf.st_mtime)
        else:
            return False
        if here > remote:
            return True
        else:
            return False

    def output(self):
        return luigi.LocalTarget(self.config.get("file"))


class EFFixIDs(EFTask):
    def requires(self):
        """
        requires EFDownload.complete()
        """
        return EFDownload()

    def run(self):
        with gzip.open(self.config.get("file"), "rt") as f:
            with gzip.open(self.config.get("fixfile"), "wt") as out:
                for line in f:
                    try:
                        record = json.loads(line)
                        # make "d-nb.info/gnd/081547-11" to "081547-11"
                        record["@id"] = record["@id"].split("/")[-1]
                    except:  # bad json or missing "@id"
                        continue
                    print(json.dumps(record, indent=None), file=out)

    def output(self):
        return luigi.LocalTarget(self.config.get("fixfile"))


class EFFillEsIndex(EFTask):
    """
    Loads processed EF data into a given ElasticSearch index (with help of esbulk)
    """
    date = datetime.today()
    es = None

    files = None

    def requires(self):
        """
        requires EFFixIDs.output()
        """
        return EFFixIDs()

    def run(self):
        """
        load Fixed Data into elasticsearch with help of esbulk
        """
        cmd = "esbulk -z -purge -verbose -server http://{host}:{port} -index {index} -w {workers} -type {type} -id @id {fixfile}""".format(
            **self.config)
        shellout(cmd)
        pass

    def complete(self):
        """
        check if the data in elasticsearch is the same as the data on disc
        """
        self.es = elasticsearch.Elasticsearch(
            [{'host': self.config.get("host")}], port=self.config.get("port"))
        cmd = "http://{host}:{port}/{index}/{type}/_search?size=0".format(
            **self.config)
        uniq = set()
        r = get(cmd)
        if r.ok:
            hits=r.json()
        # result=self.es.search(index=self.config["index"],doc_type=typ,size=0)
        if os.path.exists(self.config["fixfile"]):
            with gzip.open(self.config["fixfile"], "rt") as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        uniq.add(record["@id"])
                    except:
                        continue
            if hits.get("hits") and len(uniq) == hits["hits"].get("total"):
                return True
        else:
            return False
        return False
