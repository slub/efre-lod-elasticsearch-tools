
from requests import head, get, delete
from gluish.task import BaseTask
from gluish.utils import shellout
from es2json import put_dict
import luigi
import gzip
import os
from dateutil import parser


class GNTask(BaseTask):
    """
    Just a Base class for geonames
    inline-config
    """

    TAG = 'gn'

    config = {
        "url": "http://download.geonames.org/export/dump/",
        "file": "allCountries",
        "server": "http://127.0.0.1:9200",
        "index": "geonames",
        "type": "record",
        "workers": 8
    }

    ldj = 0
    txt = 0

    def closest(self):
        return daily(date=self.date)


class GNDownload(GNTask):
    def run(self):
        """
        downloads the Geonames Dump and unzips it
        """
        cmdstring = "wget {url}{file}.zip -O {file}.zip".format(**self.config)
        shellout(cmdstring)
        cmd = "unzip -o {file}.zip".format(**self.config)
        shellout(cmd)
        with open(self.config.get("file")+".txt") as f:
            for i, line in enumerate(f):
                pass
            GNTask.txt = i
        return 0

    def complete(self):
        """
        checks if the local copy is from the same date as the file on geonames.org
        """
        r = head(str(self.config.get("url")+self.config.get("file")+".zip"))
        remote = None
        if r.headers.get("Last-Modified"):
            datetime_object = parser.parse(r.headers["Last-Modified"])
            remote = float(datetime_object.timestamp())
        if os.path.isfile(self.config.get("file")+".zip"):
            statbuf = os.stat(self.config.get("file")+".zip")
            here = float(statbuf.st_mtime)
        else:
            return False
        if here < remote:
            return False
        if self.txt > 0:
            return True
        else:
            return False

    def output(self):
        """
        returns the unzipped geonames dump
        """
        return luigi.LocalTarget(self.config.get("file")+".txt")


class GNtsv2json(GNTask):
    def requires(self):
        """
        requires GNDownload
        """
        return [GNDownload()]

    def run(self):
        """
        transforms the geonames TSV Dump to line-delimited JSON
        """
        cmd = ". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/helperscripts/tsv2json.py {file}.txt | gzip > {file}.ldj.gz".format(
            **self.config)
        shellout(cmd)

    def complete(self):
        """
        checks whether the number of records is still identical
        """
        try:
            i = 0
            with gzip.open(self.config.get("file")+".ldj.gz") as f:
                for i, l in enumerate(f):
                    pass
            if i == GNTask.txt:
                GNTask.ldj = i
                return True
        except FileNotFoundError:
            return False
        return False


class GNIngest(GNTask):
    def requires(self):
        """
        requires GNtsv2json
        """
        return [GNtsv2json()]

    def run(self):
        """
        deletes the geonames index
        creates an geonames index with the proper type:geo_point mapping
        ingests the json data
        """
        delete("{server}/{index}".format(**self.config))
        put_dict("{server}/{index}".format(**self.config), {"mappings": {"{type}".format(
            **self.config): {"properties": {"location": {"type": "geo_point"}}}}})
        cmd = "esbulk -z -server {server} -index {index} -type {type} -w {workers} -id id -verbose {file}.ldj.gz".format(
            **self.config)
        shellout(cmd)

    def complete(self):
        """
        is complete when there are as many Records in the index as in the files
        """
        r = get("{server}/{index}/{type}/_search?size=0".format(**self.config))
        if r.ok:
            if GNTask.ldj == r.json().get("hits").get("total") and GNTask.ldj > 0:
                return True
        return False
