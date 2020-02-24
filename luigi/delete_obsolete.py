#!/usr/bin/python3
# coding: utf-8

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#
#
#
# need to keep this License because some lines copied from the FINC Project

# README
# needs an config file 'deletions_conf.json'

import luigi
import json
import time
import requests
from ftplib import FTP
import os
from gluish.task import BaseTask
from gluish.utils import shellout
from es2json import esidfilegenerator
from es2json import esgenerator
from datetime import date
from datetime import datetime


class DeleteTask(BaseTask):
    """
    Just a base class for DeleteStuff
    """
    today = str(date.today().strftime("%y%m%d"))
    with open('deletions_conf.json') as data_file:    
        config = json.load(data_file)


class getDelPPNs(DeleteTask):
    """
    we get the DeleteLists.
    """
    delete_lines = set()

    def run(self):
        """
        we download the deleteLines from the FTP with the correct credentials and save them to our set
        """
        with FTP(self.config["ftp"]) as ftp:
            ftp.login(self.config["username"], self.config["password"])
            for data in ftp.nlst(self.config["filename_pattern"]):
                ftp.retrlines('RETR ' + data, self.delete_lines.add)
        
        """
        we iterate thru the set and extract the correct PPNs and put them into the correct files, which are line-delimited PPNs
        """
        ppns = set()
        for line in self.delete_lines:
            # dissect line
            del_record = {}
            ts = line[0:11].replace('-', '0')  # YYDDDHHMMSS or %y%j%H%M%S
            del_record["ts"] = datetime.strptime(ts, "%y%j%H%M%S").isoformat()

            del_record["d_type"] = line[11:12]
            del_record["xpn"] = line[12:22].strip()
            if del_record["d_type"] == '9':
                del_record["type"] = "local"
                del_record["iln"] = line[22:26]
            if del_record["d_type"] == "9" and del_record["iln"] == self.config["ILN"]:
                ppns.add(json.dumps(del_record))  # converting this already to a string because a dict would be unhashable and we need a set for deduplication

            #  __xpn is an PPN for title data 
            elif del_record["d_type"] == "A":
                del_record["type"] = "title"
                ppns.add(json.dumps(del_record))

            # __xpn is a authority data
            elif del_record["d_type"] in ("B", "C", "D"):
                del_record["type"] = "authority"
                ppns.add(json.dumps(del_record))

            # everything else is not in our interest
            else:
                continue

        with open("delete_records-{date}.ldj".format(date=self.today), "w") as outp:
            for record in ppns:
                print(record, file=outp)

    def output(self):
        return luigi.LocalTarget("delete_records-{date}.ldj".format(date=self.today))


class ingest_dellist(DeleteTask):
    """
    we ingest the created Dellists into our deletions index"
    """
    def requires(self):
        return getDelPPNs()

    def run(self):
        shellout("esbulk -server http://{host}:{port} -index delete_ppns -type ppn -id xpn -verbose -w 1 delete_records-{date}.ldj".format(**self.config, date=self.today))

    def complete(self):
        es_ids = set()
        file_ids = set()
        try:
            with open("delete_records-{date}.ldj".format(date=self.today)) as inp:
                for line in inp:
                    record = json.loads(line)
                    file_ids.add(record["xpn"])
            shellout("jq .xpn -rc < delete_records-{date}.ldj > ids.txt".format(date=self.today))
            for record in esidfilegenerator(host=self.config["host"], port=self.config["port"], index="delete_ppns", type="ppn", idfile="ids.txt", source="False"):
                if not record.get("found"):
                    return False
                es_ids.add(record.get("_id"))
        except FileNotFoundError:
            return False
        for _id in file_ids:
            if _id not in es_ids:
                return False
        return True


class deletePPNsFirstHand(DeleteTask):
    def requires(self):
        return ingest_dellist()

    def run(self):
        """
        we iterate over the files/indices described in the config and delete all the PPNs
        """
        header = {"Content-type": "application/x-ndjson"}
        for _type in self.config["indices"]:
            for index in self.config["indices"][_type]:
                bulk = ""
                for delPPN in esgenerator(host=self.config["host"], port=self.config["port"],index="delete_ppns", type="ppn", source="False",body={"query": {"match": {"type.keyword": _type}}}):
                    bulk += json.dumps({"delete": {"_index": index["_index"], "_type": index["_doc_type"], "_id": delPPN["_id"]}}) + "\n"
                if bulk:
                    url = "http://{host}:{port}/{_index}/_bulk".format(**index)
                    response = requests.post(url, data=bulk, headers=header)

    def complete(self):
        """
        just a check if there are still records described by those PPNs
        """
        for _type in self.config["indices"]:
            query = {"query": {"match": {"type.keyword": _type}}}
            iterable = [x["_id"] for x in esgenerator(host=self.config["host"],
                                                      port=self.config["port"],
                                                      index="delete_ppns",
                                                      type="ppn",
                                                      source="False",
                                                      body=query)]
            if not iterable:
                return False
            for index in self.config["indices"][_type]:
                for response in esidfilegenerator(host=index["host"],
                                                  port=index["port"],
                                                  index=index["_index"],
                                                  type=index["_doc_type"],
                                                  source=None,
                                                  headless=False,
                                                  idfile=iterable):
                    if response.get("error"):
                        print(response["error"])
                    elif response["found"]:
                        return False
                    else:
                        continue
        return True


class getAssociatedDelPPNs(DeleteTask):
    header = {"Content-type": "Application/json"}

    def requires(self):
        return deletePPNsFirstHand()

    def get_ass_ppns(self):

        ass_ppns = set()
        query = {"query": {"match": {"type.keyword": "local"}}}
        iterable = [x["_id"] for x in esgenerator(host=self.config["host"],
                                                  port=self.config["port"],
                                                  index="delete_ppns",
                                                  type="ppn",
                                                  source="False",
                                                  body=query)]
        if not iterable:
            return False
        for lok_record in esidfilegenerator(host=self.config["indices"]["local"][0]["host"],
                                            port=self.config["indices"]["local"][0]["port"],
                                            index=self.config["indices"]["local"][0]["_index"],
                                            type=self.config["indices"]["local"][0]["_doc_type"],
                                            idfile=iterable,
                                            headless=True):
            if lok_record and "004" in lok_record:
                ass_ppns.add(lok_record["004"][0])

        deathList = set()
        for ppn in ass_ppns:
            url = "http://{host}:{port}/{_index}/_search".format(**self.config["indices"]["local"][0])
            query = {"query": {"bool": {"must": [{"match": {"004.keyword": ppn}}, {"match": {"852.__.a.keyword": self.config["ISIL"]}}]}}}
            response = requests.post(url, json=query, headers=self.header, params={"size": 0})
            if response.json()["hits"]["total"] > 0:
                continue  # there are still other local data records pointing to that epn, so you live
            elif response.json()["hits"]["total"] == 0:
                deathList.add(ppn)
        return deathList

    def run(self):
        deathList = self.get_ass_ppns()
        for index in self.config["indices"]["title"]:
            bulk = ""
            for ppn in deathList:
                if ppn:  # avoid empty ppn
                    bulk += json.dumps({"delete": {"_index": index["_index"], "_type": index["_doc_type"], "_id": ppn}}) + "\n"
            if bulk:
                print(bulk)
                url = "http://{host}:{port}/{_index}/_bulk".format(**index)
                response = requests.post(url, data=bulk, headers=self.header)

    def complete(self):
        deathList = self.get_ass_ppns()
        if deathList == "False":
            return False
        if not deathList:
            return True
        for index in self.config["indices"]["title"]:
            for response in esidfilegenerator(host=index["host"],
                                              port=index["port"],
                                              index=index["_index"],
                                              type=index["_doc_type"],
                                              source=None,
                                              headless=False,
                                              idfile=deathList):
                if response.get("error"):
                    print(response["error"])
                elif response["found"]:
                    return False
                else:
                    continue
        return True
