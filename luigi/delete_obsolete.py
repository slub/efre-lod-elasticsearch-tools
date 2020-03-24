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
from datetime import timedelta
from dateutil import rrule


class DeleteTask(BaseTask):
    """
    Just a base class for DeleteStuff
    """
    today = str(date.today().strftime("%y%m%d"))
    with open('deletions_conf.json') as data_file:    
        config = json.load(data_file)

    lastupdate = datetime.strptime(config["lastupdate"], "%y%m%d")

    def get_ass_ppns(self):
        ass_ppns = set()
        query = {"query": {"match": {"type.keyword": "local"}}}
        #  this iterable contains the IDs of the local records we got over the deleteLists.
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
        return ass_ppns


class DelPPNDailySlices(DeleteTask):
    """
    we get the DeleteLists.
    """
    delete_lines = set()
    days = {}

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
        for line in self.delete_lines:
            ts = line[0:11].replace('-', '0')  # YYDDDHHMMSS or %y%j%H%M%S
            ts = datetime.strptime(ts, "%y%j%H%M%S").strftime("%y%m%d")
            if ts not in self.days:
                self.days[ts] = [line]
            elif ts in self.days:
                self.days[ts].append(line)

        for day, lines in self.days.items():
            if int(day) >= int(self.lastupdate.strftime("%y%m%d")):
                with open("{path}{day}-LOEKXP".format(path=self.config["slices_path"], day=day), "wt") as outp:
                    for line in lines:
                        print(line, file=outp)

    def complete(self):
        if not self.delete_lines:
            return False
        for day, lines in self.days.items():
            if int(day) >= int(self.lastupdate.strftime("%y%m%d")):
                if not os.path.isfile("{path}{day}-LOEKXP".format(path=self.config["slices_path"], day=day)):
                    return False
        return True


class DelPPNDelete(DeleteTask):
    def requires(self):
        """
        requires DelPPNDailySlices().complerte()
        """
        return DelPPNDailySlices()

    def run(self):
        ppns = set()
        for day in rrule.rrule(rrule.DAILY, dtstart=self.lastupdate, until=datetime.today()):
            datefile = "{path}{date}-LOEKXP".format(path=self.config["slices_path"],date=day.strftime("%y%m%d"))
            if not os.path.isfile(datefile):
                continue
            with open(datefile, "rt") as date_fd:
                for line in date_fd:
                    # dissect line
                    del_record = {}
                    ts = line[0:11].replace('-', '0')  # YYDDDHHMMSS or %y%j%H%M%S
                    del_record["ts"] = datetime.strptime(ts, "%y%j%H%M%S").isoformat()
                    del_record["d_type"] = line[11:12]
                    del_record["xpn"] = line[12:22].strip()
                    if del_record["d_type"] == "9":
                        del_record["type"] = "local"
                        del_record["iln"] = line[22:26]
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
        shellout("esbulk -server http://{host}:{port} -index delete_ppns -type ppn -id xpn -verbose -w 1 delete_records-{date}.ldj".format(**self.config, date=self.today))
        """
        set for associated PPNs, we check if linked title data records in
        deleted local data records are still linked to non deleted local records
        if not, we delete them as well, because they are abgesiegelt
        """
        ass_ppns = self.get_ass_ppns()
        ndjson_header = {"Content-type": "application/x-ndjson"}
        successfull_deletions = set()
        for _type in self.config["indices"]:
            if _type == "local":
                query = {"query": {"bool": {"must": [{"match": {"type.keyword": _type}}, {"match": {"iln.keyword": self.config["ILN"]}}]}}}
            else:
                query = {"query": {"match": {"type.keyword": _type}}}
            for index in self.config["indices"][_type]:
                bulk = ""
                for delPPN in esgenerator(host=self.config["host"], port=self.config["port"], index="delete_ppns", type="ppn", source="False", body=query):
                    bulk += json.dumps({"delete": {"_index": index["_index"], "_type": index["_doc_type"], "_id": delPPN["_id"]}}) + "\n"
                if bulk:
                    url = "http://{host}:{port}/{_index}/_bulk".format(**index)
                    response = requests.post(url, data=bulk, headers=ndjson_header)
                    for item in response.json().get("items"):
                        if item["delete"].get("result") == "deleted":
                            successfull_deletions.add("http://{host}:{port}/{index}/{type}/{id}".format(**index,index=item["delete"]["_index"],type=item["delete"]["_type"],id=item["delete"]["_id"]))
        deathList = set()
        for ppn in ass_ppns:
            url = "http://{host}:{port}/{_index}/_search".format(**self.config["indices"]["local"][0])
            query = {"query": {"bool": {"must": {"match": {"004.keyword": ppn}}, "must_not": []}}}
            for isil in self.config["ISIL"]:
                query["query"]["bool"]["must_not"].append({"match": {"852.__.a.keyword": isil}})
            response = requests.post(url, json=query, headers={"Content-type": "application/json"}, params={"size": 0})
            if response.json()["hits"]["total"] > 0:
                deathList.add(ppn)  # there is no other data record pointing to this (defined by must_not search, so you die!)
        for index in self.config["indices"]["title"]:
            if index["delete_associated"]: # we don't want to delete source data records of non-associated records
                bulk = ""
                for ppn in deathList:
                    if ppn:  # avoid empty ppn
                        bulk += json.dumps({"delete": {"_index": index["_index"], "_type": index["_doc_type"], "_id": ppn}}) + "\n"
                if bulk:
                    url = "http://{host}:{port}/{_index}/_bulk".format(**index)
                    response = requests.post(url, data=bulk, headers=ndjson_header)
                    for item in response.json().get("items"):
                        if item["delete"].get("result") == "deleted":
                            successfull_deletions.add("http://{host}:{port}/{index}/{type}/{id}".format(**index,index=item["delete"]["_index"],type=item["delete"]["_type"],id=item["delete"]["_id"]))
        with open("deletions-{date}.txt".format(date=self.today), "wt") as outp:
            for line in successfull_deletions:
                print(line, file=outp)
        self.config["lastupdate"] = self.today
        with open('deletions_conf.json', 'wt') as data_file:
            print(json.dumps(self.config, indent=4),file=data_file)

    def output(self):
        return luigi.LocalTarget("deletions-{date}.txt".format(date=self.today))
