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
import os
from gluish.task import BaseTask
from gluish.utils import shellout
from es2json import esidfilegenerator
from datetime import date


class DeleteTask(BaseTask):
    """
    Just a base class for DeleteStuff
    """
    today = str(date.today().strftime("%y%m%d"))
    with open('deletions_conf.json') as data_file:    
        config = json.load(data_file)


class getDelList(DeleteTask):
    """
    we get the DeleteLists.
    """
    def run(self):
        """
        we download the deleteLists from the FTP with the correct credentials and save them to our working-directory
        """
        cmdstring = "wget -P {date}-delPPN -rnd --user {username} --password {password} {url}".format(**self.config,date=self.today)
        output = shellout(cmdstring)
        return 0

    def output(self):
        return luigi.LocalTarget("{date}-delPPN".format(date=self.today))

    def complete(self):
        return True if os.path.exists("{date}-delPPN".format(date=self.today)) else False


class getDelPPNs(DeleteTask):
    def requires(self):
        return getDelList()

    def run(self):
        """
        we iterate thru all the deleteLists and extract the correct PPNs and put them into the correct files, which are line-delimited PPNs
        """
        lok_epns = set()  # 3 sets for deduplication
        tit_ppns = set()
        norm_ppns = set()
        
        for f in os.listdir(self.today+"-delPPN/"):
            with open("{date}-delPPN/{file}".format(date=self.today, file=f)) as handle:
                for line in handle:
                    # dissect line
                    __date = line[0:5]  # YYDDD, WTF
                    __time = line[5:11]  # HHMMSS
                    d_type = line[11:12]  # epn = 9, titledata = A, normdata = B|C|D
                    __xpn = line[12:22]
                    __iln = line[22:26]  # only in epns
                    # __xpn is an EPN and the trailing ILN is our configured ILN
                    if d_type == "9" and __iln == self.config["ILN"]:
                        lok_epns.add(__xpn)

                    #  __xpn is an PPN for title data 
                    elif d_type == "A":
                        tit_ppns.add(__xpn)

                    # __xpn is a authority data
                    elif d_type in ("B", "C", "D"):
                        norm_ppns.add(__xpn)

                    #associated-tit everything else is not in our interest
                    else:
                        continue

        with open("{date}-delPPN/kxp-lok".format(date=self.today), "w") as lok:
            for epn in lok_epns:
                print(epn, file=lok)

        with open("{date}-delPPN/kxp-tit".format(date=self.today), "w") as tit:
            for ppn in tit_ppns:
                print(ppn, file=tit)

        with open("{date}-delPPN/kxp-norm".format(date=self.today), "w") as norm:
            for ppn in norm_ppns:
                print(ppn, file=norm)

        associated_ppns = set()
        """
        we iterate thru the epns and ther corresponding local data records, save the associated PPNs which are in field 004,
        if no local data record is refering to the associated ppn, then we call it a day or abgesigelt and delete it in all our title and resources indices
        """
        for lok_record in esidfilegenerator(host=self.config["indices"]["kxp-lok"][0]["host"],
                                            port=self.config["indices"]["kxp-lok"][0]["port"],
                                            index=self.config["indices"]["kxp-lok"][0]["_index"],
                                            type=self.config["indices"]["kxp-lok"][0]["_doc_type"],
                                            idfile="{date}-delPPN/kxp-lok".format(date=self.today),
                                            headless=True):
            if lok_record and "004" in lok_record:
                associated_ppns.add(lok_record["004"][0])

        with open("{date}-delPPN/associated-tit".format(date=self.today), "w") as assoc_tit:
            for ppn in associated_ppns:
                print(ppn, file=assoc_tit)

    def output(self):
        return [
            luigi.LocalTarget("{date}-delPPN/kxp-lok".format(date=self.today)),
            luigi.LocalTarget("{date}-delPPN/kxp-tit".format(date=self.today)),
            luigi.LocalTarget("{date}-delPPN/kxp-norm".format(date=self.today)),
            luigi.LocalTarget("{date}-delPPN/associated-tit".format(date=self.today))
                ]

    def complete(self):
        if not os.path.isfile("{date}-delPPN/kxp-lok".format(date=self.today)):
            return False
        if not os.path.isfile("{date}-delPPN/kxp-tit".format(date=self.today)):
            return False
        if not os.path.isfile("{date}-delPPN/kxp-norm".format(date=self.today)):
            return False
        if not os.path.isfile("{date}-delPPN/associated-tit".format(date=self.today)):
            return False
<<<<<<< HEAD
=======

>>>>>>> f2c28feff863a26b7023730d60813b11edb51dc4
        return True


class deletePPNsFirstHand(DeleteTask):
    def requires(self):
        return getDelPPNs()

    def run(self):
        """
        we iterate over the files/indices described in the config and delete all the PPNs
        """
        header = {"Content-type": "application/x-ndjson"}
        for _file in self.config["indices"]:
            with open("{date}-delPPN/{infile}".format(date=self.today, infile=_file)) as inFile:
                for index in self.config["indices"][_file]:
                    bulk = ""
                    for line in inFile:
                        ppn = line.strip()
                        if ppn:  # avoid empty ppn
                            bulk += json.dumps({"delete": {"_index": index["_index"], "_type": index["_doc_type"], "_id": ppn}}) + "\n"
                    if bulk:
                        url = "http://{host}:{port}/{_index}/_bulk".format(**index)
                        response = requests.post(url, data=bulk, headers=header)

    def complete(self):
        """
        just a check if there are still records described by those PPNs
        """
        for _file in self.config["indices"]:
            for index in self.config["indices"][_file]:
                for response in esidfilegenerator(host=index["host"], port=index["port"], index=index["_index"], type=index["_doc_type"], idfile="{date}-delPPN/{fd}".format(date=self.today, fd=_file), headless=False):
                    if response.get("error"):
                        print(response["error"])
                    elif response["found"]:
                        return False
                    else:
                        continue

        return True


class getAssociatedDelPPNs(DeleteTask):
    def requires(self):
        return [deletePPNsFirstHand(), getDelPPNs()]

    def run(self):
        """
        here we take care of the associated PPNs, we take the saved IDs and search in the local data index if there are still records refering to them, if not: we write them out into a deathList
        """
        header = {"Content-type": "Application/json"}
        params = {"size": 0}
        
        deathList = set()
        with open("{date}-delPPN/associated-tit".format(date=self.today), "r") as inp:
            for line in inp:
                ppn = line.strip()
                if ppn:  # avoid empty ppn
                    url = "http://{host}:{port}/{_index}/_search".format(**self.config["indices"]["kxp-lok"][0])
                    query = {"query": {"bool": {"must": [{"match": {"004.keyword": ppn}}, {"match": {"852.__.a.keyword": self.config["ISIL"]}}]}}}
                    response = requests.post(url, json=query, headers=header, params=params)
                    if response.json()["hits"]["total"] > 0:
                        continue  # there are still other local data records pointing to that epn, so you live
                    elif response.json()["hits"]["total"] == 0:
                        deathList.add(ppn)

        with open("{date}-delPPN/associated-tit-DELETIONS".format(date=self.today), "w") as inp:
            for ppn in deathList:
                print(ppn, file=inp)

    #def output(self):
        #return luigi.LocalTarget("{date}-delPPN/associated-tit-DELETIONS".format(date=self.today))

    def complete(self):
        if not os.path.isfile("{date}-delPPN/associated-tit-DELETIONS".format(date=self.today)):
            return False
        return True

class delAssociatedDelPPNs(DeleteTask):

    def requires(self):
        return [getAssociatedDelPPNs(),deletePPNsFirstHand()]

    def run(self):
        """
        here we iterate over the deathList file and delete those PPNs out of our title indices
        """
        with open("{date}-delPPN/associated-tit-DELETIONS".format(date=self.today), "r") as inp:
            for index in self.config["indices"]["kxp-tit"]:
                bulk = ""
                for ppn in inp:
                    ppn = line.strip()
                    if ppn:  # avoid empty ppn
                        bulk += json.dumps({"delete": {"_index": index["_index"], "_type": index["_doc_type"], "_id": ppn}}) + "\n"
                if bulk:
                    url = "http://{host}:{port}/{_index}/_bulk".format(**index)
                    response = requests.post(url, data=bulk, headers=header)
                
                #    url = "http://{host}:{port}/{_index}/{_doc_type}/{_id}".format(**index, _id=ppn)
                #    response = requests.delete(url)
                #    print(url, response.json().get("result"))

    def complete(self):
        """
        here we check if there is an file at all and if all the PPNs are deleted tho
        """
        if not os.path.isfile("{date}-delPPN/associated-DELETIONS".format(date=self.today)):
            return False
        for index in self.config["indices"]["kxp-tit"]:
            for response in esidfilegenerator(host=index["host"], port=index["port"], index=index["_index"], type=index["_doc_type"], idfile="{date}-delPPN/associated-DELETIONS".format(date=self.today), headless=False):
                if response["found"]:
                    return False
        return True
