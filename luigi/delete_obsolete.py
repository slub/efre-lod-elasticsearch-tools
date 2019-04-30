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
# class LoeschLeser forked from the FINC Project

# README
# needs an config file 'deletions_conf.json'

#{
    #"getstrings": {
        #"http://127.0.0.1/finc-main/mrc/_search": {
            #"body": null,
            #"method": "POST"
        #},
        #"http://127.0.0.1/events/schemaorg/": {
            #"method": "GET"
        #},
        #"http://127.0.0.1/geo/schemaorg/": {
            #"method": "GET"
        #},
        #"http://127.0.0.1/orga/schemaorg/": {
            #"method": "GET"
        #},
        #"http://127.0.0.1/persons/schemaorg/": {
            #"method": "GET"
        #},
        #"http://127.0.0.1/swb-aut/mrc/": {
            #"method": "GET"
        #},
        #"http://127.0.0.1/tags/schemaorg/": {
            #"method": "GET"
        #},
        #"http://127.0.0.1/works/schemaorg/": {
            #"method": "GET"
        #}
    #},
    #"host": "http://127.0.0.1",
    #"password": "nix",
    #"url": "ftp://vftp.bsz-bw.de/sekkor/LOEPPN-*",
    #"username": "da"
#}


import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout
from es2json import eprint, put_dict
try:
    import simplejson as json
except ImportError:
    import json
import datetime
from datetime import date
import time, os
import sys
import traceback
from requests import get, delete

class DeleteTask(BaseTask):
    """
    Just a base class for DeleteStuff
    """
    date = str(date.today().strftime("%y%m%d"))
    with open('deletions_conf.json') as data_file:    
        config = json.load(data_file)
        
    def closest(self):
        return daily(date=self.date)


class getDelList(DeleteTask):
    
    def run(self):
        cmdstring="wget -P {date}-delPPN -rnd --user {username} --password {password} {url}".format(**self.config,date=self.date)
        output=shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-delPPN".format(date=self.date))

    def complete(self):
        return True if os.path.exists("{date}-delPPN".format(date=self.date)) else False
    
class getDelPPNs(DeleteTask):
    
    def requires(self):
        return getDelList()
    
    def run(self):
        outputset=set()
        for f in os.listdir(self.date+"-delPPN/"): 
            with open(self.date+"-delPPN/"+f) as handle:
                for line in handle:     # dissect line
                    __date = line[0:5]  # YYDDD, WTF
                    __time = line[5:11] # HHMMSS
                    d_type = line[11:12]
                    # xpn, since this could be ppn or epn; it is an epn, if d_type == 9; it is a ppn if d_type == A
                    # 2018-05-17: #13108 longer EPNs
                    ## __xpn = line[12:21]
                    __xpn = line[12:22]
                    ## __iln = line[21:25] # only in epns
                    __iln = line[22:26] # only in epns
                    if d_type == 'A':
                        for url,conf in self.config.get("getstrings").items():
                            if conf.get("method")=="GET":
                                r=get(url+__xpn.strip())
                                if r.ok and r.json()["found"]:
                                    outputset.add(url[:25]+"/"+r.json()["_index"]+"/"+r.json()["_type"]+"/"+r.json()["_id"])
                            elif conf.get("method")=="POST":
                                # TODO
                                pass
        with open("{date}-toDelete.txt".format(date=self.date),"w") as outp:
            for ppn in outputset:
                outp.write(ppn)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-toDelete.txt".format(date=self.date))
    
    def complete(self):
        return True if os.path.isfile("{date}-toDelete.txt".format(date=self.date)) else False
        
class DeletePPNsByFile(DeleteTask):
    
    def requires(self):
        return getDelPPNs()
        
    def run(self):
        with open("{date}-toDelete.txt".format(date=self.date),"r") as inp:
            for url in inp:
                delete(url.strip())
                
    def complete(self):
        try:
            with open("{date}-toDelete.txt".format(date=self.date),"r") as inp:
                for url in inp:
                    r=get(url.strip())
                    if not r.status_code==404:
                        return False
            return True
        except FileNotFoundError:
            return False
