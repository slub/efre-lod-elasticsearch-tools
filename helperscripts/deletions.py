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
# forked from the FINC Project


"""
BSZ deletion line parsing to delete Records out of an elasticsearch server
deletions.py $(find -L /var/finc/data/import/002 -type f -name "LOEPPN-*")
"""


__version_info__ = ('2019','04','29')
__version__ = '-'.join(__version_info__)

# from finc.mappings import * 
# from finc.services import *

try:
    import simplejson as json
except ImportError:
    import json
import logging
import argparse
import datetime
import time, os
import sys
import traceback
from requests import get, delete

# root = logging.getLogger()
# if root.handlers:
#     for handler in root.handlers:
#         root.removeHandler(handler)

getstrings=["http://194.95.145.44:9200/persons/schemaorg/",
            "http://194.95.145.44:9200/works/schemaorg/",
            "http://194.95.145.44:9200/tags/schemaorg/",
            "http://194.95.145.44:9200/events/schemaorg/",
            "http://194.95.145.44:9200/orga/schemaorg/",
            "http://194.95.145.44:9200/geo/schemaorg/",
            "http://194.95.145.44:9200/swb-aut/mrc/",
            #"http://194.95.145.24:9201/finc-main/mrc/_search?q=980.__.a.keyword="
            ]

class LoeschLeser(object):

    def __init__(self, args):
        self.logger = logging.getLogger(__file__)
        self.ppn_deletions = {}
        self.epn_deletions = {}

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def process_line(self, line, since=None, ilns=[]):

        # dissect line
        __date = line[0:5]  # YYDDD, WTF
        __time = line[5:11] # HHMMSS

        __date_sane = ''
        try:
            __date_sane = (
                datetime.datetime(
                    int('20%s' % __date[0:2]), # year
                    1, # month
                    1, # date
                    int(__time[0:2]), # hour
                    int(__time[2:4]), # minute
                    int(__time[4:6])) + 
                datetime.timedelta(int(__date[2:5])-1) # the delta, #1388
            ) #.isoformat() # .strftime('%Y%m%d%H%M')
            # print(__date_sane, file=sys.stderr)
        except Exception as exc:
            self.logger.error('error parsing date: %s' % exc)
            # print('error parsing date: %s' % exc, file=sys.stderr)
        
        d_type = line[11:12]
        # xpn, since this could be ppn or epn; it is an epn, if d_type == 9; it is a ppn if d_type == A
        # 2018-05-17: #13108 longer EPNs
        ## __xpn = line[12:21]
        __xpn = line[12:22]
        ## __iln = line[21:25] # only in epns
        __iln = line[22:26] # only in epns


        # filter

        if since:
            if __date_sane < since:
                # print("not adding ppn/epn of date %s" % __date_sane, file=sys.stderr)
                return

        # https://wiki.bsz-bw.de/doku.php?id=v-team:daten:datendienste:sekkor
        if d_type == '9':
            self.epn_deletions[__xpn] = __date_sane

            if ilns and len(__iln) > 0: # no iln for ppns / title deletions
                intiln = int(__iln)
                if intiln not in ilns:
                    self.logger.debug("not adding ppn/epn to iln %s" % __iln)
                    return

        if d_type == 'A':
            self.ppn_deletions[__xpn] = __date_sane


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='deletions.py', description='delete bsz ppns out of your EFRE LOD Elasticsearch Server')
    parser.add_argument('-V', '--version', action='version', version="{prog}s ({version})".format(prog="%(prog)", version=__version__))
    # parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('infile', type=str, nargs='*')
    parser.add_argument('--since', type=str, help='collect ppns/epns since given date only, yyyy-mm-dd format') #, default='2013-01-01')
    parser.add_argument('--ilns', type=str, help='collect ppns/epns of specified ilns only, comma separated') # 5,10,20,27,48,50,57,61,89,97,161,400
    parser.add_argument('--dtype', type=str, help='output either only ppns (ppn), epns (epn), or both (all)', default="ppn")

    args = parser.parse_args()

    date_since = None
    if args.since:
        date_since = datetime.datetime.strptime(args.since, '%Y-%m-%d')

    ilns = []
    if args.ilns:
        ilns = [int(iln) for iln in args.ilns.split(',')]

    logging.basicConfig(
    # filename='/var/log/mdma.log',
    filename='/dev/stderr',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

    # multiple with ... python 2.7 or contextlib

    start = datetime.datetime.now()

    with LoeschLeser(args) as lm:
        for infile in args.infile:
            lm.logger.info("reading file %s" % infile)
            with open(infile) as handle:

                # timestamp = datetime.datetime.strptime(time.ctime(os.path.getctime(args.infile)), "%a %b %d %H:%M:%S %Y")

                for i, line in enumerate(handle, 1):
                    try:
                        lm.process_line(line, since=date_since, ilns=ilns)
                    except Exception as exc:
                        lm.logger.error('error parsing %s: %s' % (line, exc))
                        lm.logger.error(traceback.format_exc())

        # lm.logger.info("%d deletions in %d seconds (%d bytes). printing to stdout ..." % ( len(lm.deletions), ( datetime.datetime.now() - start ).seconds, sys.getsizeof(lm.deletions)) )#, file=sys.stderr)
        if args.dtype in ('all', 'epn'):
            for xpn,dat in lm.epn_deletions.items():
                print("%s %s" % (xpn, dat.strftime('%Y%m%d%H%M')))
        if args.dtype in ('all', 'ppn'):
            for xpn,dat in lm.ppn_deletions.items():
                for getstring in getstrings:
                    r=get(getstring+xpn.strip())
                    if r.ok and r.json()["found"]:
                        delete(getstring[:25]+"/"+r.json()["_index"]+"/"+r.json()["_type"]+"/"+r.json()["_id"])
                        #print(getstring[:25]+"/"+r.json()["_index"]+"/"+r.json()["_type"]+"/"+r.json()["_id"])
            
#				print("%s" % (xpn))
