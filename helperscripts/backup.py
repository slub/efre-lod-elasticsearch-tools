#!/usr/bin/python3
import json
import bz2
import argparse
from es2json import esgenerator

# script needs a config file to know which indices to save. can be multiple machines
#[
  #{
    #"host": "es_host",
    #"port": 9200,
    #"indices": [
      #"tags",
      #"gnd-subjects",
      #"ddc",
      #"dnb-jobs",
      #"geo",
      #"gnd-records",
      #"gvi-tit",
      #"persons",
      #"slub-resources",
      #"gvi-tit-new",
      #"swb-aut",
      #"resources-fidmove",
      #"orga",
      #"works",
      #"tages",
      #"dnb-titel",
      #"resources",
      #"events",
      #"gnd-bnodes"
    #]
  #},
  #{
    #"host": "localhost",
    #"port": 9201,
    #"indices": [
      #"ef",
      #"date",
      #"fidmove-enriched-aut",
      #"kibana",
      #"geonames",
      #"finc-main",
      #"dnb-titel",
      #"resources-fidmovetest"
    #]
  #}
#]


if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Backup your ES-Index')
    parser.add_argument('-help',action="store_true",help="print this help")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    with open("backup_conf.json","r") as con:
        conf=json.load(con)
        for machine in conf:
            for index in machine.get("indices"):
                with bz2.open("{}-{}-{}.ldj.bz2".format(machine.get("host"),machine.get("port"),index) ,"wt") as fileout:
                    for record in esgenerator(host=machine.get("host"),port=machine.get("port"),index=index,headless=True):
                        print(json.dumps(record),file=fileout)
