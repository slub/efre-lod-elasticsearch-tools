#!/usr/bin/python3
import json
import gzip
import argparse
from es2json import esfatgenerator

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
    parser.add_argument('-conf',type=str,default="backup_conf.json",help="where to look for the backup configuration")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    with open(args.conf,"r") as con:
        conf=json.load(con)
        for machine in conf:
            for index in machine.get("indices"):
                for records in esfatgenerator(host=machine.get("host"),port=machine.get("port"),index=index):
                    if records:
                        with gzip.open("{}-{}-{}-{}.ldj.gz".format(machine.get("host"),machine.get("port"),index,records[0].get("_type")) ,"at") as fileout:
                            for record in records:
                                print(json.dumps(record),file=fileout)
                        
