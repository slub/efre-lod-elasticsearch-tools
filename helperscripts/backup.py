#!/usr/bin/python3
import json
import gzip
import argparse
import traceback
from es2json import esfatgenerator
from multiprocessing import Pool

# script needs a config file to know which indices to save. can be multiple machines
#[
  #{
    #"host": "es_host",
    #"port": 9200,
    #"index": "tags"},
  #{
    #"host": "localhost",
    #"port": 9201,
    #"index":"ef",
  #}
#]

def backup(conf):
    try:
        for records in esfatgenerator(host=conf.get("host"),port=conf.get("port"),index=conf.get("index"),timeout=60):
            if records:
                with gzip.open("{}-{}-{}-{}.ldj.gz".format(conf.get("host"),conf.get("port"),conf.get("index"),records[0].get("_type")) ,"at") as fileout:
                    for record in records:
                        if "_source" in record:
                            print(json.dumps(record["_source"]),file=fileout)
    except Exception as e:
        with open("errors.txt",'a') as f:
            traceback.print_exc(file=f)
    
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
        p=Pool(5)
        p.map(backup,conf) 
