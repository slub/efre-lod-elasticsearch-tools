#!/usr/bin/python3
import argparse
from es2json import esgenerator
from es2json import esfatgenerator
from es2json import ArrayOrSingleValue
from es2json import eprint
from es2json import litter
from es2json import isint
from requests import get
import json
import sys

mapping={"persons":["relatedTo","hasOccupation","about","workLocation"],
         "geo":["GeoCoordinates"]}
       
def handle_record(record,host,port):
        changed=False
        for person in ["author","contributor","mentions"]:
            if record.get(person):
                if isinstance(record[person],dict):
                    if record[person].get("@id"):
                        for index,attributes in mapping.items():
                            r=get("http://"+host+":"+port+"/"+index+"/schemaorg/"+record[person]["@id"].split("/")[4])
                            if r.ok:
                                print(r.json())
                    elif record[person].get("sameAs"):
                        if isinstance(record[person]["sameAs"],list):
                            for sameAs in record[person]["sameAs"]:
                                for index,attributes in mapping.items():
                                    r=get("http://"+host+":"+port+"/"+index+"/schemaorg/_search?q=sameAs:\""+sameAs+"\"")
                                    if r.ok and r.json().get("hits").get("total")>=1:
                                        response=r.json().get("hits").get("hits")[0].get("_source")
                                        for attr in attributes:
                                            if response.get(attr):
                                                record[person][attr]=response[attr]
                        elif isinstance(record[person]["sameAs"],str):
                            for index,attributes in mapping.items():
                                r=get("http://"+host+":"+port+"/"+index+"/schemaorg/_search?q=sameAs:\""+record[person]["sameAs"]+"\"")
                                if r.ok and r.json().get("hits").get("total")>=1:
                                    response=r.json().get("hits").get("hits")[0].get("_source")
                                    for attr in attributes:
                                        if response.get(attr):
                                            record[person][attr]=response[attr]
                elif isinstance(record[person],list):
                    for i,author in enumerate(record[person]):
                        if isinstance(author,dict):
                            if author.get("@id"):
                                for index,attributes in mapping.items():
                                    r=get("http://"+host+":"+port+"/"+index+"/schemaorg/"+author["@id"].split("/")[4])
                                    if r.ok:
                                        print(r.json())
                            elif author.get("sameAs"):
                                if isinstance(author["sameAs"],list):
                                    for sameAs in author["sameAs"]:
                                        for index,attributes in mapping.items():
                                            r=get("http://"+host+":"+port+"/"+index+"/schemaorg/_search?q=sameAs:\""+sameAs+"\"")
                                            if r.ok and r.json().get("hits").get("total")>=1:
                                                response=r.json().get("hits").get("hits")[0].get("_source")
                                                for attr in attributes:
                                                    if response.get(attr):
                                                        record[person][i][attr]=response[attr]
                                elif isinstance(author["sameAs"],str):
                                    for index, attributes in mapping.items():
                                        r=get("http://"+host+":"+port+"/"+index+"/schemaorg/_search?q=sameAs:\""+author["sameAs"]+"\"")
                                        if r.ok and r.json().get("hits").get("total")>=1:
                                            response=r.json().get("hits").get("hits")[0].get("_source")
                                            for attr in attributes:
                                                if response.get(attr):
                                                    record[person][i][attr]=response[attr]
        print(json.dumps(record,indent=None))


if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Enrich Resources with authority Data for FID-Move')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-id',type=str,help='map single document, given by id')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-stdin',action="store_true",help="If you want to read from stdin")
    parser.add_argument('-debug',action="store_true",help='Dump processed Records to stdout (mostly used for debug-purposes)')
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-w',type=int,default=8,help="how many processes to use")
    args=parser.parse_args()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        if len(slashsplit)>3:
            args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    if args.help:
        parser.print_help(sys.stderr)
        exit()     
    
    
    if args.stdin:
        for line in sys.stdin:
            jline=json.loads(line)
            handle_record(jline,args.host,args.port)
    else:
        for record in esgenerator(host=args.host,port=9200,index=args.index,type=args.type,body=None,source=True,source_exclude=None,source_include=None,headless=True):
            handle_record(record,args.host,str(args.port))
                                        
                
