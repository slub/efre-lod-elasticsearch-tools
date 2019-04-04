#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator,isint,litter,eprint,esidfilegenerator

def enrichidentifier(identifier,host,port):
    if identifier.get("propertyID") in ["fieldOfStudy","fieldOfActivity"] and identifier.get("value"):
        gndrec=requests.get("http://{}:{}/gnd-records/record/{}?_source=preferredNameForTheSubjectHeading".format(host,port,identifier.get("value")))
        if gndrec.ok and "preferredNameForTheSubjectHeading" in gndrec.json().get("_source"):
            identifier["name"]=gndrec.json().get("_source").get("preferredNameForTheSubjectHeading")[0]
            return identifier
    return None

def enrichabout(about,host,port):
    change=False
    if isinstance(about.get("identifier"),list):
        for n,elem in enumerate(about.get("identifier")):
            newId=enrichidentifier(elem,host,port)
            if newId:
                about["identifier"][n]=newId
                change=True
    elif isinstance(about.get("identifier"),dict):
        newId=enrichidentifier(about["identifier"],host,port)
        if newId:
            about["identifier"]=newId
            change=True
    if change:
        return about
    else:
        return None


def enrichrecord(record,host,port):
    change=False
    for field in ["author","contributor"]:
        if isinstance(record.get(field),list):
            for n,person in enumerate(record.get(field)):
                if person.get("about"):
                    if isinstance(person.get("about"),list):
                        for m,about in enumerate(person.get("about")):
                            newAbout=enrichabout(about,host,port)
                            if newAbout:
                                record[field][n]["about"][m]=newAbout
                                change=True
                    elif isinstance(person.get("about"),dict):
                        newAbout=enrichabout(person.get("about"),host,port)
                        if newAbout:
                            record[field][n]["about"]=newAbout
                            change=True
    if change:
        print(json.dumps(record))
                    
    
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich titledata over about!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument("-id",type=str,help="retrieve single document (optional)")
    parser.add_argument('-stdin',action="store_true",help="get data from stdin")
    parser.add_argument('-pipeline',action="store_true",help="output every record (even if not enriched) to put this script into a pipeline")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty") #no, i don't steal the syntax from esbulk...
    parser.add_argument('-searchserver',type=str,help="search instance to use. default is -server e.g. http://127.0.0.1:9200")
    parser.add_argument('-idfile',type=str,help="path to a file with IDs to process")
    args=parser.parse_args()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    if args.searchserver:
        slashsplit=args.searchserver.split("/")
        search_host=slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            search_port=args.searchserver.split(":")[2].split("/")[0]
    else:
        search_host=args.host
        search_port=args.port
    searchbody={
  "query": {
    "bool": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "author.about.identifier.propertyID": "fieldOfStudy"
              }
            },
            {
              "match": {
                "author.about.identifier.propertyID": "fieldOfActivity"
              }
            },
            {
              "match": {
                "contributor.about.identifier.propertyID": "fieldOfStudy"
              }
            },
            {
              "match": {
                "contributor.about.identifier.propertyID": "fieldOfActivity"
              }
            }
          ]
        }
      }
    }
  }
}
    for rec in esgenerator(host=args.host,port=args.port,index=args.index,body=searchbody,id=args.id,type=args.type,headless=True):
            enrichrecord(rec,args.host,args.port)
            
