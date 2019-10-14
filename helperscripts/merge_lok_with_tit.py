#!/usr/bin/python3

from argparse import ArgumentParser
from sys import stderr
from json import dumps,loads
from es2json import eprint,esfatgenerator,esidfilegenerator,isint
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError


def main():
    #argstuff
    parser=ArgumentParser(description='Merging of local and title marc records in MarcXchange Json format on ElasticSearch')
    parser.add_argument('-title_host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-title_port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-title_type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-title_index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-title_server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-local_host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-local_port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-local_type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-local_index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-local_server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-selectbody',type=loads,default={"query":{"match":{"852.__.a.keyword":"DE-14"}}})
    parser.add_argument('-idfile',type=str,help='idfile to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    args=parser.parse_args()
    if args.help:
        parser.print_help(stderr)
        exit()        
    if args.title_server:
        slashsplit=args.title_server.split("/")
        args.title_host=slashsplit[2].rsplit(":")[0]
        if isint(args.title_server.split(":")[2].rsplit("/")[0]):
            args.title_port=args.title_server.split(":")[2].split("/")[0]
        args.title_index=args.title_server.split("/")[3]
        if len(slashsplit)>4:
            args.local_type=slashsplit[4]
    if args.local_server:
        slashsplit=args.local_server.split("/")
        args.local_host=slashsplit[2].rsplit(":")[0]
        if isint(args.local_server.split(":")[2].rsplit("/")[0]):
            args.local_port=args.local_server.split(":")[2].split("/")[0]
        args.local_index=args.local_server.split("/")[3]
        if len(slashsplit)>4:
            args.local_type=slashsplit[4]
            
    if args.title_server or ( args.title_host and args.title_port ):
        td=Elasticsearch([{"host":args.title_host}],port=args.title_port)
    else:
        eprint("no server for title data submitted. exiting.")
        exit(-1)
    if (args.local_server or (args.local_host and args.local_port )) and args.idfile:
        ids=dict()
        for i,record in enumerate(esidfilegenerator(host=args.local_host,port=args.local_port,index=args.local_index,type=args.local_type,body=args.selectbody,source="852,004,938",idfile=args.idfile)):
            ids[record["_source"]["004"][0]]={"852":record["_source"]["852"],"938":record["_source"]["852"]}
        titlerecords=td.mget(index=args.title_index,doc_type=args.title_type,body={"ids":[_id for _id in ids]})
        for record in titlerecords["docs"]:
            if "_source" in record:
                for field in ["852","938"]:
                    record["_source"][field]=ids[record["_id"]][field]
                print(dumps(record["_source"]))
            else:
                eprint(dumps(record))
    elif not args.idfile and (args.local_server or (args.local_host and args.local_port)):
        for records in esfatgenerator(host=args.local_host,port=args.local_port,index=args.local_index,type=args.local_type,body=args.selectbody,source="852,004,938"):
            ids=dict()
            for record in records:
                ids[record["_source"]["004"][0]]={"852":record["_source"]["852"],"938":record["_source"]["852"]}
            try:
                titlerecords=td.mget(index=args.title_index,doc_type=args.title_type,body={"ids":[_id for _id in ids]})
            except NotFoundError:
                continue
            except  RequestError:
                continue
            for record in titlerecords["docs"]:
                if "_source" in record:
                    for field in ["852","938"]:
                        record["_source"][field]=ids[record["_id"]][field]
                    print(dumps(record["_source"]))
                else:
                    eprint(dumps(record))

    else:
        eprint("no server for local data submitted. exiting.")
        exit(-1)

if __name__ == "__main__":
    main()



