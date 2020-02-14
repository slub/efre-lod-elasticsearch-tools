#!/usr/bin/env python3

import sys
import json
import argparse
import requests
from es2json import esfatgenerator
from es2json import eprint
from es2json import isint
from es2json import litter


def traverse(dict_or_list, path):
    """
    iterate through a python dict or list, yield all the values
    """
    iterator = None
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list, list):
        iterator = enumerate(dict_or_list)
    elif isinstance(dict_or_list, str):
        strarr = []
        strarr.append(dict_or_list)
        iterator = enumerate(strarr)
    else:
        return
    if iterator:
        for k, v in iterator:
            yield path + str([k]), v
            if isinstance(v, (dict, list)):
                for k, v in traverse(v, path + str([k])):
                    yield k, v

def run():
    parser = argparse.ArgumentParser(description='Test your internal open data links!')
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id, id and type are optional, point to your local backend elasticsearch index")
    parser.add_argument(
        '-base_uri', type=str, help="use http://opendata.yourinstitution.org, determinate which base_uri should be tested.")
    args = parser.parse_args()
    
    if not args.server:
        eprint("error, -server argument missing!")
        exit(-1)
    slashsplit = args.server.split("/")
    host = slashsplit[2].rsplit(":")[0]
    if isint(args.server.split(":")[2].rsplit("/")[0]):
        port = args.server.split(":")[2].split("/")[0]
    index = args.server.split("/")[3]
    if len(slashsplit) > 4:
        doc_type = slashsplit[4]
        _id = None
    if len(slashsplit) > 5:
        if "?pretty" in args.server:
            pretty = True
            _id = slashsplit[5].rsplit("?")[0]
        else:
            _id = slashsplit[5]
    
    header = {"Content-type": "Application/json"}
    for records in esfatgenerator(host=host, port=port, index=index, type=doc_type):
        mget_body = {"docs": []}
        target_source_map = {}
        for record in records:
            for key, value in traverse(record["_source"], ""):
                if isinstance(value, str) and value.startswith(args.base_uri):
                    if not "source" in value:
                        mget_body["docs"].append({"_index":value.split("/")[-2],"_id":value.split("/")[-1]})
                        if not value in target_source_map:
                            target_source_map[value]=[]
                        target_source_map[value].append({key: record["_source"]["@id"]})
        r = requests.post("http://{host}:{port}/_mget".format(host=host,port=port), json=mget_body, headers=header)
        for doc in r.json()["docs"]:
            if doc["found"]:
                continue
            else:
                for obj in target_source_map[args.base_uri+"/"+doc["_index"]+"/"+doc["_id"]]:
                    for key, base in obj.items():
                        print(base, "has an 404 in", key, args.base_uri+"/"+doc["_index"]+"/"+doc["_id"])
                #r = requests.get(value)
                #if not r.ok:
                    #eprint(record["@id"], "has an", r.status_code, "in",  key, value)


if __name__ == "__main__":
    run()
