#!/usr/bin/env python3

import sys
import json
import argparse
import requests
from es2json import esgenerator
from es2json import eprint
from es2json import isint


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

    for record in esgenerator(host=host, port=port, index=index, type=doc_type, id=_id, headless=True):
        for key, value in traverse(record, ""):
            if isinstance(value, str) and value.startswith(args.base_uri):
                r = requests.get(value)
                if not r.ok:
                    eprint(record["@id"], "has an", r.status_code, "in",  key, value)


if __name__ == "__main__":
    run()
