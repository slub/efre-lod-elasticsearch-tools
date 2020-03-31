#!/usr/bin/env python3

import sys
import json
import argparse
import requests
import xml.etree.ElementTree as ET
from es2json import esfatgenerator
from es2json import esgenerator
from es2json import eprint
from es2json import isint
from es2json import ArrayOrSingleValue

header = {"Content-type": "Application/json"}
with open("config_linkchecker.json", "r") as inp:
    mapping = json.load(inp)


def uniq(lst):
    """
    return lst only with unique elements in it
    """
    last = object()
    for item in lst:
        if item == last:
            continue
        yield item
        last = item


def oldgnd2newgnd(_id):
    query = {"_source": False,"query": {"match": {"035.__.z.keyword": "(DE-588)"+str(_id)}}}
    url = "http://{host}:{port}/{index}/{type}/_search".format(**mapping["(DE-588)"])
    r = requests.post(url, json=query, headers=header)
    if r.ok and r.json()["hits"]["total"]>0:
        for hit in r.json()["hits"]["hits"]:
            yield hit["_id"]
    else:
        return None



def getmarcvalues(record, regex, entity):
    if len(regex) == 3 and regex in record:
        yield record.get(regex)
    else:
        record = record.get(regex[:3])
        """
        beware! hardcoded traverse algorithm for marcXchange record encoded data !!! 
        temporary workaround: http://www.smart-jokes.org/programmers-say-vs-what-they-mean.html
        """
        # = [{'__': [{'a': 'g'}, {'b': 'n'}, {'c': 'i'}, {'q': 'f'}]}]
        if isinstance(record, list):
            for elem in record:
                if isinstance(elem, dict):
                    for k in elem:
                        if isinstance(elem[k], list):
                            for final in elem[k]:
                                if regex[-1] in final:
                                    yield final.get(regex[-1])






def check_datahub(ppn): # ppn is sth like: "(DE-588)131291223X
    aut = ppn[:8]  # getting (DE-588)
    xpn = ppn[8:]  # getting 131291223X
    r = requests.get("https://data.slub-dresden.de/{aut_provider}/{ppn}".format(aut_provider=mapping[aut]["aut_provider"], ppn=xpn))
    if r.ok:
        return True
    return False


def check_aut(ppn):
    aut = ppn[:8]
    xpn = ppn[8:]
    provider = mapping[aut]["aut_provider"]
    if provider == "swb": 
        sru_xml_data = requests.get("http://swb2.bsz-bw.de/sru/DB=2.1/username=/password=/&operation=searchRetrieve&maximumRecords=10&recordSchema=dc&query=pica.ppn:{ppn}".format(ppn=xpn))
        if sru_xml_data.ok:
            num_of_records = ET.fromstring(sru_xml_data.content).find("{http://www.loc.gov/zing/srw/}numberOfRecords").text
            if int(num_of_records) > 0:
                return True
    elif provider == "gnd":
        gnd_data = requests.get("https://d-nb.info/gnd/{ppn}".format(ppn=xpn))
        if gnd_data.ok:
            return True
    return False


def get_swb_ts(ppn):
    aut = ppn[:8]
    xpn = ppn[8:]
    provider = mapping[aut]["aut_provider"]
    if provider == "swb": 
        url = "https://sru.bsz-bw.de/ognd"
        params = {
            "operation": "searchRetrieve",
            "maximumRecords": 10,
            "recordSchema": "marcxmlk10os",
            "query": "pica.ppn=\"{}\"".format(xpn)
        }
        sru_xml_data = requests.get(url, params=params)
        if sru_xml_data.ok:
            for line in sru_xml_data.text.split("\n"):
                if "\"005\"" in line:
                    return float(line.split(">")[1].split("<")[0])
    return 0.0


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


def check_key(key):
    """
    by the Marc21 standard, there are some already as invalid marked control numbers in the record, such as 035.*.z, we check, if we dont want to print them
    """
    field = int(key.split("'")[1])
    subfield = key.split("'")[-2]

    # https://www.loc.gov/marc/bibliographic/bd035.html
    if field == 35 and subfield == 'z':
        return False

    return True


def run():
    parser = argparse.ArgumentParser(description='Test your internal open data links!')
    parser.add_argument(
        '-server', type=str, required=True,
        help="use http://host:port/index/type/id, id and type are optional, "
             "point to your local backend elasticsearch index")
    args = parser.parse_args()
    
    slashsplit = args.server.split("/")
    host = slashsplit[2].rsplit(":")[0]
    if isint(args.server.split(":")[2].rsplit("/")[0]):
        port = args.server.split(":")[2].split("/")[0]
    index = args.server.split("/")[3]
    doc_type=None
    if len(slashsplit) > 4:
        doc_type = slashsplit[4]
        _id = None
    if len(slashsplit) > 5:
        if "?pretty" in args.server:
            pretty = True
            _id = slashsplit[5].rsplit("?")[0]
        else:
            _id = slashsplit[5]

    sys.stdout.write("{},{},{},{},{}\n".format("subject",
                                                  "path",
                                                  "not found id",
                                                  "entity",
                                                  "comment"
                                                  #"existent at authority-provider")
                                                  ))
    sys.stdout.flush()
    for records in esfatgenerator(host=host, port=port, index=index, type=doc_type):
        mget_bodys = {}
        for key in mapping:
            mget_bodys[key] = {"docs": []}
        target_source_map = {}
        for record in records:
            entity = None
            if "682" in record["_source"] and "Umlenkung" in str(record["_source"]["682"][0]["__"][0]):
                continue
            for value in getmarcvalues(record["_source"], "079..v" , None):
                entity = value
            if not entity:
                for value in getmarcvalues(record["_source"], "079..b" , None):
                    entity = value
            for key, value in traverse(record["_source"], ""):
                for isil in mapping:
                    if isinstance(value, str) and value.startswith(isil):
                        line = {"_index": mapping[isil]["index"], "_type": mapping[isil]["type"], "_id": value[8:]}
                        mget_bodys[isil]["docs"].append(line)
                        if value not in target_source_map:
                            target_source_map[value] = []
                        target_source_map[value].append({key: {"id":"http://{host}:{port}/{index}/{typ}/".format(host=host,port=port,index=index,typ=doc_type)+record["_source"]["001"], "type":entity, "ts": record["_source"]["005"][0]}})
        for isil in mget_bodys:
            if mget_bodys[isil]["docs"]:
                r = requests.post("http://{host}:{port}/_mget".format(host=mapping[isil]["host"], port=mapping[isil]["port"]), json=mget_bodys[isil], headers=header)
                for doc in r.json().get("docs"):
                    if doc.get("found"):
                        continue
                    else:
                        for obj in target_source_map[isil+doc["_id"]]:
                            for key, base in obj.items():
                                attrib = isil+doc["_id"]
                                comment = ""
                                if isil == "(DE-588)":
                                    deprecated_gnds = oldgnd2newgnd(doc["_id"])
                                    if deprecated_gnds:
                                        comment+="GND ist veraltet, mögliche, aktuelle GND(s): {}".format(";".join(deprecated_gnds))
                                if check_key(key):
                                    ts_swb = get_swb_ts("(DE-627)"+base["id"].split("/")[-1])
                                    if ts_swb != float(base["ts"]):
                                        comment += ";zeitstempel im SWB weicht ab! hier: {} swb: {}".format(base["ts"],ts_swb)
                                    sys.stdout.write("{},{},{},{},{}\n".format(base["id"], str(key.split("'")[1])+str(key.split("'")[-2]), attrib, base["type"],comment))
                                    sys.stdout.flush()
                                        


if __name__ == "__main__":
    run()
