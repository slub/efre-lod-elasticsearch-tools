#!/usr/bin/python3
# -*- coding: utf-8 -*-
from rdflib import URIRef
import traceback
from multiprocessing import Pool, current_process
import elasticsearch
import json
#import urllib.request
import argparse
import sys
import io
import copy
import os.path
import re
import gzip
from es2json import esgenerator, esidfilegenerator, esfatgenerator, ArrayOrSingleValue, eprint, eprintjs, litter, isint
from swb_fix import marc2relation, isil2sameAs

es = None
entities = None
base_id = None
target_id = None
base_id_delimiter = "="
# lookup_es=None


def main():
    """
    Argument Parsing for cli
    """
    parser = argparse.ArgumentParser(
        description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument(
        '-host', type=str, help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port', type=int, default=9200,
                        help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type', type=str, help='ElasticSearch Type to use')
    parser.add_argument('-index', type=str, help='ElasticSearch Index to use')
    parser.add_argument(
        '-id', type=str, help='map single document, given by id')
    parser.add_argument('-help', action="store_true", help="print this help")
    parser.add_argument('-z', action="store_true",
                        help="use gzip compression on output data")
    parser.add_argument('-prefix', type=str, default="ldj/",
                        help='Prefix to use for output data')
    parser.add_argument('-debug', action="store_true",
                        help='Dump processed Records to stdout (mostly used for debug-purposes)')
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-pretty', action="store_true",
                        default=False, help="output tabbed json")
    parser.add_argument('-w', type=int, default=8,
                        help="how many processes to use")
    parser.add_argument('-idfile', type=str,
                        help="path to a file with IDs to process")
    parser.add_argument('-query', type=str, default={},
                        help='prefilter the data based on an elasticsearch-query')
    parser.add_argument('-base_id_src', type=str, default="http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
                        help="set up which base_id to use for sameAs. e.g. http://d-nb.info/gnd/xxx")
    parser.add_argument('-target_id', type=str, default="https://data.slub-dresden.de/",
                        help="set up which target_id to use for @id. e.g. http://data.finc.info")
#    parser.add_argument('-lookup_host',type=str,help="Target or Lookup Elasticsearch-host, where the result data is going to be ingested to. Only used to lookup IDs (PPN) e.g. http://192.168.0.4:9200")
    args = parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    if args.server:
        slashsplit = args.server.split("/")
        args.host = slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port = args.server.split(":")[2].split("/")[0]
        args.index = args.server.split("/")[3]
        if len(slashsplit) > 4:
            args.type = slashsplit[4]
        if len(slashsplit) > 5:
            if "?pretty" in args.server:
                args.pretty = True
                args.id = slashsplit[5].rsplit("?")[0]
            else:
                args.id = slashsplit[5]
    if args.server or (args.host and args.port):
        es = elasticsearch.Elasticsearch([{"host": args.host}], port=args.port)
    global base_id
    global target_id
    base_id = args.base_id_src
    target_id = args.target_id
    if args.pretty:
        tabbing = 4
    else:
        tabbing = None

    if args.host and args.index and args.type and args.id:
        json_record = None
        source = get_source_include_str()
        json_record = es.get_source(
            index=args.index, doc_type=args.type, id=args.id, _source=source)
        if json_record:
            print(json.dumps(process_line(json_record, args.host,
                                          args.port, args.index, args.type), indent=tabbing))
    elif args.host and args.index and args.type and args.idfile:
        setupoutput(args.prefix)
        pool = Pool(args.w, initializer=init_mp, initargs=(
            args.host, args.port, args.prefix, args.z))
        for ldj in esidfilegenerator(host=args.host,
                                     port=args.port,
                                     index=args.index,
                                     type=args.type,
                                     source=get_source_include_str(),
                                     body=args.query,
                                     idfile=args.idfile
                                     ):
            pool.apply_async(worker, args=(ldj,))
        pool.close()
        pool.join()
    elif args.host and args.index and args.type and args.debug:
        init_mp(args.host, args.port, args.prefix, args.z)
        for ldj in esgenerator(host=args.host,
                               port=args.port,
                               index=args.index,
                               type=args.type,
                               source=get_source_include_str(),
                               headless=True,
                               body=args.query
                               ):
            record = process_line(
                ldj, args.host, args.port, args.index, args.type)
            if record:
                for k in record:
                    print(json.dumps(record[k], indent=None))
    elif args.host and args.index and args.type:  # if inf not set, than try elasticsearch
        setupoutput(args.prefix)
        pool = Pool(args.w, initializer=init_mp, initargs=(
            args.host, args.port, args.prefix, args.z))
        for ldj in esfatgenerator(host=args.host,
                                  port=args.port,
                                  index=args.index,
                                  type=args.type,
                                  source=get_source_include_str(),
                                  body=args.query
                                  ):
            pool.apply_async(worker, args=(ldj,))
        pool.close()
        pool.join()
    else:  # oh noes, no elasticsearch input-setup. then we'll use stdin
        eprint("No host/port/index specified, trying stdin\n")
        init_mp("localhost", "DEBUG", "DEBUG", "DEBUG")
        with io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8') as input_stream:
            for line in input_stream:
                ret = process_line(json.loads(
                    line), "localhost", 9200, "data", "mrc")
                if isinstance(ret, dict):
                    for k, v in ret.items():
                        print(json.dumps(v, indent=tabbing))


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


def getiso8601(date):
    """
    Try to transform the parameter date into ISO-6801, but if it fails, return the original date to keep the information
    """
    p = re.compile(r'[\d|X].\.[\d|X].\.[\d|X]*')  # test if D(D).M(M).Y(YYY)
    m = p.match(date)
    datestring = ""
    if m:
        slices = list(reversed(date.split('.')))
        if isint(slices[0]):
            datestring += str(slices[0])
        for slice in slices[1:]:
            if isint(slice):
                datestring += "-"+str(slice)
            else:
                break
        return datestring
    else:
        return date  # was worth a try


def dateToEvent(date, schemakey):
    """
    return birthDate and deathDate schema.org attributes

    don't return deathDate if the person is still alive (determined if e.g. the date looks like "1979-")
    """
    if '-' in date:
        dates = date.split('-')
        if date[0] == '[' and date[-1] == ']':  # oh..
            return str("["+dateToEvent(date[1:-1], schemakey)+"]")
        if "irth" in schemakey:  # (date of d|D)eath(Dates)
            return getiso8601(dates[0])
        elif "eath" in schemakey:  # (date of d|D)eath(Date)
            if len(dates) == 2:
                return getiso8601(dates[1])
            elif len(dates) == 1:
                return None  # still alive! congrats
        else:
            return date


def handlesex(record, key, entity):
    """
    return the determined sex (not gender), found in the MARC21 code
    """
    for v in key:
        marcvalue = getmarc(v, record, entity)
        if isinstance(marcvalue, list):
            marcvalue = marcvalue[0]
    if isint(marcvalue):
        marcvalue = int(marcvalue)
    if marcvalue == 0:
        return "Unknown"
    elif marcvalue == 1:
        return "Male"
    elif marcvalue == 2:
        return "Female"
    else:
        return None


def gnd2uri(string):
    """
    Transforms e.g. (DE-588)1231111151 to an URI .../1231111151
    """
    try:
        if isinstance(string, list):
            for n, uri in enumerate(string):
                string[n] = gnd2uri(uri)
            return string
        if string and "(DE-" in string:
            if isinstance(string, list):
                ret = []
                for st in string:
                    ret.append(gnd2uri(st))
                return ret
            elif isinstance(string, str):
                return uri2url(string.split(')')[0][1:], string.split(')')[1])
    except:
        return


def uri2url(isil, num):
    """
    Transforms a URI like .../1231111151 to https://d-nb.info/gnd/1231111151,
    not only GNDs, also SWB, GBV, configureable over isil2sameAs in swb_fix.py
    """

    if isil and num and isil in isil2sameAs:
        return str(isil2sameAs.get(isil)+num)
    # else:
        # return str("("+isil+")"+num)    #bugfix for isil not be able to resolve for sameAs, so we give out the identifier-number


def id2uri(string, entity):
    """
    return an id based on base_id
    """
    global target_id
    if string.startswith(base_id):
        string = string.split(base_id_delimiter)[-1]
    # if entity=="resources":
    #    return "http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN="+string
    # else:
    if target_id and entity and string:
        return str(target_id+entity+"/"+string)


def getid(record, regex, entity):
    """
    wrapper function for schema.org mapping for id2uri
    """
    _id = getmarc(record, regex, entity)
    if _id:
        return id2uri(_id, entity)


def getisil(record, regex, entity):
    """
    get the ISIL of the record
    """
    isil = getmarc(record, regex, entity)
    if isinstance(isil, str) and isil in isil2sameAs:
        return isil
    elif isinstance(isil, list):
        for item in isil:
            if item in isil2sameAs:
                return item


def getnumberofpages(record, regex, entity):
    """
    get's the number of pages and sanitizes the field
    """
    nop = getmarc(record, regex, entity)
    try:
        if isinstance(nop, str):
            nop = [nop]
        if isinstance(nop, list):
            for number in nop:
                if "S." in number and isint(number.split('S.')[0].strip()):
                    nop = int(number.split('S.')[0])
                else:
                    nop = None
    except IndexError:
        pass
    except Exception as e:
        with open("error.txt", "a") as err:
            print(e, file=err)
    return nop


def getgenre(record, regex, entity):
    """
    gets the genre and builgs a genre node out of it
    """
    genre = getmarc(record, regex, entity)
    if genre:
        return {"@type": "Text",
                "Text": genre}


def getisbn(record, regex, entity):
    """
    get's the ISBN and sanitizes it
    """
    isbns = getmarc(record, regex, entity)
    if isinstance(isbns, str):
        isbns = [isbns]
    elif isinstance(isbns, list):
        for i, isbn in enumerate(isbns):
            if "-" in isbn:
                isbns[i] = isbn.replace("-", "")
            if " " in isbn:
                for part in isbn.rsplit(" "):
                    if isint(part):
                        isbns[i] = part

    if isbns:
        retarray = []
        for isbn in isbns:
            if len(isbn) == 10 or len(isbn) == 13:
                retarray.append(isbn)
        return retarray


def getmarc(record, regex, entity):
    """
    get's the in regex specified attribute from a Marc Record
    """
    if "+" in regex:
        marcfield = regex[:3]
        if marcfield in record:
            subfields = regex.split(".")[-1].split("+")
            data = None
            for array in record.get(marcfield):
                for k, v in array.items():
                    sset = {}
                    for subfield in v:
                        for subfield_code in subfield:
                            sset[subfield_code] = litter(
                                sset.get(subfield_code), subfield[subfield_code])
                    fullstr = ""
                    for sf in subfields:
                        if sf in sset:
                            if fullstr:
                                fullstr += ". "
                            if isinstance(sset[sf], str):
                                fullstr += sset[sf]
                            elif isinstance(sset[sf], list):
                                fullstr += ". ".join(sset[sf])
                    if fullstr:
                        data = litter(data, fullstr)
            if data:
                return ArrayOrSingleValue(data)
    else:
        ret = []
        if isinstance(regex, str):
            regex = [regex]
        for string in regex:
            if string[:3] in record:
                ret = litter(ret, ArrayOrSingleValue(
                    list(getmarcvalues(record, string, entity))))
        if ret:
            if isinstance(ret, list):  # simple deduplizierung via uniq()
                ret = list(uniq(ret))
            return ArrayOrSingleValue(ret)

# generator object to get marc values like "240.a" or "001". yield is used bc can be single or multi


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


def handle_about(jline, key, entity):
    """
    produces schema.org/about: nodes based on RVK, DDC and GND subjects
    """
    ret = []
    for k in key:
        if k == "936" or k == "084":
            data = getmarc(jline, k, None)
            if isinstance(data, list):
                for elem in data:
                    ret.append(handle_single_rvk(elem))
            elif isinstance(data, dict):
                ret.append(handle_single_rvk(data))
        elif k == "082" or k == "083":
            data = getmarc(jline, k+"..a", None)
            if isinstance(data, list):
                for elem in data:
                    if isinstance(elem, str):
                        ret.append(handle_single_ddc(elem))
                    elif isinstance(elem, list):
                        for final_ddc in elem:
                            ret.append(handle_single_ddc(final_ddc))
            elif isinstance(data, dict):
                ret.append(handle_single_ddc(data))
            elif isinstance(data, str):
                ret.append(handle_single_ddc(data))
        elif k == "655":
            data = get_subfield(jline, k, entity)
            if isinstance(data, dict):
                data = [data]
            if isinstance(data, list):
                for elem in data:
                    if elem.get("identifier"):
                        elem["value"] = elem.pop("identifier")
                    ret.append({"identifier": elem})
    if len(ret) > 0:
        return ret
    else:
        return None


def handle_single_ddc(data):
    """
    produces a about node based on DDC
    """
    return {"identifier": {"@type": "PropertyValue",
                           "propertyID": "DDC",
                           "value": data},
            "@id": "http://purl.org/NET/decimalised#c"+data[:3]}


def handle_single_rvk(data):
    """
    produces a about node based on RVK
    """
    sset = {}
    record = {}
    if "rv" in data:
        for subfield in data.get("rv"):
            for k, v in subfield.items():
                sset[k] = litter(sset.get(k), v)
        if "0" in sset and isinstance(sset["0"], str):
            sset["0"] = [sset.get("0")]
        if "0" in sset and isinstance(sset["0"], list):
            record["sameAs"] = []
            for elem in sset["0"]:
                if isinstance(elem, str):
                    sameAs = gnd2uri(elem)
                    if sameAs:
                        record["sameAs"].append(sameAs)
        if "a" in sset:
            record["@id"] = "https://rvk.uni-regensburg.de/api/json/ancestors/" + \
                sset.get("a").replace(" ", "%20")
            record["identifier"] = {"@type": "PropertyValue",
                                    "propertyID": "RVK",
                                    "value": sset.get("a")}
        if "b" in sset:
            record["name"] = sset.get("b")
        if "k" in sset:
            record["keywords"] = sset.get("k")
        return record


def relatedTo(jline, key, entity):
    """
    produces some relatedTo and other nodes based on Marc-Relator Codes
    """
    # e.g. split "551^4:orta" to 551 and orta
    marcfield = key[:3]
    data = []
    if marcfield in jline:
        for array in jline[marcfield]:
            for k, v in array.items():
                sset = {}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code] = litter(
                            sset.get(subfield_code), subfield[subfield_code])
                if isinstance(sset.get("9"), str) and sset.get("9") in marc2relation:
                    node = {}
                    node["_key"] = marc2relation[sset["9"]]
                    if sset.get("0"):
                        uri = gnd2uri(sset.get("0"))
                        if isinstance(uri, str) and uri.startswith(base_id):
                            node["@id"] = id2uri(sset.get("0"), "persons")
                        elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"] = uri
                        elif isinstance(uri, str):
                            node["identifier"] = sset.get("0")
                        elif isinstance(uri, list):
                            node["sameAs"] = None
                            node["identifier"] = None
                            for elem in uri:
                                if elem and isinstance(elem, str) and elem.startswith(base_id):
                                    node["@id"] = id2uri(
                                        elem.split("=")[-1], "persons")
                                elif elem and isinstance(elem, str) and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"] = litter(
                                        node["sameAs"], elem)
                                else:
                                    node["identifier"] = litter(
                                        node["identifier"], elem)
                    if sset.get("a"):
                        node["name"] = sset.get("a")
                    data.append(node)
                elif isinstance(sset.get("9"), list):
                    node = {}
                    for elem in sset["9"]:
                        if elem.startswith("v"):
                            for k, v in marc2relation.items():
                                if k.lower() in elem.lower():
                                    node["_key"] = v
                                    break
                        elif [x for x in marc2relation if x.lower() in elem.lower()]:
                            for x in marc2relation:
                                if x.lower() in elem.lower():
                                    node["_key"] = marc2relation[x]
                        elif not node.get("_key"):
                            node["_key"] = "relatedTo"
                        # eprint(elem,node)
                    if sset.get("0"):
                        uri = gnd2uri(sset.get("0"))
                        if isinstance(uri, str) and uri.startswith(base_id):
                            node["@id"] = id2uri(sset.get("0"), "persons")
                        elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"] = uri
                        elif isinstance(uri, str):
                            node["identifier"] = uri
                        elif isinstance(uri, list):
                            node["sameAs"] = None
                            node["identifier"] = None
                            for elem in uri:
                                if elem and elem.startswith(base_id):
                                    node["@id"] = id2uri(
                                        elem.split("=")[-1], "persons")
                                elif elem and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"] = litter(
                                        node["sameAs"], elem)
                                elif elem:
                                    node["identifier"] = litter(
                                        node["identifier"], elem)
                    if sset.get("a"):
                        node["name"] = sset.get("a")
                    data.append(node)
                    # eprint(node)

        if data:
            return ArrayOrSingleValue(data)


def get_subfield_if_4(jline, key, entity):
    """
    get's subfield of marc-Records and builds some nodes out of them if a clause is statisfied
    """
    # e.g. split "551^4:orta" to 551 and orta
    marcfield = key.rsplit("^")[0]
    subfield4 = key.rsplit("^")[1]
    data = []
    if marcfield in jline:
        for array in jline[marcfield]:
            for k, v in array.items():
                sset = {}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code] = litter(
                            sset.get(subfield_code), subfield[subfield_code])
                if sset.get("4") and subfield4 in sset.get("4"):
                    newrecord = copy.deepcopy(jline)
                    for i, subtype in enumerate(newrecord[marcfield]):
                        for elem in subtype.get("__"):
                            if elem.get("4") and subfield4 != elem["4"]:
                                del newrecord[marcfield][i]["__"]
                    data = litter(get_subfields(
                        newrecord, marcfield, entity), data)
    if data:
        return ArrayOrSingleValue(data)


def get_subfields(jline, key, entity):
    """
    wrapper-function for get_subfield for multi value
    """
    data = []
    if isinstance(key, list):
        for k in key:
            data = litter(data, get_subfield(jline, k, entity))
        return ArrayOrSingleValue(data)
    elif isinstance(key, str):
        return get_subfield(jline, key, entity)
    else:
        return


def handleHasPart(jline, keys, entity):
    """
    adds the hasPart node if a record contains a pointer to another record, which is part of the original record
    """
    data = []
    for key in keys:
        if key == "700" and key in jline:
            for array in jline[key]:
                for k, v in array.items():
                    sset = {}
                    for subfield in v:
                        for subfield_code in subfield:
                            sset[subfield_code] = litter(
                                sset.get(subfield_code), subfield[subfield_code])
                    node = {}
                    if sset.get("t"):
                        entityType = "works"
                        node["name"] = sset["t"]
                        node["author"] = sset["a"]
                        if entityType == "resources" and sset.get("w") and not sset.get("0"):
                            sset["0"] = sset.get("w")
                        if sset.get("0"):
                            if isinstance(sset["0"], list) and entityType == "persons":
                                for n, elem in enumerate(sset["0"]):
                                    if elem and "DE-576" in elem:
                                        sset["0"].pop(n)
                            uri = gnd2uri(sset.get("0"))
                            if isinstance(uri, str) and uri.startswith(base_id) and not entityType == "resources":
                                node["@id"] = id2uri(uri, entityType)
                            elif isinstance(uri, str) and uri.startswith(base_id) and entityType == "resources":
                                node["sameAs"] = base_id + \
                                    id2uri(uri, entityType).split("/")[-1]
                            elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                                node["sameAs"] = uri
                            elif isinstance(uri, str):
                                node["identifier"] = uri
                            elif isinstance(uri, list):
                                node["sameAs"] = None
                                node["identifier"] = None
                                for elem in uri:
                                    if isinstance(elem, str) and elem.startswith(base_id):
                                        # if key=="830":  #Dirty Workaround for finc id
                                            # rsplit=elem.rsplit("=")
                                            # rsplit[-1]="0-"+rsplit[-1]
                                            # elem='='.join(rsplit)
                                        node["@id"] = id2uri(elem, entityType)
                                    elif isinstance(elem, str) and elem.startswith("http") and not elem.startswith(base_id):
                                        node["sameAs"] = litter(
                                            node["sameAs"], elem)
                                    elif elem:
                                        node["identifier"] = litter(
                                            node["identifier"], elem)

                    if node:
                        data = litter(data, node)
                        # data.append(node)
        else:
            node = getmarc(jline, key, entity)
            if node:
                data = litter(data, node)
    if data:
        return ArrayOrSingleValue(data)


def get_subfield(jline, key, entity):
    """
    reads some subfield information and builds some nodes out of them, needs an entity mapping to work

    whenever you call get_subfield add the MARC21 field to the if/elif/else switch/case thingy with the correct MARC21 field->entity mapping
    """
    keymap = {"100": "persons",
              "700": "persons",
              "500": "persons",
              "711": "events",
              "110": "organizations",
              "710": "organizations",
              "551": "geo",
              "689": "topics",
              "550": "topics",
              "551": "geo",
              "655": "topics",
              "830": "resources",
              }
    entityType = keymap.get(key)
    data = []
    if key in jline:
        for array in jline[key]:
            for k, v in array.items():
                sset = {}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code] = litter(
                            sset.get(subfield_code), subfield[subfield_code])
                node = {}
                if sset.get("t"):
                    continue
                for typ in ["D", "d"]:
                    if sset.get(typ):  # http://www.dnb.de/SharedDocs/Downloads/DE/DNB/wir/marc21VereinbarungDatentauschTeil1.pdf?__blob=publicationFile Seite 14
                        node["@type"] = "http://schema.org/"
                        if sset.get(typ) == "p":
                            node["@type"] += "Person"
                            entityType = "persons"
                        elif sset.get(typ) == "b":
                            node["@type"] += "Organization"
                            entityType = "organizations"
                        elif sset.get(typ) == "f":
                            node["@type"] += "Event"
                            entityType = "events"
                        elif sset.get(typ) == "u":
                            node["@type"] += "CreativeWork"
                        elif sset.get(typ) == "g":
                            node["@type"] += "Place"
                        else:
                            node.pop("@type")
                if entityType == "resources" and sset.get("w") and not sset.get("0"):
                    sset["0"] = sset.get("w")
                if sset.get("0"):
                    if isinstance(sset["0"], list) and entityType == "persons":
                        for n, elem in enumerate(sset["0"]):
                            if elem and "DE-576" in elem:
                                sset["0"].pop(n)
                    uri = gnd2uri(sset.get("0"))
                    if isinstance(uri, str) and uri.startswith(base_id) and not entityType == "resources":
                        node["@id"] = id2uri(uri, entityType)
                    elif isinstance(uri, str) and uri.startswith(base_id) and entityType == "resources":
                        node["sameAs"] = base_id + \
                            id2uri(uri, entityType).split("/")[-1]
                    elif isinstance(uri, str) and uri.startswith("http") and not uri.startswith(base_id):
                        node["sameAs"] = uri
                    elif isinstance(uri, str):
                        node["identifier"] = uri
                    elif isinstance(uri, list):
                        node["sameAs"] = None
                        node["identifier"] = None
                        for elem in uri:
                            if isinstance(elem, str) and elem.startswith(base_id):
                                # if key=="830":  #Dirty Workaround for finc id
                                    # rsplit=elem.rsplit("=")
                                    # rsplit[-1]="0-"+rsplit[-1]
                                    # elem='='.join(rsplit)
                                node["@id"] = id2uri(elem, entityType)
                            elif isinstance(elem, str) and elem.startswith("http") and not elem.startswith(base_id):
                                node["sameAs"] = litter(node["sameAs"], elem)
                            elif elem:
                                node["identifier"] = litter(
                                    node["identifier"], elem)
                if isinstance(sset.get("a"), str) and len(sset.get("a")) > 1:
                    node["name"] = sset.get("a")
                elif isinstance(sset.get("a"), list):
                    for elem in sset.get("a"):
                        if len(elem) > 1:
                            node["name"] = litter(node.get("name"), elem)

                if sset.get("v") and entityType == "resources":
                    node["position"] = sset["v"]
                if sset.get("i"):
                    node["description"] = sset["i"]
                if sset.get("n") and entityType == "events":
                    node["position"] = sset["n"]
                for typ in ["D", "d"]:
                    if sset.get(typ):  # http://www.dnb.de/SharedDocs/Downloads/DE/DNB/wir/marc21VereinbarungDatentauschTeil1.pdf?__blob=publicationFile Seite 14
                        node["@type"] = "http://schema.org/"
                        if sset.get(typ) == "p":
                            node["@type"] += "Person"
                        elif sset.get(typ) == "b":
                            node["@type"] += "Organization"
                        elif sset.get(typ) == "f":
                            node["@type"] += "Event"
                        elif sset.get(typ) == "u":
                            node["@type"] += "CreativeWork"
                        elif sset.get(typ) == "g":
                            node["@type"] += "Place"
                        else:
                            node.pop("@type")

                if node:
                    data = litter(data, node)
                    # data.append(node)
        if data:
            return ArrayOrSingleValue(data)



def getsameAs(jline, keys, entity):
    """
    produces sameAs information out of the record
    """
    sameAs = []
    for key in keys:
        data = getmarc(jline, key, entity)
        if isinstance(data, list):
            for elem in data:
                if not "DE-576" in elem:  # ignore old SWB id for root SameAs
                    data = gnd2uri(elem)
                    if data and isinstance(data, str):
                        data=[data]
                    if isinstance(data,list):
                        for elem in data:
                            if elem and elem.startswith("http"):
                                sameAs.append({"@id": elem,
                                                "publisher": {
                                                    "@id": "data.slub-dresden.de",},
                                                "isBasedOn": {
                                                    "@type": "Dataset",
                                                    "@id": "",
                                                }
                                            })
    for n,item in enumerate(sameAs):
        if "d-nb.info" in item["@id"]:
            sameAs[n]["publisher"]["preferredName"]="Deutsche Nationalbibliothek"
            sameAs[n]["publisher"]["@id"]="https://data.slub-dresden.de/organizations/514366265"
        elif "swb.bsz-bw.de" in item["@id"]:
            sameAs[n]["publisher"]["preferredName"]="K10Plus"
            sameAs[n]["publisher"]["@id"]="https://data.slub-dresden.de/organizations/103302212"
    return sameAs


def deathDate(jline, key, entity):
    """
    calls marc_dates with the correct key for a date-mapping
    """
    return marc_dates(jline.get(key), "deathDate")


def birthDate(jline, key, entity):
    """
    calls marc_dates with the correct key for a date-mapping

    """
    return marc_dates(jline.get(key), "birthDate")


def marc_dates(record, event):
    """
    builds the date nodes based on the data which is sanitzed by dateToEvent, gets called by the deathDate/birthDate functions
    """
    recset = {}
    if record:
        for indicator_level in record:
            for subfield in indicator_level:
                sset = {}
                for sf_elem in indicator_level.get(subfield):
                    for k, v in sf_elem.items():
                        if k == "a" or k == "4":
                            sset[k] = litter(sset.get(k), v)
                if isinstance(sset.get("4"), str):
                    sset["4"] = [sset.get("4")]
                if isinstance(sset.get("4"), list):
                    for elem in sset.get("4"):
                        if elem.startswith("dat"):
                            recset[elem] = sset.get("a")
    if recset.get("datx"):
        return dateToEvent(recset["datx"], event)
    elif recset.get("datl"):
        return dateToEvent(recset["datl"], event)
    else:
        return None


def getgeo(arr):
    """
    sanitzes geo-coordinate information from raw MARC21 values
    """
    for k, v in traverse(arr, ""):
        if isinstance(v, str):
            if '.' in v:
                return v

            #key : {"longitude":["034..d","034..e"],"latitude":["034..f","034..g"]}


def getGeoCoordinates(record, key, entity):
    """
    get the geographic coordinates of a place from its Marc21 authority Record
    """
    ret = {}
    for k, v in key.items():
        coord = getgeo(getmarc(record, v, entity))
        if coord:
            ret["@type"] = "GeoCoordinates"
            ret[k] = coord.replace("N", "").replace(
                "S", "-").replace("E", "").replace("W", "-")
    if ret:
        return ret


def getav_katalogbeta(record, key, entity):
    """
    produce a link to the katalogbeta for availability information
    """
    retOffers = list()
    finc_id = getmarc(record, key[1], entity)
    branchCode = getmarc(record, key[0], entity)
    # eprint(branchCode,finc_id)
    if finc_id and isinstance(branchCode, str) and branchCode == "DE-14":
        branchCode = [branchCode]
    if finc_id and isinstance(branchCode, list):
        for bc in branchCode:
            if bc == "DE-14":
                retOffers.append({
                    "@type": "Offer",
                    "offeredBy": {
                        "@id": "https://data.slub-dresden.de/organizations/191800287",
                        "@type": "Library",
                        "name": isil2sameAs.get(bc),
                        "branchCode": "DE-14"
                    },
                    "availability": "https://katalogbeta.slub-dresden.de/id/"+finc_id
                })
    if retOffers:
        return retOffers


def getav(record, key, entity):
    """
    produce a link to the UBLeipzig DAIA/PAIA for availability information
    """
    retOffers = list()
    offers = getmarc(record, [0], entity)
    ppn = getmarc(record, key[1], entity)
    if isinstance(offers, str):
        offers = [offers]
    if ppn and isinstance(offers, list):
        for offer in offers:
            if offer in isil2sameAs:
                retOffers.append({
                    "@type": "Offer",
                    "offeredBy": {
                        "@id": "https://data.finc.info/resource/organisation/"+offer,
                        "@type": "Library",
                        "name": isil2sameAs.get(offer),
                        "branchCode": offer
                    },
                    "availability": "http://data.ub.uni-leipzig.de/item/wachtl/"+offer+":ppn:"+ppn
                })
    if retOffers:
        return retOffers


def removeNone(obj):
    """
    sanitize target records from None Objects
    """
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
                         for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj


def removeEmpty(obj):
    """
    sanitize target records from empty Lists/Objects
    """
    if isinstance(obj, dict):
        toDelete = []
        for k, v in obj.items():
            if v:
                v = ArrayOrSingleValue(removeEmpty(v))
            else:
                toDelete.append(k)
        for key in toDelete:
            obj.pop(key)
        return obj
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, list):
        for elem in obj:
            if elem:
                elem = removeEmpty(elem)
            else:
                del elem
        return obj


def getName(record, key, entity):
    """
    get the name of the record
    """
    data = getAlternateNames(record, key, entity)
    if isinstance(data, list):
        data = " ".join(data)
    return data if data else None


def getAlternateNames(record, key, entity):
    """
    get the alternateName of the record
    """
    data = getmarc(record, key, entity)
    if isinstance(data, str):
        if data[-2:] == " /":
            data = data[:-2]
    elif isinstance(data, list):
        for n, i in enumerate(data):
            if i[-2:] == " /":
                data[n] = i[:-2]
    return data if data else None


def getpublisher(record, key, entity):
    """
    get the publish name and the publish place from two different fields to produce a node out of it
    """
    pub_name = getmarc(record, ["260..b", "264..b"], entity)
    pub_place = getmarc(record, ["260..a", "264..a"], entity)
    if pub_name or pub_place:
        data = {}
        if pub_name:
            if pub_name[-1] in [".", ",", ";", ":"]:
                data["name"] = pub_name[:-1].strip()
            else:
                data["name"] = pub_name
            data["@type"] = "Organization"
        if pub_place:
            if pub_place[-1] in [".", ",", ";", ":"] and isinstance(pub_place, str):
                data["location"] = {"name": pub_place[:-1].strip(),
                                    "type": "Place"}
            else:
                data["location"] = {"name": pub_place,
                                    "type": "Place"}
        return data


def single_or_multi(ldj, entity):
    """
    make Fields single or multi valued according to spec 
    """
    for k in entities[entity]:
        for key, value in ldj.items():
            if key in k:
                if "single" in k:
                    ldj[key] = ArrayOrSingleValue(value)
                elif "multi" in k:
                    if not isinstance(value, list):
                        ldj[key] = [value]
    return ldj


map_entities = {
    "p": "persons",  # Personen, individualisiert
    "n": "persons",  # Personen, namen, nicht individualisiert
    "s": "topics",  # Schlagwörter/Berufe
    "b": "organizations",  # Organisationen
    "g": "geo",  # Geographika
    "u": "works",  # Werktiteldaten
    "f": "events"
}


def getentity(record):
    """
    get the entity type of the described Thing in the record, based on the map_entity table
    """
    zerosevenninedotb = getmarc(record, "079..b", None)
    if zerosevenninedotb in map_entities:
        return map_entities[zerosevenninedotb]
    elif not zerosevenninedotb:
        return "resources"  # Titeldaten ;)
    else:
        return


def getdateModified(record, key, entity):
    """
    get the DateModified field from the Marcrecord,
    date of the last modification of the MarcRecord
    """
    date = getmarc(record, key, entity)
    newdate = ""
    if date:
        for i in range(0, 13, 2):
            if isint(date[i:i+2]):
                newdate += date[i:i+2]
            else:
                newdate += "00"
            if i in (2, 4):
                newdate += "-"
            elif i == 6:
                newdate += "T"
            elif i in (8, 10):
                newdate += ":"
            elif i == 12:
                newdate += "Z"
        return newdate


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


def get_source_include_str():
    """
    get the source_include string when calling the ElasticsSearch Instance,
    so you just get the values you need and save bandwidth
    """
    items = set()
    items.add("079")
    for k, v in traverse(entities, ""):
        # eprint(k,v)
        if isinstance(v, str) and isint(v[:3]) and v not in items:
            items.add(v[:3])
    _source = ",".join(items)
    # eprint(_source)
    return _source


def process_field(record, value, entity):
    """
    process a single field according to the mapping
    """
    ret = []
    if isinstance(value, dict):
        for function, parameter in value.items():
            # Function with parameters defined in mapping
            ret.append(function(record, parameter, entity))
    elif isinstance(value, str):
        return value
    elif isinstance(value, list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record, elem, entity)))
    elif callable(value):
        # Function without paremeters defined in mapping
        return ArrayOrSingleValue(value(record, entity))
    if ret:
        return ArrayOrSingleValue(ret)


# processing a single line of json without whitespace
def process_line(jline, host, port, index, type):
    """
    process a record according to the mapping, calls process_field for every field and adds some context,
    """
    entity = getentity(jline)
    if entity:
        mapline = {}
        for sortkey, val in entities[entity].items():
            key = sortkey.split(":")[1]
            value = ArrayOrSingleValue(process_field(jline, val, entity))
            if value:
                if "related" in key and isinstance(value, dict) and "_key" in value:
                    dictkey = value.pop("_key")
                    mapline[dictkey] = litter(mapline.get(dictkey), value)
                elif "related" in key and isinstance(value, list) and any("_key" in x for x in value):
                    for elem in value:
                        if "_key" in elem:
                            relation = elem.pop("_key")
                            dictkey = relation
                            mapline[dictkey] = litter(
                                mapline.get(dictkey), elem)
                else:
                    mapline[key] = litter(mapline.get(key), value)
        if mapline:
            if "publisherImprint" in mapline:
                mapline["@context"] = list(
                    [mapline.pop("@context"), URIRef(u'http://bib.schema.org/')])
            if "isbn" in mapline:
                mapline["@type"] = URIRef(u'http://schema.org/Book')
            if "issn" in mapline:
                mapline["@type"] = URIRef(
                    u'http://schema.org/CreativeWorkSeries')
            if index:
                mapline["isBasedOn"] = target_id+"source/" + \
                    index+"/"+getmarc(jline, "001", None)
            if isinstance(mapline.get("sameAs"),list):
                for n,sameAs in enumerate(mapline["sameAs"]):
                    mapline["sameAs"][n]["isBasedOn"]["@id"]=mapline["isBasedOn"]
            return {entity: single_or_multi(removeNone(removeEmpty(mapline)), entity)}


def setupoutput(prefix):
    """
    set up the output environment
    """
    if prefix:
        if not os.path.isdir(prefix):
            os.mkdir(prefix)
        if not prefix[-1] == "/":
            prefix += "/"
    else:
        prefix = ""
    for entity in entities:
        if not os.path.isdir(prefix+entity):
            os.mkdir(prefix+entity)


def init_mp(h, p, pr, z):
    """
    initialize the multiprocessing environment for every worker
    """
    global host
    global port
    global prefix
    global comp
    if not pr:
        prefix = ""
    elif pr[-1] != "/":
        prefix = pr+"/"
    else:
        prefix = pr
    comp = z
    port = p
    host = h


def worker(ldj):
    """
    worker function for multiprocessing
    """
    try:
        if isinstance(ldj, dict):
            ldj = [ldj]
        if isinstance(ldj, list):    # list of records
            for source_record in ldj:
                target_record = process_line(source_record.pop(
                    "_source"), host, port, source_record.pop("_index"), source_record.pop("_type"))
                if target_record:
                    for entity in target_record:
                        name = prefix+entity+"/" + \
                            str(current_process().name)+"-records.ldj"
                        if comp:
                            opener = gzip.open
                            name += ".gz"
                        else:
                            opener = open
                        with opener(name, "at") as out:
                            print(json.dumps(
                                target_record[entity], indent=None), file=out)
    except Exception as e:
        with open("errors.txt", 'a') as f:
            traceback.print_exc(file=f)


"""Mapping:
 a dict() (json) like table with function pointers
 entitites={"entity_types:{"single_or_multi:target":"string",
                           "single_or_multi:target":{function:"source"},
                           "single_or_multi:target:function}
                           }
 
"""

entities = {
    "resources": {   # mapping is 1:1 like works
        "single:@type": [URIRef(u'http://schema.org/CreativeWork')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: ["001"]},
        #       "single:offers"                    :{getav:["852..a","980..a"]}, for SLUB and UBL via broken UBL DAIA-API
        # for SLUB via katalogbeta
        "single:offers": {getav_katalogbeta: ["852..a", "001"]},
        "single:_isil": {getisil: ["003", "852..a", "924..b"]},
        "single:_ppn": {getmarc: "001"},
        "single:_sourceID": {getmarc: "980..b"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "single:name": {getName: ["245..a", "245..b"]},
        "single:nameShort": {getAlternateNames: "245..a"},
        "single:nameSub": {getAlternateNames: "245..b"},
        "single:alternativeHeadline": {getAlternateNames: ["245..c"]},
        "multi:alternateName": {getAlternateNames: ["240..a", "240..p", "246..a", "246..b", "245..p", "249..a", "249..b", "730..a", "730..p", "740..a", "740..p", "920..t"]},
        "multi:author": {get_subfields: ["100", "110"]},
        "multi:contributor": {get_subfields: ["700", "710"]},
        "single:publisher": {getpublisher: ["260..a""260..b", "264..a", "264..b"]},
        "single:datePublished": {getmarc: ["260..c", "264..c"]},
        "single:Thesis": {getmarc: ["502..a", "502..b", "502..c", "502..d"]},
        "multi:issn": {getmarc: ["022..a", "022..y", "022..z", "029..a", "490..x", "730..x", "773..x", "776..x", "780..x", "785..x", "800..x", "810..x", "811..x", "830..x"]},
        "multi:isbn": {getisbn: ["020..a", "022..a", "022..z", "776..z", "780..z", "785..z"]},
        "multi:genre": {getgenre: "655..a"},
        "multi:hasPart": {handleHasPart: ["700"]},
        "multi:isPartOf": {getmarc: ["773..t", "773..s", "773..a"]},
        "multi:partOfSeries": {get_subfield: "830"},
        "single:license": {getmarc: "540..a"},
        "multi:inLanguage": {getmarc: ["377..a", "041..a", "041..d", "130..l", "730..l"]},
        "single:numberOfPages": {getnumberofpages: ["300..a", "300..b", "300..c", "300..d", "300..e", "300..f", "300..g"]},
        "single:pageStart": {getmarc: "773..q"},
        "single:issueNumber": {getmarc: "773..l"},
        "single:volumeNumer": {getmarc: "773..v"},
        "multi:locationCreated": {get_subfield_if_4: "551^4:orth"},
        "multi:relatedTo": {relatedTo: "500..0"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
        "multi:description": {getmarc: ["500..a", "520..a"]},
        "multi:mentions": {get_subfield: "689"},
        "multi:relatedEvent": {get_subfield: "711"}
    },
    "works": {
        "single:@type": [URIRef(u'http://schema.org/CreativeWork')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "multi:name": {getmarc: ["100..t", "110..t", "130..t", "111..t"]},
        "single:alternativeHeadline": {getmarc: ["245..c"]},
        "multi:alternateName": {getmarc: ["400..t", "410..t", "411..t", "430..t", "240..a", "240..p", "246..a", "246..b", "245..p", "249..a", "249..b", "730..a", "730..p", "740..a", "740..p", "920..t"]},
        "multi:author": {get_subfield: "500"},
        "multi:contributor": {get_subfield: "700"},
        "single:publisher": {getpublisher: ["260..a""260..b", "264..a", "264..b"]},
        "single:datePublished": {getmarc: ["130..f", "260..c", "264..c", "362..a"]},
        "single:Thesis": {getmarc: ["502..a", "502..b", "502..c", "502..d"]},
        "multi:issn": {getmarc: ["022..a", "022..y", "022..z", "029..a", "490..x", "730..x", "773..x", "776..x", "780..x", "785..x", "800..x", "810..x", "811..x", "830..x"]},
        "multi:isbn": {getmarc: ["020..a", "022..a", "022..z", "776..z", "780..z", "785..z"]},
        "single:genre": {getmarc: "655..a"},
        "single:hasPart": {getmarc: "773..g"},
        "single:isPartOf": {getmarc: ["773..t", "773..s", "773..a"]},
        "single:license": {getmarc: "540..a"},
        "multi:inLanguage": {getmarc: ["377..a", "041..a", "041..d", "130..l", "730..l"]},
        "single:numberOfPages": {getnumberofpages: ["300..a", "300..b", "300..c", "300..d", "300..e", "300..f", "300..g"]},
        "single:pageStart": {getmarc: "773..q"},
        "single:issueNumber": {getmarc: "773..l"},
        "single:volumeNumer": {getmarc: "773..v"},
        "single:locationCreated": {get_subfield_if_4: "551^orth"},
        "single:relatedTo": {relatedTo: "500..0"}
    },
    "persons": {
        "single:@type": [URIRef(u'http://schema.org/Person')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:name": {getmarc: "100..a"},
        "single:gender": {handlesex: "375..a"},
        "multi:alternateName": {getmarc: ["400..a", "400..c"]},
        "multi:relatedTo": {relatedTo: "500..0"},
        "multi:hasOccupation": {get_subfield: "550"},
        "single:birthPlace": {get_subfield_if_4: "551^ortg"},
        "single:deathPlace": {get_subfield_if_4: "551^orts"},
        "single:workLocation": {get_subfield_if_4: "551^ortw"},
        "multi:honorificSuffix": {get_subfield_if_4: "550^adel"},
        "multi:honorificSuffix": {get_subfield_if_4: "550^akad"},
        "single:birthDate": {birthDate: "548"},
        "single:deathDate": {deathDate: "548"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
    "organizations": {
        "single:@type": [URIRef(u'http://schema.org/Organization')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:name": {getmarc: "110..a+b"},
        "multi:alternateName": {getmarc: "410..a+b"},

        "single:additionalType": {get_subfield_if_4: "550^obin"},
        "single:parentOrganization": {get_subfield_if_4: "551^adue"},
        "single:location": {get_subfield_if_4: "551^orta"},
        "single:fromLocation": {get_subfield_if_4: "551^geoa"},
        "single:areaServed": {get_subfield_if_4: "551^geow"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
    "geo": {
        "single:@type": [URIRef(u'http://schema.org/Place')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:name": {getmarc: "151..a"},
        "multi:alternateName": {getmarc: "451..a"},
        "single:description": {get_subfield: "551"},
        "single:geo": {getGeoCoordinates: {"longitude": ["034..d", "034..e"], "latitude": ["034..f", "034..g"]}},
        "single:adressRegion": {getmarc: "043..c"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
    "topics": {
        "single:@type": [URIRef(u'http://schema.org/Thing')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},
        "single:name": {getmarc: "150..a"},
        "multi:alternateName": {getmarc: "450..a+x"},
        "single:description": {getmarc: "679..a"},
        "multi:additionalType": {get_subfield: "550"},
        "multi:location": {get_subfield_if_4: "551^orta"},
        "multi:fromLocation": {get_subfield_if_4: "551^geoa"},
        "multi:areaServed": {get_subfield_if_4: "551^geow"},
        "multi:contentLocation": {get_subfield_if_4: "551^punk"},
        "multi:participant": {get_subfield_if_4: "551^bete"},
        "multi:relatedTo": {get_subfield_if_4: "551^vbal"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
    "events": {
        "single:@type": [URIRef(u'http://schema.org/Event')],
        "single:@context": "http://schema.org",
        "single:@id": {getid: "001"},
        "single:identifier": {getmarc: "001"},
        "single:_isil": {getisil: "003"},
        "single:dateModified": {getdateModified: "005"},
        "multi:sameAs": {getsameAs: ["035..a", "670..u"]},

        "single:name": {getmarc: ["111..a"]},
        "multi:alternateName": {getmarc: ["411..a"]},
        "single:location": {get_subfield_if_4: "551^ortv"},
        "single:startDate": {birthDate: "548"},
        "single:endDate": {deathDate: "548"},
        "single:adressRegion": {getmarc: "043..c"},
        "multi:about": {handle_about: ["936", "084", "083", "082", "655"]},
    },
}

if __name__ == "__main__":
    main()
