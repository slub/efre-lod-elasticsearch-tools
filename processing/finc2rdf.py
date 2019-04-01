#!/usr/bin/python3

import sys
import json

from es2json import ArrayOrSingleValue


def getGND(record,prop):
    ret=[]
    if isinstance(record.get(prop),str):
        return "http://d-nb.info/gnd/"+record.get(prop)
    elif isinstance(record.get(prop),list):
        for elem in record.get(prop):
            ret.append("http://d-nb.info/gnd/"+elem)
    if ret:
        return ret
    else:
        return None


def getProperty(record,prop):
    ret=[]
    if isinstance(prop,str):
        if prop in record:
            return record.get(prop)
    elif isinstance(prop,list):
        for elem in prop:
            if isinstance(record.get(elem),str):
                ret.append(record[elem])
            elif isinstance(record.get(elem),list):
                for elen in record[elem]:
                    ret.append(elen)
    if ret:
        return ret
    else:
        return None

# mapping={ "target_field":"someString"},

#           "target_field":{function:"source_field"}}
mapping={ "dct:identifier":{getProperty:["record_id","swb_id_str","kxp_id_str","source_id"]},
          "bibo:issn":{getProperty:"issn"},
          "bibo:isbn":{getProperty:"isbn"},
          "umbel:isLike":{getProperty:"url"},
          "dcterms:title":{getProperty:"title"},
          "rdau:P60493":{getProperty:["title_part","title_sub"]},
          "bibo:shortTitle":{getProperty:"title_short"},
          "dcterms:alternative":{getProperty:"title_alt"},
          "dcterms:creator":{getProperty:"author"},
          "dcterms:contributor":{getProperty:"author2"},
          "dcterms:creator":{getGND:"author_id"},
          #"dcterms:title":{getProperty,"title"}
          "rdau:P60333":{getProperty:"imprint"},
          "rdau:P60163":{getProperty:"publishPlace"},
          "dc:publisher":{getProperty:"publisher"},
          "dctermes:issued":{getProperty:"publishDate"},
          "rdau:P60489":{getProperty:"dissertation_note"},
          "isbd:P1053":{getProperty:"physical"},
          "dct:language":{getProperty:"language"},
          "dct:isPartOf":{getProperty:"hierarchy_top_id"},
          "dct:bibliographicCitation":{getProperty:["container_title","container_reference"]},
          "dct:isPartOf":{getProperty:"hierarchy_parent_id"},
          "rdf:type":{getProperty:"format_finc"},
          "http://dbpedia.org/ontology/openAccessContent":{getProperty:"facet_avail"}
          }

def process_field(record,source_field):
    ret=[]
    if isinstance(source_field,dict):
        for function,parameter in source_field.items():
            ret.append(function(record,parameter))
    elif isinstance(source_field,str):
        return value
    elif isinstance(source_field,list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record,elem)))
    elif callable(source_field):
        return ArrayOrSingleValue(source_field(record))
    if ret:
        return ArrayOrSingleValue(ret)

def removeNone(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
            for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj

def process_line(record):
    mapline={}
    for key,val in mapping.items():
        #key=sortkey.split(":")[1]
        value=process_field(record,val)
        if value:
            mapline[key]=value
    mapline=removeNone(mapline)
    if mapline:
        return mapline
    else:
        return None
    
def main():
    for line in sys.stdin:
        target_record=process_line(json.loads(line))
        if target_record:
            print(json.dumps(target_record))

if __name__ == "__main__":
    main()
