#!/usr/bin/python3

import argparse
import sys
import json

from es2json import ArrayOrSingleValue

baseuri="http://data.finc.info/resources/"


prop2isil={"swb_id_str":"(DE-576)",
           "kxp_id_str":"(DE-627)"
               }

def getIDs(record,prop):
    if isinstance(prop,str):
        if prop in prop2isil and prop in record:
            return str(prop2isil[prop]+record[prop])
        elif prop in record and not prop in prop2isil:
            return str(record[prop])
    elif isinstance(prop,list):
        ret=[]
        for elem in prop:
            if elem in prop2isil and elem in record:
                ret.append(str(prop2isil[elem]+record[elem]))
            elif elem in record and not elem in prop2isil:
                ret.append(record[elem])
        if ret:
            return ret

def getoAC(record,prop):
    if isinstance(record.get(prop),str):
        if record.get(prop)=="Free":
            return "Yes"
    elif isinstance(record.get(prop),list):
        for elem in record.get(prop):
            if elem=="Free":
                return "Yes"
            
def getAtID(record,prop):
    if record.get(prop):
        return baseuri+record[prop]
        
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

def getTitle(record,prop):
    title=getProperty(record,prop)
    if title:
        if isinstance(title,str):
            if title[-2:]==" /":
                title=title[:-2]
        elif isinstance(title,list):
            for n, elem in enumerate(title):
                if elem[-2:]==" /":
                    title[n]=title[n][:-2]
        return title

def getformat(record,prop,formattable):
    if isinstance(record.get(prop),str) and record.get(prop) in formattable:
            return formattable.get(record.get(prop))
    elif isinstance(record.get(prop),list):
        for elem in record.get(prop):
            if elem in formattable:
                return formattable.get(elem)

def getFormatRdfType(record,prop):
    formatmapping={ "Article, E-Article":"bibo:Article",           
                        "Book, E-Book":"bibo:Book",
                        "Journal, E-Journal":"bibo:Periodical",
                        "Manuscript":"bibo:Manuscript",
                        "Map":"bibo:Map",
                        "Thesis":"bibo:Thesis",
                        "Unknown Format":"bibo:Document",
                        "Video":"bibo:AudioVisualDocument"
                            }
    value=getformat(record,prop,formatmapping)
    if value:
        return value
                   

def getFormatDctMedium(record,prop):
    formatmapping={"Audio":"rdamt:1001",
                         "Microform":"rdamt:1002",
                         "Notated Music":"rdau:P60488"
                             }
    value=getformat(record,prop,formatmapping)
    if value:
        return value

def getOfferedBy(record,prop):
        if record.get(prop):
            return {
           "@type": "Offer",
           "offeredBy": {
                "@id": "https://data.finc.info/resource/organisation/DE-15",
                "@type": "Library",
                "name": "Univeristätsbibliothek Leipzig",
                "branchCode": "DE-15"
            },
           "availability": "http://data.ub.uni-leipzig.de/item/wachtl/DE-15:ppn:"+record[prop]
       }

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

context={ "dct:identifier":"http://purl.org/dc/terms/dct:identifier",
          "bibo:issn":"http://purl.org/ontology/bibo/issn",
          "bibo:isbn":"http://purl.org/ontology/bibo/isbn",
          "umbel:isLike":"http://umbel.org/umbel/isLike",
          "dcterms:title":"http://purl.org/dc/terms/title",
          "rdau:P60493":"http://rdaregistry.info/Elements/u/P60493",
          "bibo:shortTitle":"http://purl.org/ontology/bibo/shortTitle",
          "dcterms:alternative":"http://purl.org/dc/elements/1.1/alternative",
          "dcterms:creator":"http://purl.org/dc/elements/1.1/creator",
          "dcterms:contributor":"http://purl.org/dc/elements/1.1/contributor",
          "dcterms:creator":"http://purl.org/dc/elements/1.1/creator",
          "rdau:P60333":"http://rdaregistry.info/Elements/u/P60333",
          "rdau:P60163":"http://rdaregistry.info/Elements/u/P60163",
          "dc:publisher":"http://purl.org/dc/terms/publisher",
          "dctermes:issued":"http://purl.org/dc/terms/issued",
          "rdau:P60489":"http://rdaregistry.info/Elements/u/P60489",
          "isbd:P1053":"http://iflastandards.info/ns/isbd/elements/P1053",
          "dct:language":"http://purl.org/dc/terms/language",
          "dct:isPartOf":"http://purl.org/dc/terms/isPartOf",
          "dct:bibliographicCitation":"http://purl.org/dc/terms/bibliographicCitation",
          "openAccessContent":"http://dbpedia.org/ontology/openAccessContent",
          "offeredBy":"http://schema.org/offeredBy"
          }

mapping={ "@id":{getAtID:"id"},
          "dct:identifier":{getIDs:["record_id","swb_id_str","kxp_id_str","source_id"]},
          "bibo:issn":{getProperty:"issn"},
          "bibo:isbn":{getProperty:"isbn"},
          "umbel:isLike":{getProperty:"url"},
          "dcterms:title":{getTitle:"title"},
          "rdau:P60493":{getTitle:["title_part","title_sub"]},
          "bibo:shortTitle":{getTitle:"title_short"},
          "dcterms:alternative":{getTitle:"title_alt"},
          "dcterms:creator":{getProperty:"author"},
          "dcterms:contributor":{getProperty:"author2"},
          "dcterms:creator":{getGND:"author_id"},
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
          "rdf:type":{getFormatRdfType:"format_de15"},
          "dct:medium":{getFormatDctMedium:"format_de15"},
          "openAccessContent":{getoAC:"facet_avail"},
           "offeredBy": {getOfferedBy:"record_id"},
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
        mapline["@context"]=context
        return mapline
    else:
        return None
    
def main():
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-gen_cmd',action="store_true",help='generate bash command')
    args=parser.parse_args()
    if args.gen_cmd:
        fl=""
        for k,v in mapping.items():
            for c,w in v.items():
                if isinstance(w,str):
                    fl+=w+","
                elif isinstance(w,list):
                    for elem in w:
                        fl+=elem+","
        fl=fl[:-1]
        print("solrdump -verbose -server https://index.ubl-proxy.slub-dresden.de/solr/biblio/ -q institution:DE-15 -fl {} | {}".format(fl,sys.argv[0]))
        quit()
    for line in sys.stdin:
        target_record=process_line(json.loads(line))
        if target_record:
            print(json.dumps(target_record))

if __name__ == "__main__":
    main()
