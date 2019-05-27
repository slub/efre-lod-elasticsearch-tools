#!/usr/bin/python3

import argparse
import sys
import json

from es2json import ArrayOrSingleValue, eprint
from fincsolr2marc import fixRecord
from pymarc import MARCReader


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

def getLanguage(record,prop):
    lang=getProperty(record,prop)
    if lang:
        language={"en":lang}
        return language
    
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
    formatmapping={     "Article, E-Article":"bibo:Article",           
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
        return {"@id":context.get(value)}
                   

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
           "@type": "http://schema.org/Offer",
           "schema:offeredBy": {
                "@id": "https://data.finc.info/organisation/DE-15",
                "@type": "schema:Library",
                "schema:name": "Univeristätsbibliothek Leipzig",
                "schema:branchCode": "DE-15"
            },
           "schema:availability": "http://data.ub.uni-leipzig.de/item/wachtl/DE-15:ppn:"+record[prop]
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

def getIsPartOf(record,prop):
    data=getProperty(record,prop)
    if isinstance(data,str):
        return {"@id":"https://data.finc.info/resources/"+data}
    elif isinstance(data,list):
        ret=[]
        for elem in data:
            ret.append({"@id":"https://data.finc.info/resources/"+elem})
        return ret
    
def getIssued(record,prop):
    data=getProperty(record,prop)
    if isinstance(data,str):
        return {context.get("dateTime"):data}
    elif isinstance(data,list):
        ret=[]
        for elem in data:
            ret.append({"@type": "xsd:gYear",
                        "@value":elem})
        return ret

"""...
  "contribution" : [ {
    "type" : [ "Contribution" ],
    "agent" : {
      "id" : "http://d-nb.info/gnd/1049709292",
      "type" : [ "Person" ],
      "dateOfBirth" : "1974",
      "gndIdentifier" : "1049709292",
      "label" : "Nichols, Catherine" 
    },
    "role" : {
      "id" : "http://id.loc.gov/vocabulary/relators/edt",
      "label" : "Herausgeber/in" 
    }
  }, {
    "type" : [ "Contribution" ],
    "agent" : {
      "id" : "http://d-nb.info/gnd/130408026",
      "type" : [ "Person" ],
      "dateOfBirth" : "1951",
      "gndIdentifier" : "130408026",
      "label" : "Blume, Eugen" 
    },
    "role" : {
      "id" : "http://id.loc.gov/vocabulary/relators/ctb",
      "label" : "Mitwirkende" 
    }
  }
"""
def get_contributon(record,prop):
    fullrecord_fixed=fixRecord(record=getProperty(record,prop),record_id=record.get("record_id"),validation=False,replaceMethod='decimal')
    reader=MARCReader(fullrecord_fixed.encode('utf-8'))
    data=[]
    fields=["100","110","111","700","710","711"]
    for record in reader:
        for field in fields:
            for f in record.get_fields(field):
                contributor = {
                    "@type" : [ "bf:Contribution" ],
                    "bf:agent" : {
                    "@id" : "http://d-nb.info/gnd/"
                    },
                    "bf:role" : {
                        "@id" : "http://id.loc.gov/vocabulary/relators/",
                        }
                }
                if f['a']:
                    contributor["bf:agent"]["https://www.w3.org/TR/rdf-schema/#ch_label"]=f['a']
                if f['0'] and f['0'].startswith("(DE-588)"):
                    contributor["bf:agent"]["gndIdentifier"]=f['0'].split(")")[1]
                    contributor["bf:agent"]["@id"]+=contributor["bf:agent"]["gndIdentifier"]
                else:
                    del contributor['bf:agent']['@id']
                if f['4']:
                    contributor['bf:role']['@id']+=f['4']
                else:
                    del contributor['bf:role']
                if contributor['bf:agent'].get('https://www.w3.org/TR/rdf-schema/#ch_label'):
                    data.append(contributor)
    return data if data else None


def putContext(record):
    return context

# mapping={ "target_field":"someString"},

#           "target_field":{function:"source_field"}}

context={
    "xsd":"http://www.w3.org/2001/XMLSchema#",
    "bf":"http://id.loc.gov/ontologies/bibframe/",
    "dct":"http://purl.org/dc/terms/",
    "dc":"http://purl.org/dc/terms/",
    "bibo":"http://purl.org/ontology/bibo/",
    "rdau":"http://rdaregistry.info/Elements/u/",
    "umbel":"http://umbel.org/umbel/",
    "isbd":"http://iflastandards.info/ns/isbd/elements/",
    "schema":"http://schema.org/",
    "bf":"http://id.loc.gov/ontologies/bibframe",
    "issued":{
        "@id": "dct:issued",
        "@type": "xsd:gYear"
    },
    "identifier":{
        "@id":"dct:identifier",
        "@type":"xsd:string"
    },
    "language":{
        "@id":"http://purl.org/dc/terms/language",
        "@container":"@language"
    },
    "openAccessContent":"http://dbpedia.org/ontology/openAccessContent",
}


mapping = { 
          "@context":putContext,
          "@id":{getAtID:"id"},
          "identifier":{getIDs:["swb_id_str","kxp_id_str"]},
          "bibo:issn":{getProperty:"issn"},
          "bibo:isbn":{getProperty:"isbn"},
          "umbel:isLike":{getProperty:"url"},
          "dct:title":{getTitle:"title"},
          "rdau:P60493":{getTitle:["title_part","title_sub"]},
          "bibo:shortTitle":{getTitle:"title_short"},
          "dct:alternative":{getTitle:"title_alt"},
          #"rdau:P60327":{getProperty:"author"},
          #"dc:contributor":{getProperty:"author2"},
          #"author_id":{getGND:"author_id"},
          "rdau:P60333":{getProperty:"imprint_str_mv"},
          "rdau:P60163":{getProperty:"publishPlace"},
          "dct:publisher":{getProperty:"publisher"},
          "issued":{getIssued:"publishDate"},
          "rdau:P60489":{getProperty:"dissertation_note"},
          "isbd:P1053":{getProperty:"physical"},
          "language":{getLanguage:"language"},
          "dct:isPartOf":{getIsPartOf:"hierarchy_top_id"},
          "dct:bibliographicCitation":{getProperty:["container_title","container_reference"]},
          "https://www.w3.org/TR/rdf-schema/#ch_type":{getFormatRdfType:"format_finc"},
          "dct:medium":{getFormatDctMedium:"format_finc"},
          "openAccessContent":{getoAC:"facet_avail"},
          "schema:offers": {getOfferedBy:"record_id"},
          "bf:contribution":{get_contributon:"fullrecord"}
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
        value=process_field(record,val)
        if value:
            mapline[key]=value
    mapline=removeNone(mapline)
    if mapline:
        return mapline
    else:
        return None
    
def main():
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-gen_cmd',action="store_true",help='generate bash command')
    parser.add_argument('-server',type=str,help="which server to use for harvest, only used for cmd prompt definition")
    args=parser.parse_args()
    if args.gen_cmd:
        fl=set()
        for k,v in mapping.items():
            for c,w in v.items():
                if isinstance(w,str):
                    fl.add(w)
                elif isinstance(w,list):
                    for elem in w:
                        fl.add(elem)
        print("solrdump -verbose -server {} -q institution:DE-15 -fl {}".format(args.server,','.join(fl)))
        quit()
    for line in sys.stdin:
        target_record=process_line(json.loads(line))
        if target_record:
            print(json.dumps(target_record))

if __name__ == "__main__":
    main()
