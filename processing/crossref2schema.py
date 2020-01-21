#!/usr/bin/python3

import json
import sys
from es2json import eprint

def handleperson(schemaorg_attr, sourceRecord):
    ret=[]
    schemaorg_name_mapping={"given":"givenName",
                            "family":"familyName",
                            "name":"name",
                            "suffix":"honorifixSuffix",
                            "ORCID":"sameAs",
                            "affiliation":"affiliation"
                            }
    if "author" in sourceRecord:
        for elem in sourceRecord["author"]:
            obj={}
            for source,target in schemaorg_name_mapping.items():
                if source in elem:
                    obj[target]=elem[source]
            if obj:
                ret.append(obj)
    return ret if ret else None

def handleevent(schemaorg_attr, sourceRecord):
    ret=[]
    schemaorg_name_mapping={"startDate":"start",
                            "endDate":"end",
                            "location":"locaton",
                            "name":"name",
                            "alternateName":"acronym",
                            "sponsor":"sponsor",
                            "position":"number",
                            "affiliation":"affiliation"
                            }
    if "event" in sourceRecord:
        eprint(sourceRecord["event"])
        obj={}
        for target,source in schemaorg_name_mapping.items():
            if source in sourceRecord["event"]:
                obj[target]=sourceRecord["event"][source]
        if obj:
            ret.append(obj)
    return ret if ret else None

def handletime(schemaorg_attr, sourceRecord):
    mapping={"dateCreated":"created",
             "dateIssued":"issued",
             "dateModified":"deposited",
             }
    ret=[]
    dateParts=sourceRecord.get(mapping[schemaorg_attr])
    if isinstance(dateParts,dict) and "date-parts" in dateParts:
        for datePart in dateParts.get("date-parts"):
            retDate=""
            for i,date in enumerate(datePart):
                retDate+=str(date)
                if i<2:
                    retDate+="-"
            if i==1:
                retDate+="00"
            elif i==0:
                retDate+="00-00"
            ret.append(retDate)
    return ret if ret else None
    
def getlicense(schemaorg_attr,sourceRecord):
    licenses=[]
    if schemaorg_attr in sourceRecord:
        for elem in sourceRecord.get(schemaorg_attr):
            if elem.get("URL"):
                licenses.append(elem.get("URL"))
    return licenses if licenses else None

def map_record(sourceRecord):
    record={}
    for target_value,source_value in mapping.items():
        if callable(source_value):
            value=source_value(target_value,sourceRecord)
            if value:
                record[target_value]=value
        elif isinstance(source_value,str) and source_value in sourceRecord:
            record[target_value]=sourceRecord[source_value]
    if record:
        record["@context"]="http://schema.org"
    return record

def handlepublisher(schemaorg_attr,sourceRecord):
    publisher={}
    if schemaorg_attr in sourceRecord:
        publisher["name"]=sourceRecord[schemaorg_attr]
        if "publisher-location" in sourceRecord:
            publisher["location"]=sourceRecord["publisher-location"]
        return publisher

def handlecitation(schemaorg_attr,sourceRecord):
    citate=[]
    if "reference" in sourceRecord:
        for elem in sourceRecord["reference"]:
            citation={}
            if "DOI" in elem:
                citation["@id"]=elem["DOI"]
            if "author" in elem:
                citation["author"]=elem["author"]
            if "journal-title" in elem:
                citation["name"]=elem["journal-title"]
            if "first-page" in elem:
                citation["pagination"]=elem["first-page"]
            if "ISSN" in elem:
                citation["issn"]=elem["ISSN"]
            if "ISBN" in elem:
                citation["isbn"]=elem["ISBN"]
            if citation:
                citate.append(citation)
    return citate if citate else None
                
                

def main():
    for line in sys.stdin:
        record=map_record(json.loads(line))
        print(json.dumps(record))


mapping={"url":"URL",
         "@id":"DOI",
         "issn":"ISSN",
         "isbn":"ISBN",
         "author":handleperson,
         "editor":handleperson,
         "translator":handleperson,
         "language":"language",
         "publication":handleevent,
         "isPartOf":"container-title",
         "dateCreated":handletime,
         "dateIssued":handletime,
         "dateModified":handletime,
         "name":"title",
         "pagination":"page",
         "publisher":handlepublisher,
         "description":"abstract",
         "language":"language",
         "genre":"type",
         "about":"subject",
         "license":getlicense,
         "citation":handlecitation,
         }

if __name__ == "__main__":
    main()
