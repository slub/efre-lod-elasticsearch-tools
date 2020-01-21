#!/usr/bin/python3

import sys
import json
from es2json import eprint,litter,isint

def getNestedJsonObject(record,attribut_string):
    attr_list=attribut_string.split(">")
    if len(attr_list)==1 and attr_list[0] in record:
        return record.get(attr_list[0])
    elif len(attr_list)>1 and attr_list[0] in record:
        return getNestedJsonObject(record[attr_list[0]],">".join(attr_list[1:]))
    else:
        return None
    
def maketitleobj(obj):
    name_obj={}
    if obj.get("title"):
        name_obj["@value"]=obj.get("title")
    name_obj["@type"]="schema:Text"
    if obj.get("lang"):
        name_obj["@language"]=obj["lang"]
    if obj.get("subTitle"):
        name_obj["description"]=obj["subTitle"]
        
    return name_obj

def handleid(obj,id):
    if isinstance(obj,dict) and obj.get("type")==id:
        return obj.get("_")
    return None

def getUrn(attr,rec):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>identifier"
    item=getNestedJsonObject(rec,path)
    if isinstance(item,dict):
        urn=handleid(item,"qucosa:urn")
        return (attr,"http://nbn-resolving.de/"+urn) if urn else (attr,None)
    elif isinstance(item,list):
        for elem in item:
            urn=handleid(elem,"qucosa:urn")
            if urn:
                return (attr,"http://nbn-resolving.de/"+urn)
    return None,None

def getSameAs(attr,rec):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>identifier"
    item=getNestedJsonObject(rec,path)
    if isinstance(item,dict):
        urn=handleid(item,"swb-ppn")
        if urn and len(urn)==10:
            return (attr,"data.slub-dresden.de/slub-resources/"+urn)
    elif isinstance(item,list):
        for elem in item:
            urn=handleid(elem,"swb-ppn")
            if urn and len(urn)==10:
                return (attr,"data.slub-dresden.de/slub-resources/"+urn)
    return None,None


def traverse(dict_or_list, path):
    iterator=None
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list, list):
        iterator = enumerate(dict_or_list)
    elif isinstance(dict_or_list,str):
        strarr=[]
        strarr.append(dict_or_list)
        iterator=enumerate(strarr)
    else:
        return
    if iterator:
        for k, v in iterator:
            if not isinstance(v,list):
                if isint(k):
                    yield path, v
                else:
                    yield path + str(k), v
            if isinstance(v, (dict, list)) and isint(k):
                for k,v in traverse(v,path):
                    yield k, v
            elif isinstance(v, (dict, list)):
                for k, v in traverse(v, path + str(k)):
                    yield k, v

def handleIdentifiers(obj):
    if "typeURI" in obj:
            return obj["typeURI"]+"/"+obj.get("_")
    elif obj.get("_") and obj.get("_").startswith("http"):
            return obj.get("_")
    elif not "typeURI" in obj and isinstance(obj.get("type"),str) and obj.get("type").lower()=='gnd':
        return "http://d-nb.info/gnd/"+obj.get("_")
    elif not "typeURI" in obj and not "type" in obj and obj.get("authority").lower()=="gnd":
        return "http://d-nb.info/gnd/"+obj.get("_")
    else:
        eprint(obj)
        return None

def handleRoles(obj,rec):
    retobj={}
    key=""
    #eprint(json.dumps(ob))
    role=""
    kv_table={"given":"givenName",
              "family":"familyName",
              "termsOfAddress":"honorificPrefix",
              'ID':'@id',
              'namePart':'name',
              "date":"birthDate",
              "aut":"author",
              "edt":"contributor",
              "pbl":"publisher",
              "dgg":"sourceOrganisation",
              "prv":"provider",
              "rev":"contributor",
              "dgs":"contributor",
              "ctb":"contributor",
              "oth":"contributor",
              "red":"contributor",
              "ill":"illustrator",
              "fnd":"funder",
              "cmp":"composer",
              "ths":"instructor",
              "sad":"contributor",
              "trl":"translator",
              "art":"artist",
              "Den akademischen Grad verleihende / prüfende Institution":"sourceOrganisation",
              'Den akademischen Grad verleihende Institution':"sourceOrganisation",
              #'Medizinische Fakultät':"sourceOrganisation",
              #'Universität Leipzig':"sourceOrganisation",
              'Den akademischen Grad verleihende/prüfende Institution':"sourceOrganisation",
              }
    if isinstance(obj.get("role"),dict):
        for k,v in traverse(obj["role"],""):
            if k=="roleTerm" and v.get("_"):
                if v["_"].strip() in kv_table:      # using strip() to avoid this nonesense: e.g.: '_': '\n                \n                prv\n              '
                    role=kv_table[v["_"].strip()]
                else:
                    pass
                    #_bla,_blubb = getUrn("None",rec)
                    #eprint(_blubb)
    if role:
        retobj[role]={}
        for key in obj:
            if key in kv_table and isinstance(obj[key],str):
                retobj[role][kv_table[key]]=obj[key]
            if key in kv_table and isinstance(obj[key],list):
                for elem in obj[key]:
                    if elem.get("type") in kv_table:
                        retobj[role][kv_table[elem["type"]]]=elem.get("_")
                    else:
                        pass
        if isinstance(obj.get("nameIdentifier"),list):
            for elem in obj["nameIdentifier"]:
                retobj[role]["sameAs"]=litter(retobj.get("sameAs"),handleIdentifiers(elem))
        elif isinstance(obj.get("nameIdentifier"),dict):
            retobj[role]["sameAs"]=litter(retobj.get("sameAs"),handleIdentifiers(obj.get("nameIdentifier")))
        if retobj[role].get("givenName") and retobj[role].get("familyName"):
            retobj[role]["name"]=retobj[role].pop("familyName")+", "+retobj[role].pop("givenName")
        #if obj.get("role") and obj["role"].get("roleTerm") and isinstance(obj["role"]["roleTerm"],dict) and obj["role"]["roleTerm"]["_"]=="aut":
            #key=="author"
        #elif obj.get("role") and obj["role"].get("roleTerm") and isinstance(obj["role"]["roleTerm"],dict) and obj["role"]["roleTerm"]["_"]=="prv":
            #key=="provider"
        #eprint(retobj)
        return retobj


def handlePerson(attr,rec):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>name"
    items=getNestedJsonObject(rec,path)
    if isinstance(items,dict):
        return ("__array__",handleRoles(items,rec))
    elif isinstance(items,list):
        ret=[]
        for item in items:
            ret_obj=handleRoles(item,rec)
            if ret_obj:
                ret.append(ret_obj)
        #eprint(ret)
        return ("__array__",ret) if ret else (attr,None)
    return None,None

def handleRelated(attr,rec):
    data=getNestedJsonObject(rec,"metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>relatedItem")
    ret=[]
    if isinstance(data,list):
        for elem in data:
            retobj={}
            if elem.get("type")=="host":
                role="isPartOf"
            elif elem.get("type")=="original" and elem.get("note"):
                role="pagination"
                retobj[role]=elem["note"].get("_")
                if retobj[role]:
                    ret.append(retobj)
                    continue
            else:
                continue
            retobj[role]={}
            if elem.get("titleInfo") and elem["titleInfo"].get("title"):
                retobj[role]["name"]=elem["titleInfo"]["title"]
            if elem.get("identifier") and isinstance(elem["identifier"],list):
                for _id in elem["identifier"]:
                    if _id["type"]=="urn":
                        retobj[role]["@id"]="http://nbn-resolving.de/"+_id["_"]
            if retobj[role]:
                ret.append(retobj)
            
        return "__array__",ret
    return None,None

def handletitle(attribut,record):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>titleInfo"
    titleobj=getNestedJsonObject(record,path)
    if isinstance(titleobj,dict):
        return (attribut,[maketitleobj(titleobj)])
    elif isinstance(titleobj,list):
        ret=[]
        for elem in titleobj:
            ret.append(maketitleobj(elem))
        return (attribut,ret)
    return None,None

def handledateIssued(attribut,record):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>originInfo"
    titleobj=getNestedJsonObject(record,path)
    if isinstance(titleobj,list):
        for elem in titleobj:
            if elem.get("eventType")=="publication" and elem.get("dateIssued") and elem["dateIssued"].get("_"):
                return (attribut,elem["dateIssued"]["_"])
    elif isinstance(titleobj,dict):
        if titleobj.get("eventType")=="publication" and titleobj.get("dateIssued") and titleobj["dateIssued"].get("_"):
            return (attribut,titleobj["dateIssued"]["_"])
    return (None, None)
    
def handlelanguage(attribut,record):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>language>languageTerm>_"
    titleobj=getNestedJsonObject(record,path)
    return (attribut,titleobj) if titleobj else (None, None)
    
def handleaboutelem(attribut,obj):
    retobj=[]
    if obj.get("authority").lower()=="rvk" and obj.get("_"):
        for rvk in obj["_"].split(","):
            retobj=litter(retobj,{"@id":"https://rvk.uni-regensburg.de/api/json/ancestors/"+rvk.replace(" ","%20").strip(),
                       "identifier":{  "@type"     :"PropertyValue",
                                    "propertyID":"RVK",
                                "@value"     :rvk.strip()}})
    elif obj.get("authority").lower()=="ddc" and obj.get("_"):
        for ddc in obj.get("_").split(","):
            retobj=litter(retobj,{"identifier":{"@type"     :"PropertyValue",
                                                "propertyID":"DDC",
                                                "@value"     :ddc.strip()},
                                                "@id":"http://purl.org/NET/decimalised#c"+ddc.strip()[:3]})
    elif obj.get("authority").lower()=="z" and obj.get("_"):
        newObj={"@value":obj.get("_"),
                       "@type":"schema:Text"}
        if obj.get("lang"):
            newObj["@language"]=obj["lang"]
        retobj=litter(retobj,newObj)
        
    elif obj.get("authority").lower()=="sswd" and obj.get("_"):
        retobj=litter(retobj,{"@value":obj.get("_"),
                       "@type":"schema:Text",
                       "@language":"ger"})
    return retobj if retobj else None

def handleabout(attribut,record):
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>classification"
    titleobj=getNestedJsonObject(record,path)
    retobj=[]
    if isinstance(titleobj,list):
        for elem in titleobj:
            retobj=litter(retobj,handleaboutelem(attribut,elem))
    elif isinstance(titleobj,dict):
            retobj=litter(retobj,handleaboutelem(attribut,titleobj))
    return (attribut,retobj) if retobj else (None, None)
    
def handleabstract(attribut,record):
    mapping={"@value":"_",
             "@language":"lang",
             }
    path="metadata>mets:mets>mets:dmdSec>mets:mdWrap>mets:xmlData>mods>abstract"
    abstractobj=getNestedJsonObject(record,path)
    retobj={"@type":"schema:Text"}
    for k,v in mapping.items():
        if abstractobj and v in abstractobj:
            retobj[k]=abstractobj[v]
    return (attribut,retobj) if retobj.get("@value") else (None, None)

def handlemetsfile(elem):
    retobj=None
    if elem["mets:file"].get("mets:FLocat") and elem["mets:file"]["mets:FLocat"].get("xlin:href"):
        retobj={"contentUrl":elem["mets:file"]["mets:FLocat"]["xlin:href"]}
        if elem["mets:file"].get("MIMETYPE"):
            retobj["encodingFormat"]=elem["mets:file"]["MIMETYPE"]
        if elem["mets:file"].get("mext:LABEL"):
            retobj["disambiguatingDescription"]=elem["mets:file"]["mext:LABEL"]
        if elem["mets:file"].get("ID"):
            retobj["identifier"]=elem["mets:file"]["ID"]
    return retobj if retobj else None

def handlefile(attribut,record):
    retobj=[]
    path="metadata>mets:mets>mets:fileSec>mets:fileGrp"
    objects=getNestedJsonObject(record,path)
    if objects:
        for elem in objects:
            if elem.get("USE")=="DELETED":
                continue
            try:
                if elem.get("USE")=="DOWNLOAD" and elem.get("mets:file") and isinstance(elem["mets:file"],dict):
                    bnode=handlemetsfile(elem)
                    if bnode:
                        retobj.append(bnode)
                elif elem.get("mets:file") and isinstance(elem["mets:file"],list):
                    for fd in elem["mets:file"]:
                        #eprint(fd)
                        if fd.get("USE")=="DOWNLOAD":
                            eprint(fd)
            except AttributeError:
                eprint(elem)
                exit(-1)
    return (attribut, retobj) if retobj else (None,None)
    
mapping={"@id":getUrn,
         "name":handletitle,
         "relatedTo":handleRelated,
         "sameAs":getSameAs,
         "person":handlePerson,
         "dateIssued":handledateIssued,
         "about":handleabout,
         "language":handlelanguage,
         "abstract":handleabstract,
         "associatedMedia":handlefile,
         }


def map_record(sourceRecord):
    record={}
    for target_value,source_value in mapping.items():
        if callable(source_value):
            key,value=source_value(target_value,sourceRecord)
            if value:
                if key=="__array__":
                    for elem in value:
                        if isinstance(elem,dict):
                            for subkey,item in elem.items():
                                record[subkey]=litter(record.get(subkey),item)
                else:
                    record[key]=value
            elif isinstance(source_value,str) and source_value in sourceRecord:
                record[target_value]=sourceRecord[source_value]
    if record:
        record["@context"]="http://schema.org"
    return record

def main():
    for line in sys.stdin:
        record=map_record(json.loads(line))
        print(json.dumps(record))

if __name__ == "__main__":
    main()
