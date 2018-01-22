#!/usr/bin/python3 -Wd
# -*- coding: utf-8 -*-
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from rdflib import URIRef
from pprint import pprint
from multiprocessing import Pool
from time import sleep
import json
#import urllib.request
import codecs
import argparse
import itertools
import sys
import io
import subprocess
from es2json import esgenerator
from es2json import eprint



baseuri="http://data.slub-dresden.de/"

args=None
estarget=None
outstream=None
count=0
actions=[]
es_totalsize=0
totalcount=0

def isint(num):
    try: 
        int(num)
        return True
    except:
        return False
    
def ArrayOrSingleValue(array):
    if array:
        length=len(array)
        if length>1 or isinstance(array,dict):
            return array
        elif length==1:
            for elem in array:
                 return elem
        elif length==0:
            return None

def getiso8601(date):
    p=re.compile(r'[\d|X].\.[\d|X].\.[\d|X]*') #test if D(D).M(M).Y(YYY)
    m=p.match(date)
    datestring=""
    if m:
        slices=list(reversed(date.split('.')))
        if isint(slices[0]):
            datestring+=str(slices[0])
        for slice in slices[1:]:
            if isint(slice):
                datestring+="-"+str(slice)
            else:
                break
        return datestring
    else:
        return date #was worth a try
    

def dateToEvent(date,schemakey):
    if '-' in date:
        dates=date.split('-')
        if date[0]=='[' and date[-1]==']': #oh..
            return str("["+dateToEvent(date[1:-1],schemakey)+"]")        
        if "irth" in schemakey: # (date of d|D)eath(Dates)
                return getiso8601(dates[0])
            #if(len(date)==5 and date[4]=='-' and isint(date[0:3])):
                #return date[0:4]
            #elif len(date)==9 and date[4]=='-' and isint(date[0:3]) and isint(date[5:8]):
                #return date[0:4]
        elif "eath" in schemakey: #(date of d|D)eath(Date)
            if len(dates)==2:
                return getiso8601(dates[1])
            elif len(dates)==1: #if   len(date)==5 and date[4]=='-' and isint(date[0:3]):                
                return None # still alive! congrats
            #elif len(date)==9 and date[4]=='-' and isint(date[0:3]) and isint(date[5:8]):
            #    return date[5:9]
    #elif event=="lifespan":    ### after finished coding this block, it appears useless to me. skip this
    #    if   len(date)==9 and date[4]=='-' and isint(date[0:3]) and isint(date[5:8]):
    #        return date
    #    elif len(date)==5 and date[4]=='-' and isint(date[0:3]):
    #        return date
    #    else:
    #        return date
        else:
            return date


def handlesex(jline,schemakey,schemavalue):
    for v in schemavalue:
        marcvalue=getmarc(v,jline)
        if isinstance(marcvalue,list):
            marcvalue=marcvalue[0]
    if isint(marcvalue):
        marcvalue=int(marcvalue)
    if marcvalue==0:
        return "Unknown"
    elif marcvalue==1:
        return "Male"
    elif marcvalue==2:
        return "Female"
    elif marcvalue==9:
        return None

marc2relation = {
    "VD-16 Mitverf": "contributor",
    "v:Mitverf": "contributor",
    "v:Co-Autor": "contributor",
    "v:Doktorvater": "contributor",
    "v:Illustrator": "contributor",
    "v:Übersetzer": "contributor",
    "v:Biograf": "contributor",
    "v:Förderer": "contributor",
    "v:Berater": "contributor",
    "v:hrsg": "contributor",
    "v:Mitautor": "contributor",
    "v:Partner": "contributor",
    
    
    "v:Tocher": "children",            #several typos in SWB dataset
    "tochter": "children",
    "Sohn": "children",
    "v:Nachkomme": "children",
    "v:zweites Kind": "children",
    
    "v:Gattin": "spouse",
    "v:Gatte": "spouse",
    "v:Gemahl": "spouse",
    "Ehe": "spouse",
    "Frau": "spouse",
    "Mann": "spouse",
    
    
    "v:Jüngster Bruder": "sibling",
    "Bruder": "sibling",
    "v:Zwilling": "sibling",
    "Schwester": "sibling",
    "v:Halbschwester": "sibling",
    "v:Halbbruder": "sibling",
    "Vater": "parent",
    "v:Stiefvater": "parent",
    "Mutter": "parent",
    
    "Nachfolger": "follows",
    "Vorgänger": "follows",
    
    "v:Vorfahr": "relatedTo",
        
    "v:Schüler": "relatedTo",        
    "Lehrer": "relatedTo",        
    "v:frühere Ehefrau": "relatedTo",
    "v:Schwager": "relatedTo",        
    "v:Ur": "relatedTo",          
    "v:Muse": "relatedTo",
    "v:Nachfahre": "relatedTo",         
    "v:Groß": "relatedTo",    
    "v:Langjähriger Geliebter": "relatedTo",    
    "v:Lebensgefährt": "relatedTo",    
    "v:Nichte": "relatedTo",
    "v:Stiefnichte": "relatedTo",
    "v:Neffe": "relatedTo",
    "v:Onkel": "relatedTo",
    "v:Tante": "relatedTo",
    "v:Verlobt": "relatedTo",
    "v:Vorfahren": "relatedTo",
    "v:Vetter": "relatedTo",
    "v:Tauf": "relatedTo",
    "v:Pate": "relatedTo",
    "v:Schwägerin": "relatedTo",
    "v:Schwiegervater": "relatedTo",
    "v:Schwiegermutter": "relatedTo",
    "v:Schwiegertochter": "relatedTo",
    "v:Schwiegersohn": "relatedTo",
    "v:Enkel": "relatedTo",
    "v:Mätresse": "relatedTo",
    "Freund": "knows",
    "v:Großvater": "relatedTo",
    "v:Cousin": "relatedTo",
    "v:Lebenspartner": "relatedTo",
    "v:Berater und Freund": "relatedTo",
    "v:Geliebte": "relatedTo",
    "v:Modell und Lebensgefährtin":"relatedTo",
    "v:Liebesbeziehung":"relatedTo",
    
    "v:publizistische Zusammenarbeit und gemeinsame öffentliche Auftritte": "colleague",
    "v:Sekretär": "colleague", 
    "v:Privatsekretär": "colleague", 
    "v:Kolleg": "colleague", 
    "v:Mitarbeiter": "colleague", 
    "v:Kommilitone": "colleague", 
    "v:Zusammenarbeit mit": "colleague", 
    "v:gemeinsames Atelier": "colleague", 
    "v:Geschäftspartner": "colleague" , 
    "v:musik. Partnerin": "colleague" ,
    "v:Künstler. Partner": "colleague" ,  
    "assistent": "colleague",
    
    
    #generic marc fields!
    "bezf":"relatedTo",
    "bezb":"colleague",
    "beza":"knows",
    "4:bete": "contributor",
    "4:rela":"knows",
    "4:affi":"knows"
}

def gnd2uri(string,entity):
    ret=[]
    if isinstance(string,str):
        if "(DE-588)" in string:
            ret.append("http://d-nb.info/gnd/"+string.split(')')[1])
        elif "(DE-576)" in string:
            ret.append(id2uri(string.split(')')[1],entity))
    elif isinstance(string,list):
        for st in string:
            ret.append(gnd2uri(st,entity))
    return ArrayOrSingleValue(ret)

def id2uri(string,entity):
    if entity=="Person":
        return baseuri+"persons/"+string
    elif entity=="CreativeWork":
        return baseuri+"resource/"+string


def getmarc(regex,json):
    ret=[]
    try:
        if len(regex)==3:
            return ArrayOrSingleValue(json[regex])
        if isinstance(regex,list):
            for string in regex:
                ret.append(getmarc(string,json))
        elif isinstance(regex,str):
            if str(regex[0:3]) in json:             ### beware! hardcoded traverse algorithm for marcXchange json encoded data !!!
                json=json[regex[0:3]]
                if isinstance(json,list):
                    for elem in json:
                        if isinstance(elem,dict):
                            for k,v in elem.items():
                                if isinstance(elem[k],list):
                                    for final in elem[k]:
                                        if regex[-1] in final:
                                            ret.append(final[regex[-1]])        
    except:
        pass
    if ret:
        return ArrayOrSingleValue(ret)
    
        

def handlerelative(jline,schemakey,schemavalue):
    data=None
    global args
    if schemakey=="relatedTo":
        data=[]
        try:
            for i in jline[schemavalue[0][0:3]][0]:
                for j in jline[schemavalue[0][0:3]][0][i]:
                    sset={}
                    person={}
                    for k in jline[schemavalue[0][0:3]][0][i]:
                        for c,w in k.items():
                            sset[c]=w
                    for key, value in sset.items():
                        if key=='9':
                            notfound=True
                            if isinstance(value,list):
                                for val in value:
                                    for k,v in marc2relation.items():
                                        if k.lower()==val:
                                            notfound=False
                                            person["_key"]=v
                                            break
                                if notfound:
                                    for k, v in marc2relation.items():
                                        if k.lower() in val.lower():
                                            notfound=False
                                            person["_key"]=v
                                            break  
                            elif isinstance(value,str):
                                for k,v in marc2relation.items():
                                    if k.lower()==value:
                                        notfound=False
                                        person["_key"]=v
                                        break
                                if notfound:
                                    for k, v in marc2relation.items():
                                        if k.lower() in value.lower():
                                            notfound=False
                                            person["_key"]=v
                                            break  
                            if notfound:
                                    person["_key"]="knows"
                        elif key=='0':
                            _id=value
                            if isinstance(_id,list):
                                for uri in _id:
                                    if "(DE-576)" in uri:
                                        person["@id"]=gnd2uri(uri,"Persons")
                            elif isinstance(_id,str) and "(DE-576)" in _id:
                                person["@id"]=gnd2uri(_id,"Person")
                        elif key=='a':
                            person["name"]=value
                    if person not in data:
                        data.append(person) ###filling the array with the person(s)
        except:
            pass
    elif schemakey=="@id":
        data=id2uri(getmarc(ArrayOrSingleValue(schemavalue),jline),args.entity)
    elif "Place" in schemakey:
        data=[]
        place={}        
        try:
            for i in jline[schemavalue[0][0:3]][0]:
                sset={}
                for c,w in i.items():
                    for elem in w:
                        for k,v in elem.items():
                            sset[k]=v
                conti=False
                if "9" in sset:
                    if sset["9"]=='4:ortg' and schemakey=="birthPlace":
                        conti=True
                    elif sset["9"]=='4:orts' and schemakey=="deathPlace":
                        conti=True
                if conti:
                    if "a" in sset:
                        place["name"]=sset["a"]
                    if "0" in sset:
                        place["@id"]=gnd2uri(sset["0"],"Place")
                if place:
                    data.append(place)
        except:
            pass
    elif schemakey=="honorificSuffix":
        data=[]
        try:
            for i in jline[schemavalue[0][0:3]][0]:
                sset={}
                for j in jline[schemavalue[0][0:3]][0][i]:
                    for k,v in dict(j).items():
                        sset[k]=v
                    conti=False
                    if "9" in sset:
                        if sset["9"]=='4:adel' or sset["9"]=='4:akad':
                                conti=True
                    if conti and "a" in sset:
                        data.append(sset["a"])
        except:
            pass
    elif schemakey=="hasOccupation":
        try:
            data=[]
            for i in jline[schemavalue[0][0:3]]:
                conti=False
                job={}
                sset={}
                for k,v in i.items():               # v = [{'0': ['(DE-576)210258373', '(DE-588)4219681-4']}, {'a': 'Romanist'}, {'9': '4:berc'}, {'w': 'r'}, {'i': 'Charakteristischer Beruf'}]
                    for w in v:                     # w = {'0': ['(DE-576)210258373', '(DE-588)4219681-4']}
                        for c,y in dict(w).items(): # c =0 y = ['(DE-576)210258373', '(DE-588)4219681-4']
                            sset[c]=y
                if "9" in sset:                     #sset = {'a': 'Romanist', 'w': 'r', '0': ['(DE-576)210258373', '(DE-588)4219681-4'], 'i': 'Charakteristischer Beruf', '9': '4:berc'}
                    if sset["9"]=='4:berc' or sset["9"]=='4:beru' or sset['9']=='4:akti':
                            conti=True
                if conti:
                    for key in schemavalue:
                        if key[-1] in sset and key[-1]=='0':
                            if isinstance(sset[key[-1]],list):
                                for field in sset[key[-1]]:
                                    if "(DE-588)" in field:
                                        job["@id"]=gnd2uri(field,"hasOccupation")
                        elif key[-1]=='a':
                            if key[-1] in sset:
                                job["name"]=str(sset[key[-1]])
                        if "name" in job and "@id" in job:
                            data.append(job)
        except:
            pass
    elif schemakey=="birthDate" or schemakey=="deathDate" or schemakey=="Birth" or schemakey=="Death":
        try:
            data=[]
            for i in jline[schemavalue[0][0:3]][0]:
                sset={}
                for j in jline[schemavalue[0][0:3]][0][i]:
                    for k,v in dict(j).items():
                        sset[k]=v
                if "9" in sset:
                    if sset['9']=='4:datx':
                        if "a" in sset:
                             data.append(dateToEvent(sset['a'],schemakey))
                    elif sset['9']=='4:datl':
                        if "a" in sset:
                            data.append(dateToEvent(sset['a'],schemakey))
        except:
            pass
    if data:
        return ArrayOrSingleValue(data)
          
def removeNone(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
            for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj

def removeEmpty(obj):
    if isinstance(obj,dict):
        toDelete=[]
        for k,v in obj.items():
            if v:
                v = ArrayOrSingleValue(removeEmpty(v))
            else:
                toDelete.append(k)
        for key in toDelete:
            obj.pop(key)
        return obj
    elif isinstance(obj,str):
        return obj
    elif isinstance(obj,list):
        for elem in obj:
            if elem:
                elem = removeEmpty(elem)
            else:
                del elem
        return obj

#make data more RDF
def check(ldj):
    ldj=removeNone(ldj)
    ldj=removeEmpty(ldj)
    for k,v in ldj.items():
        v=ArrayOrSingleValue(v)
    if 'author_finc' in ldj:
        if isinstance(ldj['author_finc'],str):
            ldj["author"]="http://data.slub-dresden.de/persons/swb-"+ldj.pop('author_finc')
        elif isinstance(ldj['author_finc'],list):
            ldj["author"]=[]
            for author in ldj['author_finc']:
                    ldj["author"]="http://data.slub-dresden.de/persons/swb-"+ldj["author_finc"]
            ldj.pop("author_finc")
    for person in ["author","contributor"]:
        if person in ldj:
            if isinstance(ldj[person],str):
                if "DE-576" in ldj[person]:
                    uri=gnd2uri(ldj.pop(person),"Person")
                    ldj[person]=uri
                if "DE-588" in ldj[person]:
                    ldj.pop(person)
            elif isinstance(ldj[person],list):
                persons=[]
                for author in ldj[person]:
                    if "DE-576" in author:
                        persons.append(gnd2uri(author,"Person"))
                ldj.pop(person)
                ldj[person]=persons
    if 'name' in ldj:
        name=ArrayOrSingleValue(ldj.pop("name"))
        if isinstance(name,str):
            if name[-2:]==" /":
                ldj['name']=name[:-2]
        elif isinstance(name,list):
            for elem in name:
                if elem[-2:]==" /":
                    elem=elem[:-2]
        else:
            ldj['name']=name
    if 'oclc_num' in ldj:
        if 'sameAs' not in ldj:
            ldj['sameAs']=[]
        if isinstance(ldj['oclc_num'],str):
             ldj['sameAs'].append("http://www.worldcat.org/oclc/"+str(ArrayOrSingleValue(ldj.pop('oclc_num')))+".rdf")
        elif isinstance(ldj['oclc_num'],list):
            for elem in ldj['oclc_num']:
                ldj['sameAs'].append("http://www.worldcat.org/oclc/"+str(ArrayOrSingleValue(elem))+".rdf")
            ldj.pop('oclc_num')
    if 'genre' in ldj:
        genre=ldj.pop('genre')
        ldj['genre']={}
        ldj['genre']['@type']="Text"
        ldj['genre']["Text"]=genre
    if 'bookEdition' in ldj:
        be=ldj.pop('bookEdition')
        ldj['bookEdition']={}
        ldj['bookEdition']['@type']="Book"
        ldj['bookEdition']["Text"]=be
    if 'numberOfPages' in ldj:
        numstring=ldj.pop('numberOfPages')
        try:
            if isint(numstring.split(' ')[0]):
                if numstring.split(' ')[1] == "S.":
                    num=int(numstring.split(' ')[0])
                    ldj['numberOfPages']=num
        except IndexError:
            if isint(numstring):
                ldj['numberOfPages']=numstring
            else:
                pass
        except AttributeError:
            pass
    if args.entity=="Person":
        checks=["relatedTo","hasOccupation","birthPlace","deathPlace"]
        for key in checks:
            if key in ldj:
                if isinstance(ldj[key],list):
                    for pers in ldj[key]:
                        if "@id" not in pers:
                            del pers
                elif isinstance(ldj[key],dict):
                    if "@id" not in ldj[key]:
                        ldj.pop(key)
                elif isinstance(ldj[key],str):
                    ldj.pop(key)
    elif args.entity=="CreativeWork":
        if '@id' in ldj:
            num=ldj.pop('@id')
            ldj['@id']="http://data.slub-dresden.de/resources/swb-"+str(num)
            ldj['identifier']="swb-"+str(num)
    for label in ["name","alternativeHeadline"]:
        if label in ldj:
            if ldj[label][-2:]==" /":
                name=ldj.pop(label)
                ldj[label]=name[:-2]
    if "publisherImprint" in ldj:
        ldj["@context"].append(URIRef(u'http://bib.schema.org/'))
    if "isbn" in ldj:
        ldj["@type"].append(URIRef(u'http://schema.org/Book'))
    return ldj

def finc(jline,schemakey,schemavalue):
    ret=[]
    for v in schemavalue:
        if v in jline:
            for k,v in traverse(jline[v],""):
                ret.append(v)                
    return ArrayOrSingleValue(ret)

schematas = {
    "resource.finc":{
        "@id"                   :[finc,"record_id"],
        "name"                  :[finc,"title"],
        "alternativeHeadline"   :[finc,"title_sub"],
        "alternateName"         :[finc,"title_alt"],
        "author_finc"             :[finc,"author_id"],
        "contributor"           :[finc,"author2"],
        "publisherImprint"      :[finc,"imprint"],
        "publisher"             :[finc,"publisher"],
        "datePublished"         :[finc,"publishDate"],
        "isbn"                  :[finc,"isbn"],
        "genre"                 :[finc,"genre","genre_facet"],
        "hasPart"               :[finc,"container_reference"],
        "isPartOf"              :[finc,"container_title"],
        "inLanguage"            :[finc,"language"],
        "numberOfPages"         :[finc,"physical"],
        "description"           :[finc,"description"],
        "bookEdition"           :[finc,"edition"],
        "comment"               :[finc,"contents"],
        "oclc_num"              :[finc,"oclc_num"],
        "identifier"            :[finc,"record_id"],
        },
   "resource.mrc":{
        "@id"               :["001"],
        "name"              :["245.*.a","245.*.b","245.*.n","245.*.p"],
        "alternateName"     :["130.*.a","130.*.p","240.*.a","240.*.p","246.*.a","246.*.b","245.*.p","249.*.a","249.*.b","730.*.a","730.*.p","740.*.a","740.*.p","920.*.t"],
        #"author"           :["100.*.a","700.*.a"],
        "author"         :["100.*.0"],
        "contributor"       :["700.*.0"],
        "publisher"         :["260.*.b","264.*.b"],
        "datePublished"     :["260.*.c","264.*.c","362.*.a"],
        "Thesis"            :["502.*.a","502.*.b","502.*.c","502.*.d"],
        "issn"              :["022.*.a","022.*.y","022.*.z","029.*.a","490.*.x","730.*.x","773.*.x","776.*.x","780.*.x","785.*.x","800.*.x","810.*.x","811.*.x","830.*.x"],
        "isbn"              :["022.*.a","022.*.z","776.*.z","780.*.z","785.*.z"],
        #"ismn"             :["024.*.a","028.*.a",],
        "genre"             :["655.*.a"],
        "hasPart"           :["773.*.g"],
        "isPartOf"          :["773.*.t","773.*.s","773.*.a"],
        "license"           :["540.*.a"],
        "inLanguage" :["041.*.a","041.*.d","130.*.l","730.*.l"],
        "numberOfPages"     :["300.*.a","300.*.b","300.*.c","300.*.d","300.*.e","300.*.f","300.*.g"],
        "pageStart"         :["773.*.q"],
        "issueNumber"       :["773.*.l"],
        "volumeNumer"       :["773.*.v"]
        },
    "person.mrc": {
        "identifier":  ["001"],
        "@id":  [handlerelative,"001"],
        "name": "100..a",
        "sameAs":   "024..a",
        "gender":   [handlesex,"375..a"],
        "alternateName":    ["400..a","400..c"],
        "relatedTo":  [handlerelative,
            "500..0",
            "500..a",
            "500..9"
        ],
        "hasOccupation": [handlerelative,
            "550..0",
            "550..a",
            "550..9"
        ],
        "birthPlace": [handlerelative,
                       "551..0",
                       "551..a",
                       "551..9"
                       ],
        "deathPlace": [handlerelative,
                       "551..0",
                       "551..a",
                       "551..9"
                       ],
        "honoricSuffix": [handlerelative,
            "550..0",
            "550..00",
            "550..i",
            "550..a",
            "550..9"
        ],
       # "jobTitle":["678..b"],
        "birthDate":    [handlerelative,"548..a","548..9"],
        "deathDate":    [handlerelative,"548..a","548..9"]
    }
}

def process_mapping():
    global args
    esmapping=""
    esmapping+="{\"mappings\":{\""+args.schema+"\":{\"properties\":{"
    items=0
    for k, v in schematas[args.schema].items():
        items+=1
        esmapping+="\""+str(k)+"\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256} } }"
        if items<len(schematas[args.schema]):
            esmapping+=","
    esmapping+="} } } }"
    return esmapping


def traverse(dict_or_list, path):
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list, list):
        iterator = enumerate(dict_or_list)
    elif isinstance(dict_or_list,str):
        strarr=[]
        strarr.append(dict_or_list)
        iterator=enumerate(strarr)
    elif callable(dict_or_list):
        return
    for k, v in iterator:
        yield path + str([k]), v
        if isinstance(v, (dict, list)):
            for k, v in traverse(v, path + str([k])):
                yield k, v


def schemas():
    for k,v in traverse(schematas,""):
        if k and v:
            print(k,v)
    exit(0)


#processing a single line of json without whitespace 
def process_stuff(jline):
    global args
    global outstream
    global estarget
    global actions
    global totalcount
    global es_totalsize
    mapline=dict()
    mapline["@context"]=[URIRef(u'http://schema.org')]
    global count
    if args.entity=="Person":
        if getmarc("079..b",jline)=="p":
            notEntity=False
            mapline["@type"]=URIRef(u'http://schema.org/Person')
    elif args.entity=="CreativeWork":
        mapline["@type"]=[]
        mapline["@type"].append(URIRef(u'http://schema.org/CreativeWork'))
        #mapline["@type"].append(URIRef(u'http://purl.org/ontology/bibo/Document'))
    if args.schema not in schematas:
        sys.stderr.write("Warning! selected schema not in schematas! Correct your mistakes and edit this file!\n")
        exit(1)
    for k,v in schematas[args.schema].items():
        value=None
        if not isinstance(v,list):
            value=getmarc(v,jline)
        elif callable(v[0]):
            value=v[0](jline,k,v[1:])
        else:
            value=getmarc(v,jline)
        if value:
            noRel=True
            if isinstance(value,dict) and "_key" in value:
                relation=value.pop("_key")
                dictkey=relation
                mapline[dictkey]=removeNone(value)
                noRel=False
            elif isinstance(value,list):
                for elem in value:
                    if isinstance(elem,dict) and "_key" in elem:
                        relation=elem.pop("_key")
                        dictkey=relation
                        if dictkey not in mapline:
                            mapline[dictkey]=[elem]
                        else:
                            mapline[dictkey].append(removeNone(elem))
                        noRel=False
                    elif isinstance(elem,dict):
                        if k not in mapline:
                            mapline[k]=[elem]
                        else:
                            mapline[k].append(elem)
                        noRel=False
                        
            if noRel:
                dictkey=k
                mapline[dictkey]=ArrayOrSingleValue(value)
    if mapline:
        mapline=check(mapline)
        if outstream:
            outstream.write(json.dumps(mapline,indent=None)+"\n")
            print(ArrayOrSingleValue(mapline["identifier"]))
        else:
            sys.stdout.write(json.dumps(mapline,indent=None)+"\n")
            sys.stdout.flush()
            
if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='return field statistics of an ElasticSearch Search Index')
    parser.add_argument('-schema',type=str,default="person",help='Select which schema to use! use -show_schemas to see the defined schemas')
    parser.add_argument('-entity',type=str,default="person",help='Select entity. Supported Entities: Person, CreativeWork')
    parser.add_argument('-i',type=str,help='Input file to process! Default is stdin if no arg is given. Faster than stdin because of multiprocessing!')
    parser.add_argument('-o',type=str,help='Output file to dump! Default is stdout if -in or -srchost is given is given')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we print ldj to stdout.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-show_schemas', action='store_true',help='show the schemas defined in the sourcecode')
    args=parser.parse_args()
    
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if args.schema not in schematas:
            sys.stderr.write("schema doesn't exist!\n")
            sys.stderr.write("existing schemas:\n")
            schemas()
            exit(-1)
    elif args.show_schemas:
        schemas()
    if args.o:
        outstream=codecs.open(args.o,'w',encoding='utf-8')
    
    if args.i: #first attempt to read vom -inf
        with codecs.open(args.i,'r',encoding='utf-8') as f: #use with parameter to close stream after context
            for line in f:
                process_stuff(json.loads(line))
    elif args.host: #if inf not set, than try elasticsearch
        if args.index and args.type:
            for hits in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True):
                process_stuff(hits)
        else:
            sys.stderr.write("Error! no Index/Type set but -host! add -index and -type or disable -host if you read from stdin/file Aborting...\n")
    else: #oh noes, no elasticsearch input-setup. then we'll use stdin
        for line in input_stream:
            process_stuff(json.loads(line))
            
    if outstream:
        outstream.close()
