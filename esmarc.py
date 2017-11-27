#!/usr/bin/python3 -Wd
# -*- coding: utf-8 -*-
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from jsonpath_rw import jsonpath, parse ### so slow!
from rdflib import URIRef
from pprint import pprint
from multiprocessing import Pool
from time import sleep
import re
import json
import urllib.request
import codecs
import argparse
import itertools
import sys
import io
import subprocess


selectedschema = "schemaorg"

baseuri="http://data.slub-dresden.de/"

entity="Person"

args=None
estarget=None
outstream=None
count=None
actions=[]
es_totalsize=0
totalcount=0

def isint(num):
    try: 
        int(num)
        return True
    except:
        return False
    
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)    
    
def ArrayOrSingleValue(array):
    if array:
        length=len(array)
        if length>1 or isinstance(array,str):
            return array
        elif isinstance(array,dict):
            if length==1:
                for k,v in array.items():
                    return v
        elif length==1:
            if array[0]:
                return array[0]
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
    
    "v:publizistische Zusammenarbeit und gemeinsame öffentliche Auftritte": "collegue",
    "v:Sekretär": "collegue", 
    "v:Privatsekretär": "collegue", 
    "v:Kolleg": "collegue", 
    "v:Mitarbeiter": "collegue", 
    "v:Kommilitone": "collegue", 
    "v:Zusammenarbeit mit": "collegue", 
    "v:gemeinsames Atelier": "collegue", 
    "v:Geschäftspartner": "collegue" , 
    "v:musik. Partnerin": "collegue" ,
    "v:Künstler. Partner": "collegue" ,  
    "assistent": "collegue",
    
    
    #generic marc fields!
    "bezf":"relatedTo",
    "bezb":"collegue",
    "beza":"knows",
    "4:bete": "contributor",
    "4:rela":"knows",
    "4:affi":"knows"
}

               
def findrelation(string):
    if string:
        for k,v in marc2relation.items():
            if k.lower()==string.lower():
                return v
        for k, v in marc2relation.items():
            if k.lower() in string.lower():
                return v
        sys.stderr.write(string+"\n")
        sys.stderr.flush()
    return "knows"

def gnd2uri(string):
    ret=[]
    if isinstance(string,str):
        if "(DE-588)" in string:
            ret.append("http://d-nb.info/gnd/"+string.split(')')[1])
        elif "(DE-576)" in string:
            ret.append(id2uri(string.split(')')[1]))
    elif isinstance(string,list):
        for st in string:
            ret.append(gnd2uri(st))
    return ArrayOrSingleValue(ret)

def id2uri(string):
    if entity=="Person":
        return baseuri+"persons/"+string
    elif entity=="CreativeWork":
        return baseuri+"resource/"+string

def handletit(jline,schemakey,schemavalue):
    data=None
    if schemakey=="inlanguage":
        return

def getmarc(regex,json):
    ret=[]
    if len(regex)==3:
        if regex in json:
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
    if ret:
        return ArrayOrSingleValue(ret)
    
        

def handlerelative(jline,schemakey,schemavalue):
    data=None
    
    if schemakey=="relatedTo":
        data=[]
        if schemavalue:
            if schemavalue[0][0:3] in jline:
                jline=jline[schemavalue[0][0:3]]
                for i in jline[0]:
                    person={}
                    for j in jline[0][i]:
                        for key in schemavalue:
                            if key[-1] in dict(j):
                                if key[-1]=='9':
                                    if isinstance(dict(j)[key[-1]],list):
                                        notfound=True     # we iterate 4 times over the dict because we first want to match the exact types, then we match the not-exact types...
                                        for k,v in marc2relation.items():
                                            if k.lower()==str(dict(j)[key[-1]][1]).lower():
                                                notfound=False
                                                person["_key"]=v
                                                break
                                        if notfound:
                                            for k, v in marc2relation.items():
                                                if k.lower() in str(dict(j)[key[-1]][1]).lower():
                                                    notfound=False
                                                    person["_key"]=v
                                                    break   
                                            for k, v in marc2relation.items():
                                                if k.lower()==str(dict(j)[key[-1]][0]).lower():
                                                    person["_key"]=v
                                                    notfound=False
                                                    break
                                            for k, v in marc2relation.items():
                                                if k.lower() in str(dict(j)[key[-1]][0]).lower():
                                                    notfound=False
                                                    person["_key"]=v
                                                    break
                                        if notfound:
                                            sys.stderr.write(str(dict(j)[key[-1]][1])+" "+str(dict(j)[key[-1]][0])+"\n")
                                            sys.stderr.flush()
                                            person["_key"]="knows"
                                elif key[-1]=='0':
                                    _id=dict(j)[key[-1]]
                                    if isinstance(_id,list):
                                        for uri in _id:
                                            if "(DE-576)" in uri:
                                                person["@id"]=gnd2uri(uri)
                                    if isinstance(_id,str) and "(DE-576)" in _id:
                                        person["@id"]=gnd2uri(_id)
                                elif key[-1]=='a':
                                    person["name"]=str(dict(j)[key[-1]])
                    
                    if person not in data:
                        data.append(person) ###filling the array with the person(s)
    elif schemakey=="@id":
        data=id2uri(getmarc(ArrayOrSingleValue(schemavalue),jline))
    elif "Place" in schemakey:
        data=[]
        place={}        #yes, Elasticsearch is that stupid that we have to put this dict into the array declared above
        if schemavalue:
            if schemavalue[0][0:3] in jline:
                jline=jline[schemavalue[0][0:3]]
                for i in jline:
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
                            place["@id"]=gnd2uri(sset["0"])
                        data.append(place)
    elif schemakey=="honoricSuffix":
        data=[]
        if schemavalue:
            if schemavalue[0][0:3] in jline:
                jline=jline[schemavalue[0][0:3]]
                for i in jline[0]:
                    sset={}
                    for j in jline[0][i]:
                        for k,v in dict(j).items():
                            sset[k]=v
                    conti=False
                    if "9" in sset:
                        if sset["9"]=='4:adel' or sset["9"]=='4:akad':
                                conti=True
                    if conti and "a" in sset:
                        data.append(sset["a"])
    elif schemakey=="hasOccupation":
        if schemavalue:
            if schemavalue[0][0:3] in jline:
                jline=jline[schemavalue[0][0:3]]
                data=[]
                for i in jline: # i = {'__': [{'0': ['(DE-576)210258373', '(DE-588)4219681-4']}, {'a': 'Romanist'}, {'9': '4:berc'}, {'w': 'r'}, {'i': 'Charakteristischer Beruf'}]}
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
                                            job["@id"]=gnd2uri(field)
                            elif key[-1]=='a':
                                if key[-1] in sset:
                                    job["name"]=str(sset[key[-1]])
                    if "name" in job and "@id" in job:
                        data.append(job)
    elif schemakey=="birthDate" or schemakey=="deathDate" or schemakey=="Birth" or schemakey=="Death":
        data=[]
        if schemavalue:
            if schemavalue[0][0:3] in jline:
                jline=jline[schemavalue[0][0:3]]
                for i in jline[0]:
                    sset={}
                    for j in jline[0][i]:
                        for k,v in dict(j).items():
                            sset[k]=v
                    if "9" in sset:
                        if sset['9']=='4:datx':
                                if "a" in sset:
                                    data.append(dateToEvent(sset['a'],schemakey))
                        elif sset['9']=='4:datl':
                                if "a" in sset:
                                    data.append(dateToEvent(sset['a'],schemakey))
    #else:
        #if schemavalue:
            #if schemavalue[0][0:3] in jline:
                #data=[]
                #for i in jline[0]:
                    #for j in jline[0][i]:
                        #for key in schemavalue:
                            #if key[-1] in dict(j):
                                #data.append(dict(j)[key[-1]][1])
    if data:
        return data
            
def handleliteral(jline,schemakey,schemavalue):
    for v in schemavalue:
        marcvalue=marc2stringarray(jline,v)
    if isinstance(marcvalue,list):
        marcvalue=marcvalue[0]
    return marcvalue

#make finc more RDF
def handlefinc(ldj):
    if 'author_id' in ldj:
        #if 'source_id' in ldj:
            #eprint(ldj['author'],ldj['author_id'],ldj['@id'],ldj['source_id'])
        if isinstance(ldj['author_id'],list):
            for j in ldj['author_id']: #iterate through IDs if multiple:
                author=dict()
                author['@id']="http://d-nb.info/gnd/"+j
                if not 'author' in ldj:
                    ldj['author']=[]
                if author:
                    ldj['author'].append(author)
        elif isinstance(ldj['author_id'],str):
                author=dict()
                author['@id']="http://d-nb.info/gnd/"+ldj['author_id']
                if not 'author' in ldj:
                    ldj['author']=[]
                if author:
                    ldj['author'].append(author)
        ldj.pop('author_id')
    if 'oclc_num' in ldj:
        if 'sameAs' not in ldj:
            ldj['sameAs']=[]
        if isinstance(ldj['oclc_num'],str):
             ldj['sameAs'].append("http://www.worldcat.org/oclc/"+str(ArrayOrSingleValue(ldj.pop('oclc_num')))+".rdf")
        elif isinstance(ldj['oclc_num'],list):
            for elem in ldj['oclc_num']:
                ldj['sameAs'].append("http://www.worldcat.org/oclc/"+str(ArrayOrSingleValue(elem))+".rdf")
            ldj.pop('oclc_num')
    return removeNone(ldj)

def finc(jline,schemakey,schemavalue):
    ret=[]
    for v in schemavalue:
        if v in jline:
            for k,v in traverse(jline[v],""):
                ret.append(v)                
    return ArrayOrSingleValue(ret)

def handleisbn(jline,schemakey,schemavalue):
    ret=[]
    if "isbn" in jline:
        for v in jline["isbn"]:
            if schemakey=="P957":
                if v[0:3]=="978":
                    ret.append(v[3:-1])
                else:
                    ret
            elif schemakey=="P212":
                if v[0:3]=="978":
                    ret.append(v)


schematas = {
    #"finc2wikidata":{
        #"PPN"           :[finc,"record_id"],
        #"P1476"         :[finc,"title"],
        #"P1680"         :[finc,"title_sub"],
        #"P50"           :[finc,"author"],
        #"P767"          :[finc,"author2"],
        #"P227"          :[finc,"author_id"],
        #"P291"          :[finc,"publish_place"],
        #"P123"          :[finc,"publisher"],
        #"P577"          :[finc,"publishDate"],
        #"P236"          :[finc,"issn"],
        #"P957"          :[handleisbn,"isbn"],
        #"P212"          :[handleisbn,"isbn"],
        #"P1208"         :[finc,"ismn"],
        #"P136"          :[finc,"genre","genrefacet"],
        #"P179"          :[finc,"container_reference"],
        #"P364"          :[finc,"language"],
        #"P1104"         :[finc,"physical"],
        #"P747"          :[finc,"ediditon"]
        #},
    #"finc2kim":{
        #"PPN"           :[finc,"record_id"],
        #"dcelements:title"    :[finc,"title"],
        #"rdau:P60493"         :[finc,"title_sub"],
        #"rdau:P60493"         :[finc,"title_part"],
        #"dcterms:alternative" :[finc,"title_alt"],
        #"dcterms:creator"     :[finc,"author"],
        #"dcterms:contributor" :[finc,"author2"],
        #"rdau:P60333"         :[finc,"imprint"],
        #"rdau:P60163"         :[finc,"publish_place"],
        #"dcterms:publisher"   :[finc,"publisher"],
        #"dcterms:issued"      :[finc,"publishDate"],
        #"rdau:p60489"         :[finc,"dissertation_note"],
        #"bibo:issn"           :[finc,"issn"],
        #"bibo:isbn"           :[finc,"isbn"],
        #"bibo:ismn"           :[finc,"ismn"],
        #"rdau:60049"          :[finc,"genre","genrefacet"],
        #"dcterms:hasPart"     :[finc,"container_reference"],
        #"dcterms:isPartOf"    :[finc,"container_title"],
        #"dcterms:language"    :[finc,"language"],
        #"isbd:P1053"          :[finc,"physical"],
        #"bibo:edition"        :[finc,"editon"]
        #},
    "resource.finc":{
        "@id"                   :[finc,"record_id"],
        "name"                  :[finc,"title"],
        "alternativeHeadline"   :[finc,"title_sub"],
        "alternateName"         :[finc,"title_alt"],
        "author_id"             :[finc,"author_id"],
        "contributor"           :[finc,"author2"],
        "publisherImprint"      :[finc,"imprint"],
        "publisher"             :[finc,"publisher"],
        "datePublished"         :[finc,"publishDate"],
        "isbn"                  :[finc,"isbn"],
        "genre"                 :[finc,"genre","genre_facet"],
        "hasPart"               :[finc,"container_reference"],
        "isPartOf"              :[finc,"container_title"],
        "availableLanguage"     :[finc,"language"],
        "numberOfPages"         :[finc,"physical"],
        "description"           :[finc,"description"],
        "workExample"           :[finc,"edition"],
        "comment"               :[finc,"contents"],
        "oclc_num"              :[finc,"oclc_num"]
        },
   "resource.mrc":{
        "@id":  ["001"],
        "name"    :["245.*.a","245.*.b"],
        "name"         :["245.*.n","245.*.p"],
        "alternateName" :["130.*.a","130.*.p","240.*.a","240.*.p","246.*.a","246.*.b","245.*.p","249.*.a","249.*.b","730.*.a","730.*.p","740.*.a","740.*.p","920.*.t"],
        "author"     :["100.*.a","700.*.a"],
        "author_id"  :["100.*.0","700.*.0"],
        "contributor" :["700.*.a","700.*.b"],
        "publisher"         :["260.*.a","260.*.b","260.*.c","264.*.a","264.*.b","264.*.c"],
        "publisher"         :["260.*.a","264.*.a"],
        "datePublished"      :["264.*.c","260.*.c"],
        "Thesis"         :["502.*.a","502.*.b","502.*.c","502.*.d"],
        "issn"           :["022.*.a","022.*.y","022.*.z","029.*.a","490.*.x","730.*.x","773.*.x","776.*.x","780.*.x","785.*.x","800.*.x","810.*.x","811.*.x","830.*.x"],
        "isbn"           :["022.*.a","022.*.z","776.*.z","780.*.z","785.*.z"],
        #"ismn"           :["024.*.a","028.*.a",],
        "genre"          :["655.*.a","600.*.v","610.*.v","611.*.v","630.*.v","648.*.v","650.*.v","651.*.v","655.*.v"],
        "hasPart"     :["773.*.g"],
        "isPartOf"    :["773.*.s"],
        "availableLanguage"    :["041.*.a","041.*.d","130.*.l","730.*.l"],
        "numberOfPages"          :["300.*.a","300.*.b","300.*.c","300.*.d","300.*.e","300.*.f","300.*.g"],
        "date" :["362.*.a"]
        },
   
   #"marc2kim":{
        #"PPN":  ["001"],
        #"dcelements:title"    :["245.*.a","245.*.b"],
        #"rdau:P60493"         :["245.*.n","245.*.p","245.*.b"],
        #"dcterms:alternative" :["130.*.a","130.*.p","240.*.a","240.*.p","246.*.a","246.*.b","245.*.p","249.*.a","249.*.b","730.*.a","730.*.p","740.*.a","740.*.p","920.*.t"],
        #"dcterms:creator"     :["100.*.a","700.*.a"],
        #"dcterms:contributor" :["700.*.a","700.*.b"],
        #"rdau:P60333"         :["260.*.a","260.*.b","260.*.c","264.*.a","264.*.b","264.*.c"],
        #"rdau:P60163"         :["260.*.a","264.*.a"],
        #"dcterms:publisher"   :["260.*.a","264.*.a"],
        #"dcterms:issued"      :["264.*.c","260.*.c"],
        #"rdau:p60489"         :["502.*.a","502.*.b","502.*.c","502.*.d"],
        #"bibo:issn"           :["022.*.a","022.*.y","022.*.z","029.*.a","490.*.x","730.*.x","773.*.x","776.*.x","780.*.x","785.*.x","800.*.x","810.*.x","811.*.x","830.*.x"],
        #"bibo:isbn"           :["022.*.a","022.*.z","776.*.z","780.*.z","785.*.z"],
        #"bibo:ismn"           :["024.*.a","028.*.a",],
        #"rdau:60049"          :["655.*.a","600.*.v","610.*.v","611.*.v","630.*.v","648.*.v","650.*.v","651.*.v","655.*.v"],
        #"dcterms:hasPart"     :["773.*.g"],
        #"dcterms:isPartOf"    :["773.*.s"],
        #"dcterms:language"    :["041.*.a","041.*.d","130.*.l","730.*.l"],
        #"isbd:P1053"          :["300.*.a","300.*.b","300.*.c","300.*.d","300.*.e","300.*.f","300.*.g"],
        #"dcterms:bibliographicCitation" :["362.*.a"]
        #},
        
        
    #"voidschema": {
        #"PPN":  ["001"],
        #"URI":  "024.*.a",
        #"GND_ID":   "035.*.a",
        #"name":    "100.*.a",
        #"lifespan": [handlerelative,"100.*.d"],
        #"sex":  [handlesex,"375.*.a"],
        #"name_variants":    "400.*.a",
        #"related": [handlerelative,
            #"500.*.0",
            #"500.*.a",
            #"500.*.9"
        #],
        #"Birth":    [handlerelative,"548.*.a"],
        #"Death":    [handlerelative,"548.*.a"],
        #"tags":    "550.*.a",
        #"name_pref":    "700.*.a"
    #},
    "bf:person": {
        "@id":  ["001"],
        "authorityLink": [
            "024.*.a",
            "035.*.a"
        ],
        "name": "100.*.a",
        "gender":   [handlesex,"375.*.a"],
        "name_variants":    "400.*.a",
        "related": [handlerelative,
            "500.*.0",
            "500.*.a",
            "500.*.9"
        ],
        "Birth":[handlerelative,
                       "551..0",
                       "551..a",
                       "551..9"
                       ],
        "Death": [handlerelative,
                       "551..0",
                       "551..a",
                       "551..9"
                       ],
        "tags": "550.*.a"
    },
    "person": {
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
        "jobTitle":["678..b"],
        "birthDate":    [handlerelative,"548..a","548..9"],
        "deathDate":    [handlerelative,"548..a","548..9"]
    }
}

def process_mapping():
    esmapping=""
    esmapping+="{\"mappings\":{\""+selectedschema+"\":{\"properties\":{"
    items=0
    for k, v in schematas[selectedschema].items():
        items+=1
        esmapping+="\""+str(k)+"\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256} } }"
        if items<len(schematas[selectedschema]):
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

"""        
def getmarc(regex,json):
    eprint(regex)
    ret=[]
    if isinstance(regex,list):
        for string in regex:
            ret.append(getmarc(string,json))
    elif isinstance(regex,str):
        newregex="$"
        for field in regex.split('.'):
            if isint(field):
                newregex+=".\""+str(field)+"\""
            else:
                newregex+="."+field
        return ArrayOrSingleValue([match.value for match in parse(newregex).find(json)])        
    return ArrayOrSingleValue(ret)
"""

def mergelists(one,two):
    ret=[]
    if len(one) != len(two):
        return
    if isinstance(one,str):
        if isinstance(two,str):
            ret.append(one)
            ret.append(two)
    else:
        for i in range(len(one)):
            ret.append([one[i],two[i]])
    return ret

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total: 
        print()


def removeNone(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
            for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj

def check(obj):
    global args
    obj=removeNone(obj)
    removekeys=[]
    for k,v in obj.items():
        if v:
            if isinstance(v,list):
                for elem in v:
                    if not elem:
                        del elem
        elif not v:
            removekeys.append(k)
        else:
            v=ArrayOrSingleValue(ArrayOrSingleValue(v))
    for k in removekeys:
        obj.pop(k)
    if args.entity=="Person":
        checks=["relatedTo","hasOccupation","birthPlace","deathPlace"]
        for key in checks:
            if key in obj:
                if isinstance(obj[key],list):
                    for pers in obj[key]:
                        if "@id" not in pers:
                            del pers
                elif isinstance(obj[key],dict):
                    if "@id" not in obj[key]:
                        obj.pop(key)
                elif isinstance(obj[key],str):
                    obj.pop(key)
    return obj

#processing a single line of json without whitespace 
def process_stuff(jline):
    global args
    global outstream
    global estarget
    global selectedschema
    global actions
    global totalcount
    global es_totalsize
    if '_source' in jline:
        jline=jline['_source']
    mapline=dict()
    global count
    if entity=="Person":
        if getmarc("079..b",jline)=="p":
            notEntity=False
            mapline["@type"]=URIRef(u'http://schema.org/Person')
            mapline["@context"]='http://schema.org'
        else:
            mapline["@type"]=URIRef(u'http://schema.org/CreativeWork')
            mapline["@context"]='http://schema.org'
    if selectedschema not in schematas:
        sys.stderr.write("Warning! selectedschema not in schematas! Correct your mistakes and edit this file!\n")
        exit(1)
    for k,v in schematas[selectedschema].items():
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
        if "finc" in selectedschema:
            mapline=handlefinc(mapline)
        count+=1
        if outstream:
            outstream.write(json.dumps(mapline,indent=None)+"\n")   
        elif args.tohost is None:
            sys.stdout.write(json.dumps(mapline,indent=None)+"\n")
            sys.stdout.flush()
        else:
            actions.append({"_index":args.toindex,"_type":selectedschema,"_id":mapline["@id"],"_source":mapline})    
            if count==1000:
                helpers.bulk(estarget,actions)
                actions=[]
                count=0
        if count==1000:
            totalcount+=count
            if(es_totalsize > 0):
                printProgressBar(float(totalcount/1000),float(es_totalsize/1000), prefix = 'Progress:', suffix = 'Complete', length = 100)
            count=0
            
def setup_output():
    global args
    global outstream
    global estarget
    #setup outputfile or elasticsearch indexing and then process data per line function
    if args.o:
        outstream=codecs.open(args.o,'w',encoding='utf-8')
    if args.same:
        args.tohost=args.host
    if args.tohost or args.same:
        if args.toindex:
            estarget=Elasticsearch([{'host':args.tohost}],port=args.toport)
            body=json.loads(process_mapping())
            estarget.indices.create(index=args.toindex,body=body)
        else:
            sys.stderr.write("toIndex/toType not set! aborting.\n")
            exit(1)
    else:
        return
    #nothing to do here, if we're here, that means: stdout

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='return field statistics of an ElasticSearch Search Index')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we print ldj to stdout.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-tohost',type=str,help='hostname or IP-Address of the ElasticSearch-node to use for ingesting the processed data.')
    parser.add_argument('-toport',type=int,default=9200,help='Port of the ElasticSearch-node to use for -tohost, default is 9200')
    parser.add_argument('-toindex',type=str,help='ElasticSearch Search Index to use to harvest data')
    parser.add_argument('-same', action='store_true',help='index new data on the same machine as -host|-port')
    parser.add_argument('-show_schemas', action='store_true',help='show the schemas defined in the sourcecode')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-i',type=str,help='Input file to process! Default is stdin if no arg is given. Faster than stdin because of multiprocessing!')
    parser.add_argument('-o',type=str,help='Output file to dump! Default is stdout if -in or -srchost is given is given')
    parser.add_argument('-schema',type=str,default="schemaorg",help='Select which schema to use! use -show_schemas to see the defined schemas')
    parser.add_argument('-entity',type=str,help='Select entity. Supported Entities: Person, CreativeWork')
    args=parser.parse_args()
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if args.schema:
        if args.schema in schematas:
            selectedschema=args.schema
        else:
            sys.stderr.write("schema doesn't exist!\n")
            sys.stderr.write("existing schemas:\n")
            schemas()
            exit(-1)
    
    if args.entity:
        entity=args.entity
    
    
    if args.show_schemas:
        print(schemas())
        exit(0)
        
    setup_output()
    count=0
    #first attempt to read vom -inf
    if args.i:
        with codecs.open(args.i,'r',encoding='utf-8') as f: #use with parameter to close stream after context
            for line in f:
                process_stuff(json.loads(line))
        #  done !
        if not outstream:
            exit(0)
    #if inf not set, than try elasticsearch
    if args.host:
        if args.index and args.type:
            es=Elasticsearch([{'host':args.host}],port=args.port)  
            try:
                page = es.search(
                    index = args.index,
                    doc_type = args.type,
                    scroll = '2m',
                    size = 1000)
            except elasticsearch.exceptions.NotFoundError:
                sys.stderr.write("not found: "+args.host+":"+args.port+"/"+args.index+"/"+args.type+"/_search\n")
                exit(-1)
            sid = page['_scroll_id']
            
            scroll_size = page['hits']['total']
            es_totalsize=scroll_size
            stats = {}
            for hits in page['hits']['hits']:
                process_stuff(hits)
            while (scroll_size > 0):
                pages = es.scroll(scroll_id = sid, scroll='2m')
                sid = pages['_scroll_id']
                scroll_size = len(pages['hits']['hits'])
                for hits in pages['hits']['hits']:
                    process_stuff(hits)
        else:
            sys.stderr.write("Error! no Index/Type set but -host! add -index and -type or disable -host if you read from stdin/file Aborting...\n")
    else: #oh noes, no elasticsearch input-setup. then we'll use stdin
        for line in input_stream:
            process_stuff(json.loads(line))
    if outstream:
        outstream.close()
        
