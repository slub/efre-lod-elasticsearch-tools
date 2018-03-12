#!/usr/bin/python3 -Wd
# -*- coding: utf-8 -*-
from rdflib import URIRef
from uuid import uuid4
import json
#import urllib.request
import codecs
import argparse
import sys
import io
import os.path
import mmap
import requests
import siphash
import re
from es2json import esgenerator
from es2json import eprint
from es2json import ArrayOrSingleValue

def isint(num):
    try: 
        int(num)
        return True
    except:
        return False
    
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
        elif "eath" in schemakey: #(date of d|D)eath(Date)
            if len(dates)==2:
                return getiso8601(dates[1])
            elif len(dates)==1: 
                return None # still alive! congrats
        else:
            return date

def handlesex(record,key,entity):
    for v in key:
        marcvalue=getmarc(v,record,entity)
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

def gnd2uri(string):
    if isinstance(string,str):
        if "(DE-588)" in string:
            return "http://d-nb.info/gnd/"+string.split(')')[1]
        elif "(DE-576)" in string:
            return "http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN="+string.split(')')[1]
        else:
            return
    elif isinstance(string,list):
        ret=[]
        for st in string:
            ret.append(gnd2uri(st))
        return ret

def id2uri(string,entity):
    baseuri="http://data.slub-dresden.de/"

    if entity=="Person":
        return baseuri+"persons/"+string
    elif entity=="CreativeWork":
        return baseuri+"resource/"+string
    elif entity=="Organization":
        return baseuri+"organizations/"+string
    elif entity=="Place":
        return baseuri+"geo/"+string

def get_or_generate_id(record,entity):
    generate=True
    #set generate to True if you're 1st time filling a infrastructure from scratch!
    # replace args.host with your adlookup service :P
    if generate==True:
        identifier = None
    else:
        r=requests.get("http://"+args.host+":8000/welcome/default/data?feld=sameAs&uri="+gnd2uri("(DE-576)"+getmarc(record,"001",entity)))
        identifier=r.json().get("identifier")
    if identifier:
        return id2uri(identifier,entity)
    else:
        return id2uri(siphash.SipHash_2_4(b'slub-dresden.de/').update(uuid4().bytes).hexdigest().decode('utf-8').upper(),entity)

def get_type(record,entity):
    return "http://schema.org/"+str(entity)

        #                    :value
        #"@id"               :0,
        #"identifier"        :{getmarc:"001"},
        #"name"              :{getmarc:["245..a","245..b","245..n","245..p"]},
        #"gender"            :{handlesex:["375..a"]},
def process_field(record,value,entity):
    ret=[]
    if isinstance(value,dict):
        for function,parameter in value.items():
            ret.append(function(record,parameter,entity))
    elif isinstance(value,str):
        return value
    elif isinstance(value,list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record,elem,entity)))
    elif callable(value):
        return ArrayOrSingleValue(value(record,entity))
    if ret:
        return ArrayOrSingleValue(ret)
            
def getmarc(json,regex,entity):
    ret=None
    if isinstance(regex,list):
        for string in regex:
            if string[:3] in json:
                ret=litter(ret,getmarc(json,string,entity))
    elif isinstance(regex,str):
        if len(regex)==3 and regex in json:
            return json.get(regex)
        #eprint(regex+":\n"+str(json)+"\n\n") ### beware! hardcoded traverse algorithm for marcXchange json encoded data !!! ### temporary workaround: http://www.smart-jokes.org/programmers-say-vs-what-they-mean.html
        else:
            json=json.get(regex[:3]) # = [{'__': [{'a': 'g'}, {'b': 'n'}, {'c': 'i'}, {'q': 'f'}]}]
            if isinstance(json,list):  
                for elem in json:    
                    if isinstance(elem,dict):
                        for k,v in elem.items():
                            if isinstance(elem[k],list):
                                for final in elem[k]:
                                    for c,w in final.items():
                                        if c==regex[-1]:
                                            ret=litter(ret,w)
                                    if regex[-1] in final:
                                        ret=litter(ret,final[regex[-1]])
    if ret:
        return ArrayOrSingleValue(ret)

def relatedTo(jline,key,entity):
    data=[]
    sset=None
    if jline.get(key[:3]):
        for mrc_indicator in jline.get(key[:3]):
            for ind,subfields in mrc_indicator.items():
                sset={}
                person={}
                for element in subfields:
                    for key,value in element.items():
                        sset[key]=value
                for key, value in sset.items():
                    if key=='9':
                        if isinstance(value,list):
                            for val in value:
                                for k,v in marc2relation.items():
                                    if k.lower()==val:
                                        person["_key"]=v
                                        break
                            if not person.get("_key"):
                                for k, v in marc2relation.items():
                                    if k.lower() in val.lower():
                                        person["_key"]=v
                                        break  
                        elif isinstance(value,str):
                            for k,v in marc2relation.items():
                                if k.lower()==value:
                                    person["_key"]=v
                                    break
                            if not person.get("_key"):
                                for k, v in marc2relation.items():
                                    if k.lower() in value.lower():
                                        person["_key"]=v
                                        break  
                        if not person.get("_key"):
                                person["_key"]="knows"
                    elif key=='0':
                        person["sameAs"]=gnd2uri(value)
                if person.get("sameAs"):
                    data=litter(data,person) ###filling the array with the person(s)
    if data:
        return data

def areaServed(jline,key,entity):
    data=gnd2uri(getmarc(ArrayOrSingleValue(key),jline,entity))
    if data:
        return data

def birthPlace(jline,key,entity):
    return Place(jline.get(key[:3]),'4:ortg')

def deathPlace(jline,key,entity):
    return Place(jline.get(key[:3]),'4:orts')

def Place(record,event):
    data=None
    sset={}
    for c,w in traverse(record,""):
        if isinstance(w,dict):
            for mrc_indicator,ind_val in w.items():
                if isinstance(ind_val,list):
                    for subfield in ind_val:
                        if isinstance(subfield,dict):
                            for k,v in subfield.items():
                                if k=="0":
                                    sset[k]=gnd2uri(v)
                                else:
                                    sset[k]=v
                        if isinstance(subfield,str):
                            uri=gnd2uri(subfield)
                            if uri:
                                if not sset["0"]:
                                    sset["0"]=uri
                                elif uri not in sset["0"]:
                                    if isinstance(sset["0"],list):
                                        sset["0"].append(uri)
                                    elif isinstance(sset["0"],str):
                                        nochnenull=sset.pop("0")
                                        sset["0"]=[]
                                        sset["0"].append(nochnenull)
                                        sset["0"].append(uri)
    if event==sset.get("9") and sset.get("0"):
        if not data:
            data=[]    
        zero=ArrayOrSingleValue(sset.get("0"))
        if isinstance(zero,str):
            data.append({"sameAs":zero})
        elif isinstance(zero,list):
            for elem in zero:
                data.append({"sameAs":elem})
    if data:
        return ArrayOrSingleValue(data)

def deathDate(jline,key,entity):
    return marc_dates(jline.get(key),"deathDate")
    
def birthDate(jline,key,entity):
    return marc_dates(jline.get(key),"birthDate")

### avoid dublettes and nested lists when adding elements into lists
def litter(lst, elm):
    if not lst:
        lst=elm
    else:
        if isinstance(lst,str):
            lst=[lst]
        if isinstance(elm,str):
            if elm not in lst:
                lst.append(elm)
        elif isinstance(elm,list):
            for element in elm:
                if element not in lst:
                    lst.append(element)
    return lst

def marc_dates(record,event):
    data=None
    subset=None 
    recset={}
    if record:
        for indicator_level in record:
            for subfield in indicator_level:
                sset={}
                for sf_elem in indicator_level.get(subfield):
                    for k,v in sf_elem.items():
                        if k=="a" or k=="9":
                            sset[k]=v
                if isinstance(sset.get("9"),str):
                    recset[sset['9']]=sset.get("a")
                elif isinstance(sset.get("9"),list):
                    for elem in sset.get("9"):
                        if elem.startswith("4:dat"):
                            recset[elem]=sset.get("a")
    #eprint(recset)
    if recset.get("4:datx"):
        return dateToEvent(recset["4:datx"],event)
    elif recset.get("4:datl"):
        return dateToEvent(recset["4:datl"],event)
    else:
        return None
    
def honoricSuffix(jline,key,entity):
    data=None
    try:
        for i in jline[key[0][0:3]][0]:
            sset={}
            for j in jline[key[0][0:3]][0][i]:
                for k,v in dict(j).items():
                    sset[k]=v
                conti=False
                if "9" in sset:
                    if sset["9"]=='4:adel' or sset["9"]=='4:akad':
                            conti=True
                if conti and "a" in sset:
                    data=litter(data,sset["a"])
    except:
        pass
    if data:
        return data

def hasOccupation(jline,key,entity):
    try:
            data=[]
            for i in jline[key[:3]]:
                conti=False
                job=None
                sset={}
                for k,v in i.items():               # v = [{'0': ['(DE-576)210258373', '(DE-588)4219681-4']}, {'a': 'Romanist'}, {'9': '4:berc'}, {'w': 'r'}, {'i': 'Charakteristischer Beruf'}]
                    for w in v:                     # w = {'0': ['(DE-576)210258373', '(DE-588)4219681-4']}
                        for c,y in dict(w).items(): # c =0 y = ['(DE-576)210258373', '(DE-588)4219681-4']
                            sset[c]=y
                if "9" in sset:                     #sset = {'a': 'Romanist', 'w': 'r', '0': ['(DE-576)210258373', '(DE-588)4219681-4'], 'i': 'Charakteristischer Beruf', '9': '4:berc'}
                    if sset["9"]=='4:berc' or sset["9"]=='4:beru' or sset['9']=='4:akti':
                            conti=True
                if conti:
                    if key[-1] in sset:
                        if isinstance(sset[key[-1]],list):
                            for field in sset[key[-1]]:
                                if "(DE-588)" in field:
                                    data.append(gnd2uri(field))
    except:
        pass
    if data:
        return ArrayOrSingleValue(data)



def getgeo(arr):
    for k,v in traverse(arr,""):
        if isinstance(v,str):
            if '.' in v:
                return v


            #key : {"longitude":["034..d","034..e"],"latitude":["034..f","034..g"]}
def getGeoCoordinates(record,key,entity):
    ret={}
    for k,v in key.items():
        coord=getgeo(getmarc(record,v,entity))
        if coord:
            ret[k]=coord
    if ret:
        return ret

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
def check(ldj,entity):
    ldj=removeNone(removeEmpty(ldj))
    for k,v in ldj.items():
        v=ArrayOrSingleValue(v)
    if not ldj:
        return
    for person in ["author","contributor"]:
        if person in ldj:
            if isinstance(ldj[person],str):
                if "DE-576" in ldj[person]:
                    uri=gnd2uri(ldj.pop(person))
                    ldj[person]={"sameAs":uri}
                if "DE-588" in ldj[person]:
                    uri=gnd2uri(ldj.pop(person))
                    ldj[person]={"sameAs":uri}
            elif isinstance(ldj[person],list):
                persons=[]
                for author in ldj[person]:
                    if "DE-576" or "DE-588" in author:
                        persons.append({"sameAs":gnd2uri(author)})
                ldj.pop(person)
                ldj[person]=persons
    if 'genre' in ldj:
        genre=ldj.pop('genre')
        ldj['genre']={}
        ldj['genre']['@type']="Text"
        ldj['genre']["Text"]=genre
    if "identifier" in ldj:
        if "sameAs" in ldj and not isinstance(ldj["sameAs"],list):
            ldj["sameAs"]=[ldj.pop("sameAs")]
            ldj["sameAs"].append(gnd2uri("(DE-576)"+ldj.pop("identifier")))
        else:
            ldj["sameAs"]=gnd2uri("(DE-576)"+ldj.pop("identifier"))
        ldj["identifier"]=ldj["@id"].split("/")[4]
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
    if entity=="Person":
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
    for label in ["name","alternativeHeadline","alternateName"]:
        if label in ldj:
            if isinstance(ldj[label],str):
                if ldj[label][-2:]==" /":
                    ldj[label]=ldj[label][:-2]
            elif isinstance(ldj[label],list):
                for n,i in enumerate(ldj[label]):
                    if i[-2:]==" /":
                        ldj[label][n]=i[:-2]
    if "publisherImprint" in ldj:
        if not isinstance(ldj["@context"],list) and isinstance(ldj["@context"],str):
            ldj["@context"]=list([ldj.pop("@context")])
        ldj["@context"].append(URIRef(u'http://bib.schema.org/'))
    if "isbn" in ldj:
        ldj["@type"]=URIRef(u'http://schema.org/Book')
    if "issn" in ldj:
        ldj["@type"]=URIRef(u'http://schema.org/CreativeWorkSeries')
    return ldj

entities = {
   "CreativeWork":{
        "@id"               :get_or_generate_id,
        "@type"             :get_type,
        "@context"          :"http://schema.org",
        "identifier"        :{getmarc:"001"},
        "name"              :{getmarc:["245..a","245..b","245..n","245..p"]},
        "alternateName"     :{getmarc:["130..a","130..p","240..a","240..p","246..a","246..b","245..p","249..a","249..b","730..a","730..p","740..a","740..p","920..t"]},
        "author"            :{getmarc:"100..0"},
        "contributor"       :{getmarc:"700..0"},
        "publisher"         :{getmarc:["260..b","264..b"]},
        "datePublished"     :{getmarc:["260..c","264..c","362..a"]},
        "Thesis"            :{getmarc:["502..a","502..b","502..c","502..d"]},
        "issn"              :{getmarc:["022..a","022..y","022..z","029..a","490..x","730..x","773..x","776..x","780..x","785..x","800..x","810..x","811..x","830..x"]},
        "isbn"              :{getmarc:["022..a","022..z","776..z","780..z","785..z"]},
        "genre"             :{getmarc:"655..a"},
        "hasPart"           :{getmarc:"773..g"},
        "isPartOf"          :{getmarc:["773..t","773..s","773..a"]},
        "license"           :{getmarc:"540..a"},
        "inLanguage"        :{getmarc:["041..a","041..d","130..l","730..l"]},
        "numberOfPages"     :{getmarc:["300..a","300..b","300..c","300..d","300..e","300..f","300..g"]},
        "pageStart"         :{getmarc:"773..q"},
        "issueNumber"       :{getmarc:"773..l"},
        "volumeNumer"       :{getmarc:"773..v"}
        },
    "Person": {
        "@id"           :get_or_generate_id,
        "@context"      :"http://schema.org",
        "@type"         :get_type,
        "identifier"    :{getmarc:"001"},
        "name"          :{getmarc:"100..a"},
        "sameAs"        :{getmarc:"024..a"},
        "gender"        :{handlesex:["375..a"]},
        "alternateName" :{getmarc:["400..a","400..c"]},
        "relatedTo"     :{relatedTo:"500..0"},
        "hasOccupation" :{hasOccupation:"550..0"},
        "birthPlace"    :{birthPlace:"551"},
        "deathPlace"    :{deathPlace:"551"},
        "honoricSuffix" :{honoricSuffix:["550..0","550..00","550..i","550..a","550..9"]},
        "birthDate"     :{birthDate:"548"},
        "deathDate"     :{deathDate:"548"}
    },
    "Organization": {
        "@id"               :get_or_generate_id,
        "@type"             :get_type,
        "@context"          :"http://schema.org",
        "identifier"        :{getmarc:"001"},
        "name"              :{getmarc:"110..a"},
        "alternateName"     :{getmarc:["410..a","410..b"]},
        "sameAs"            :{getmarc:["024..a","670..u"]},
        "areaServed"        :{areaServed:["551..0"]}
        },
    "Place": {
        "@id"               :get_or_generate_id,
        "@type"             :get_type,
        "@context"          :"http://schema.org",
        "identifier"        :{getmarc:"001"},
        "name"              :{getmarc:"151..a"},
        "description"       :{getmarc:["551..0","551..i"]},
        "sameAs"            :{getmarc:"024..a"},
        "alternateName"     :{getmarc:"451..a"},
        "GeoCoordinates"    :{getGeoCoordinates:{"longitude":["034..d","034..e"],"latitude":["034..f","034..g"]}}
        }
}

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
            yield path + str([k]), v
            if isinstance(v, (dict, list)):
                for k, v in traverse(v, path + str([k])):
                    yield k, v

#processing a single line of json without whitespace 
def process_line(jline,elastic,outstream):
    snine=getmarc(jline,"079..b",None)
    entity=None
    if snine=="p": # invididualisierte Person
        entity="Person"
    elif snine=="n":
        return
    elif snine=="s":
        return
    elif snine=="b": # Körperschaft/Organisation
        entity="Organization"
    elif snine=="g": # Geographika
        entity="Place"
    elif snine is None: # n:Personennamef:Kongresse/s:Schlagwörter Nicht interessant
        entity="CreativeWork"
    else:
        return
    mapline={}
    mapline["@type"]=[]
    mapline["@type"].append(URIRef(u'http://schema.org/'+entity))
    mapline["@context"]=[URIRef(u'http://schema.org')]
    for key,val in entities[entity].items():
        value=process_field(jline,val,entity)
        if value:
            if "related" in key:
                if isinstance(value,dict) and "_key" in value:
                    dictkey=value.pop("_key")
                    mapline[dictkey]=value
                elif isinstance(value,list):
                    for elem in value:
                        if isinstance(elem,dict) and "_key" in elem:
                            relation=elem.pop("_key")
                            dictkey=relation
                            if dictkey not in mapline:
                                mapline[dictkey]=[elem]
                            else:
                                mapline[dictkey].appen
                        elif isinstance(elem,dict):
                            if key not in mapline:
                                mapline[key]=[elem]
                            else:
                                mapline[key].append(elem)
            else:
                mapline[key]=ArrayOrSingleValue(value)
    if mapline:
        mapline=check(mapline,entity)
        if elastic.host:
            mapline["url"]="http://"+elastic.host+":"+str(elastic.port)+"/"+elastic.index+"/"+elastic.type+"/"+getmarc(jline,"001",None)+"?pretty"
        if outstream:
            outstream[entity].write(json.dumps(mapline,indent=None)+"\n")
        else:
            sys.stdout.write(json.dumps(mapline,indent=None)+"\n")
            sys.stdout.flush()

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-prefix',type=str,default="",help='Prefix to use for output data')
    parser.add_argument('-debug',action="store_true",help='Dump processed Records to stdout (mostly used for debug-purposes)')
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    outstream=None
    filepaths=[]
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    if not args.debug:
        outstream={}
        for ent,mapping in entities.items():
            filepath=str(args.prefix+ent+"-records.ldj")
            filepaths.append(filepath)
            if os.path.isfile(filepath):
                outstream[ent]=open(filepath,"a")
            else:
                outstream[ent]=open(filepath,"w")
    if args.host: #if inf not set, than try elasticsearch
        if args.index and args.type:
            for hits in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True):
                process_line(hits,args,outstream)
        else:
            sys.stderr.write("Error! no Index/Type set but -host! add -index and -type or disable -host if you read from stdin/file Aborting...\n")
    else: #oh noes, no elasticsearch input-setup. then we'll use stdin
        for line in input_stream:
            process_line(json.loads(line),args,outstream)
    if not args.debug:
        for ent,mapping in outstream.items():
            mapping.close()
        for path in filepaths:
            if os.stat(path).st_size==0:
                os.remove(path)
            
