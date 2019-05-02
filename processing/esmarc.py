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
import os.path
import re
import gzip
from es2json import esgenerator, esidfilegenerator, esfatgenerator, ArrayOrSingleValue, eprint, litter, isint
from swb_fix import marc2relation

es=None
entities=None
base_id=None
target_id=None
base_id_delimiter="="
#lookup_es=None




def main():
    #argstuff
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-id',type=str,help='map single document, given by id')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-z',action="store_true",help="use gzip compression on output data")
    parser.add_argument('-prefix',type=str,default="ldj/",help='Prefix to use for output data')
    parser.add_argument('-debug',action="store_true",help='Dump processed Records to stdout (mostly used for debug-purposes)')
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-pretty',action="store_true",default=False,help="output tabbed json")
    parser.add_argument('-w',type=int,default=8,help="how many processes to use")
    parser.add_argument('-idfile',type=str,help="path to a file with IDs to process")
    parser.add_argument('-query',type=str,default={},help='prefilter the data based on an elasticsearch-query')
    parser.add_argument('-base_id_src',type=str,default="http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",help="set up which base_id to use for sameAs. e.g. http://d-nb.info/gnd/xxx")
    parser.add_argument('-target_id',type=str,default="http://data.slub-dresden.de/",help="set up which target_id to use for @id. e.g. http://data.finc.info")
#    parser.add_argument('-lookup_host',type=str,help="Target or Lookup Elasticsearch-host, where the result data is going to be ingested to. Only used to lookup IDs (PPN) e.g. http://192.168.0.4:9200")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()        
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    if args.server or ( args.host and args.port ):
        es=elasticsearch.Elasticsearch([{"host":args.host}],port=args.port)
    global base_id
    global target_id
    base_id=args.base_id_src
    target_id=args.target_id
    if args.pretty:
        tabbing=4
    else:
        tabbing=None
        
    if args.host and args.index and args.type and args.id:
        json_record=None
        source=get_source_include_str()
        json_record=es.get_source(index=args.index,doc_type=args.type,id=args.id,_source=source)
        if json_record:
            print(json.dumps(process_line(json_record,args.host,args.port,args.index,args.type),indent=tabbing))
    elif args.host and args.index and args.type and args.idfile:
        setupoutput(args.prefix)
        pool = Pool(args.w,initializer=init_mp,initargs=(args.host,args.port,args.prefix,args.z))
        for ldj in esidfilegenerator(host=args.host,
                       port=args.port,
                       index=args.index,
                       type=args.type,
                       source=get_source_include_str(),
                       body=args.query,
                       idfile=args.idfile
                        ):
            pool.apply_async(worker,args=(ldj,))
        pool.close()
        pool.join()
    elif args.host and args.index and args.type and args.debug:
        init_mp(args.host,args.port,args.prefix,args.z)
        for ldj in esgenerator(host=args.host,
                       port=args.port,
                       index=args.index,
                       type=args.type,
                       source=get_source_include_str(),
                       headless=True,
                       body=args.query
                        ): 
            record = process_line(ldj,args.host,args.port,args.index,args.type)
            if record:
                for k in record:
                    print(json.dumps(record[k],indent=None))
    elif args.host and args.index and args.type : #if inf not set, than try elasticsearch
        setupoutput(args.prefix)
        pool = Pool(args.w,initializer=init_mp,initargs=(args.host,args.port,args.prefix,args.z))
        for ldj in esfatgenerator(host=args.host,
                       port=args.port,
                       index=args.index,
                       type=args.type,
                       source=get_source_include_str(),
                       body=args.query
                        ):
            pool.apply_async(worker,args=(ldj,))
        pool.close()
        pool.join()
    else: #oh noes, no elasticsearch input-setup. then we'll use stdin
        eprint("No host/port/index specified, trying stdin\n")
        init_mp("localhost","DEBUG","DEBUG","DEBUG")
        with io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8') as input_stream:
            for line in input_stream:
                ret=process_line(json.loads(line),"localhost",9200,"data","mrc")
                if isinstance(ret,dict):
                    for k,v in ret.items():
                        print(json.dumps(v,indent=tabbing))
    


def uniq(lst):
    last = object()
    for item in lst:
        if item == last:
            continue
        yield item
        last = item

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


def relatedTo(jline,key,entity):

    #e.g. split "551^4:orta" to 551 and orta
    marcfield=key[:3]
    data=[]
    if marcfield in jline:
        for array in jline[marcfield]:
            for k,v in array.items():
                sset={}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code]=subfield[subfield_code]
                #eprint(sset.get("9"),subfield4)
                if isinstance(sset.get("9"),str) and sset.get("9") in marc2relation:
                    node={}
                    node["_key"]=marc2relation[sset.get("9")]
                    if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith(base_id):
                            node["@id"]=id2uri(sset.get("0"),"persons")
                        elif isinstance(uri,str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"]=uri
                        elif isinstance(uri,str):
                            node["identifier"]=sset.get("0")
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem and elem.startswith(base_id):
                                    node["@id"]=id2uri(elem.split("=")[-1],"persons")
                                elif elem and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                else:
                                    node["identifier"]=litter(node["identifier"],elem)
                    if sset.get("a"):
                        node["name"]=sset.get("a")
                    data.append(node)
                elif isinstance(sset.get("9"),list):
                    node={}
                    for elem in sset.get("9"):
                        if elem.startswith("v"):
                            for k,v in marc2relation.items():
                                if k.lower() in elem.lower():
                                    node["_key"]=v
                                    break
                        elif [x for x in marc2relation if x.lower() in elem.lower()]:
                            for x in marc2relation:
                                if x.lower() in elem.lower():
                                    node["_key"]=marc2relation[x]
                        elif not node.get("_key"):
                            node["_key"]="relatedTo"
                        #eprint(elem,node)
                    if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith(base_id):
                            node["@id"]=id2uri(sset.get("0"),"persons")
                        elif isinstance(uri,str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"]=uri
                        elif isinstance(uri,str):
                            node["identifier"]=uri
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem and elem.startswith(base_id):
                                    node["@id"]=id2uri(elem.split("=")[-1],"persons")
                                elif elem and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                elif elem:
                                    node["identifier"]=litter(node["identifier"],elem)
                    if sset.get("a"):
                        node["name"]=sset.get("a")
                    data.append(node)
                    #eprint(node)
                    
        if data:
            return ArrayOrSingleValue(data)

isil2sameAs = {
    "(DE-627)":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
    "DE-627":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
    #"DE-576":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
    #"(DE-576)":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
    "DE-588":"http://d-nb.info/gnd/",
    "(DE-588)":"http://d-nb.info/gnd/",
    "DE-601":"http://gso.gbv.de/PPN?PPN=",
    "(DE-601)":"http://gso.gbv.de/PPN?PPN=",
    "DE-15":"Univeristätsbibliothek Leipzig",
    "DE-14":"Sächsische Landesbibliothek – Staats- und Universitätsbibliothek Dresden",
}


def gnd2uri(string):
    try:
        if isinstance(string,list):
            for n,uri in enumerate(string):
                string[n]=gnd2uri(uri)
            return string
        if string and "(DE-" in string:
            if isinstance(string,list):
                ret=[]
                for st in string:
                    ret.append(gnd2uri(st))
                return ret
            elif isinstance(string,str):
                return uri2url(string.split(')')[0][1:],string.split(')')[1])
    except:
        return 

def uri2url(isil,num):
    if isil and num and isil in isil2sameAs:
        return str(isil2sameAs.get(isil)+num)
    #else:
        #return str("("+isil+")"+num)    #bugfix for isil not be able to resolve for sameAs, so we give out the identifier-number

def id2uri(string,entity):
    global target_id
    if string.startswith(base_id):
        string=string.split(base_id_delimiter)[-1]
    #if entity=="resources":
    #    return "http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN="+string
    #else:
    if target_id and entity and string:
        return str(target_id+entity+"/"+string)
    
def getid(record,regex,entity):
    _id=getmarc(record,regex,entity)
    if _id:
        return id2uri(_id,entity)


def getisil(record,regex,entity):
    isil=getmarc(record,regex,entity)
    if isinstance(isil,str) and isil in isil2sameAs:
        return isil
    elif isinstance(isil,list):
        for item in isil:
            if item in isil2sameAs:
                return item

def getnumberofpages(record,regex,entity):
    nop=getmarc(record,regex,entity)
    try:
        if isinstance(nop,str):
            nop=[nop]
        if isinstance(nop,list):
            for number in nop:
                if "S." in number and isint(number.split('S.')[0].strip()):
                    nop=int(number.split('S.')[0])
                else:
                    nop=None
    except IndexError:
        pass
    except Exception as e:
        with open("error.txt","a") as err:
            print(e,file=err)
    return nop
    
def getgenre(record,regex,entity):
    genre=getmarc(record,regex,entity)
    if genre:
        return {"@type":"Text",
                "Text":genre}

def getisbn(record,regex,entity):
    isbns=getmarc(record,regex,entity)
    if isinstance(isbns,str):
        isbns=[isbns]
    elif isinstance(isbns,list):
        for i,isbn in enumerate(isbns):
            if "-" in isbn:
                isbns[i]=isbn.replace("-","")
            if " " in isbn:
                for part in isbn.rsplit(" "):
                    if isint(part):
                        isbns[i]=part
    
    if isbns:
        retarray=[]
        for isbn in isbns:
            if len(isbn)==10 or len(isbn)==13:
                retarray.append(isbn)
        return retarray

def getmarc(record,regex,entity):
    if "+" in regex:
        marcfield=regex[:3]
        if marcfield in record:
            subfields=regex.split(".")[-1].split("+")
            data=None
            for array in record.get(marcfield):
                for k,v in array.items():
                    sset={}
                    for subfield in v:
                        for subfield_code in subfield:
                            sset[subfield_code]=subfield[subfield_code]
                    fullstr=""
                    for sf in subfields:
                        if sf in sset:
                            if fullstr:
                                fullstr+=". "
                            if isinstance(sset[sf],str):
                                fullstr+=sset[sf]
                            elif isinstance(sset[sf],list):
                                fullstr+=". ".join(sset[sf])
                    if fullstr:
                        data=litter(data,fullstr)
            if data:
                return ArrayOrSingleValue(data)
    else:
        ret=[]
        if isinstance(regex,str):
            regex=[regex]
        for string in regex:
            if string[:3] in record:
                ret=litter(ret,ArrayOrSingleValue(list(getmarcvalues(record,string,entity))))
        if ret:
            if isinstance(ret,list):    #simple deduplizierung via uniq() 
                ret = list(uniq(ret))
            return ArrayOrSingleValue(ret)

#generator object to get marc values like "240.a" or "001". yield is used bc can be single or multi
def getmarcvalues(record,regex,entity):
        if len(regex)==3 and regex in record:
            yield record.get(regex)
        #eprint(regex+":\n"+str(record)+"\n\n") ### beware! hardcoded traverse algorithm for marcXchange record encoded data !!! ### temporary workaround: http://www.smart-jokes.org/programmers-say-vs-what-they-mean.html
        else:
            record=record.get(regex[:3]) # = [{'__': [{'a': 'g'}, {'b': 'n'}, {'c': 'i'}, {'q': 'f'}]}]
            if isinstance(record,list):  
                for elem in record:
                    if isinstance(elem,dict):
                        for k in elem:
                            if isinstance(elem[k],list):
                                for final in elem[k]:
                                    if regex[-1] in final:
                                        yield final.get(regex[-1])

def handle_about(jline,key,entity):
    ret=[]
    for k in key:
        if k=="936" or k=="084":
            data=getmarc(jline,k,None)
            if isinstance(data,list):
                for elem in data:
                    ret.append(handle_single_rvk(elem))
            elif isinstance(data,dict):
                ret.append(handle_single_rvk(data))
        elif k=="082" or k=="083":
            data=getmarc(jline,k+"..a",None)
            if isinstance(data,list):
                for elem in data:
                    if isinstance(elem,str):
                        ret.append(handle_single_ddc(elem))
                    elif isinstance(elem,list):
                        for final_ddc in elem:
                            ret.append(handle_single_ddc(final_ddc))
            elif isinstance(data,dict):
                ret.append(handle_single_ddc(data))
            elif isinstance(data,str):
                ret.append(handle_single_ddc(data))
        elif k=="655":
            data=get_subfield(jline,k,entity)
            if isinstance(data,dict):
                data=[data]
            if isinstance(data,list):
                for elem in data:
                    if elem.get("identifier"):
                        elem["value"]=elem.pop("identifier")
                    ret.append({"identifier":elem})
    if len(ret)>0:
        return ret
    else:
        return None
            
def handle_single_ddc(data):
    return {"identifier":{"@type"     :"PropertyValue",
                          "propertyID":"DDC",
                          "value"     :data},
                          "@id":"http://purl.org/NET/decimalised#c"+data[:3]}

def handle_single_rvk(data):
    sset={}
    record={}
    if "rv" in data:
        for subfield in data.get("rv"):
            for k,v in subfield.items():
                sset[k]=v
        if "0" in sset:
            record["sameAs"]=gnd2uri("(DE-576)"+sset.get("0"))
        if "a" in sset:
            record["@id"]="https://rvk.uni-regensburg.de/api/json/ancestors/"+sset.get("a")
            record["identifier"]={  "@type"     :"PropertyValue",
                                "propertyID":"RVK",
                                "value"     :sset.get("a")}
        if "b" in sset:
            record["name"]=sset.get("b")
        if "k" in sset:
            record["keywords"]=sset.get("k")
        return record
    

        
def get_subfield_if_4(jline,key,entity):
    #e.g. split "551^4:orta" to 551 and orta
    marcfield=key.rsplit("^")[0]
    subfield4=key.rsplit("^")[1]
    data=[]
    if marcfield in jline:
        for array in jline[marcfield]:
            for k,v in array.items():
                sset={}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code]=subfield[subfield_code]
                #eprint(sset.get("9"),subfield4)
                if "9" in sset and subfield4 in sset.get("9"):
                    node={}
                    if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith(base_id):
                            if "ort" in key:
                                node["@id"]=id2uri(sset.get("0"),"geo")
                            elif "adue" in key:
                                node["@id"]=id2uri(sset.get("0"),"organizations")
                        elif isinstance(uri,str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"]=uri
                        elif isinstance(uri,str):
                            node["identifier"]=uri
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem.startswith(base_id):
                                    if "ort" in key:
                                        node["@id"]=id2uri(elem.split("=")[-1],"geo")
                                    elif "adue" in key:
                                        node["@id"]=id2uri(elem.split("=")[-1],"organizations")
                                elif elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                else:
                                    node["identifier"]=litter(node["identifier"],elem)
                    if sset.get("a"):
                        node["name"]=sset.get("a")
                    data.append(node)
        if data:
            return ArrayOrSingleValue(data)

def get_subfields(jline,key,entity):
    data=[]
    if isinstance(key,list):
        for k in key:
           data=litter(data,get_subfield(jline,k,entity))
        return ArrayOrSingleValue(data)
    elif isinstance(key,str):
        return get_subfield(jline,key,entity)
    else:
        return


#whenever you call get_subfield add the MARC21 field to the if/elif/else switch/case thingy with the correct MARC21 field->entity mapping
def get_subfield(jline,key,entity):
    keymap={"100":"persons",
            "700":"persons",
            "500":"persons",
            "711":"events",
            "110":"organizations",
            "710":"organizations",
            "551":"geo",
            "830":"resources",
            "689":"topics",
            "550":"topics",
            "655":"topics",
            }
    entityType=keymap.get(key)
    data=[]
    if key in jline:
        for array in jline[key]:
            for k,v in array.items():
                sset={}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code]=subfield[subfield_code]
                #eprint(sset.get("9"),subfield4)
                node={}
                for typ in ["D","d"]:
                    if sset.get(typ):   #http://www.dnb.de/SharedDocs/Downloads/DE/DNB/wir/marc21VereinbarungDatentauschTeil1.pdf?__blob=publicationFile Seite 14
                        node["@type"]="http://schema.org/"
                        if sset.get(typ)=="p":
                            node["@type"]+="Person"
                            entityType="persons"
                        elif sset.get(typ)=="b":
                            node["@type"]+="Organization"
                            entityType="organizations"
                        elif sset.get(typ)=="f":
                            node["@type"]+="Event"
                            entityType="events"
                        elif sset.get(typ)=="u":
                            node["@type"]+="CreativeWork"
                        elif sset.get(typ)=="g":
                            node["@type"]+="Place"
                        else:
                            node.pop("@type")
                if entityType=="resources" and sset.get("w") and not sset.get("0"):
                    sset["0"]=sset.get("w")
                if sset.get("0"):
                        if isinstance(sset["0"],list) and entityType=="persons":
                            for n,elem in enumerate(sset["0"]):
                                if "DE-576" in elem:
                                    sset["0"].pop(n)
                        uri=gnd2uri(sset.get("0"))
                        #eprint(uri)
                        if isinstance(uri,str) and uri.startswith(base_id) and not entityType=="resources":
                            node["@id"]=id2uri(uri,entityType)
                        elif isinstance(uri,str) and uri.startswith(base_id) and entityType=="resources":
                            node["sameAs"]=base_id+id2uri(uri,entityType).split("/")[-1]
                        elif isinstance(uri,str) and uri.startswith("http") and not uri.startswith(base_id):
                            node["sameAs"]=uri
                        elif isinstance(uri,str):
                            node["identifier"]=uri
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if isinstance(elem,str) and elem.startswith(base_id):
                                    node["@id"]=id2uri(elem,entityType)
                                elif isinstance(elem,str) and elem.startswith("http") and not elem.startswith(base_id):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                elif elem:
                                    node["identifier"]=litter(node["identifier"],elem)
                if isinstance(sset.get("a"),str) and len(sset.get("a"))>1:
                    node["name"]=sset.get("a")
                elif isinstance(sset.get("a"),list):
                    for elem in sset.get("a"):
                        if len(elem)>1:
                            node["name"]=litter(node.get("name"),elem)
                            
                if sset.get("v") and entityType=="resources":
                    node["position"]=sset["v"]
                if sset.get("n") and entityType=="events":
                    node["position"]=sset["n"]
                for typ in ["D","d"]:
                    if sset.get(typ):   #http://www.dnb.de/SharedDocs/Downloads/DE/DNB/wir/marc21VereinbarungDatentauschTeil1.pdf?__blob=publicationFile Seite 14
                        node["@type"]="http://schema.org/"
                        if sset.get(typ)=="p":
                            node["@type"]+="Person"
                        elif sset.get(typ)=="b":
                            node["@type"]+="Organization"
                        elif sset.get(typ)=="f":
                            node["@type"]+="Event"
                        elif sset.get(typ)=="u":
                            node["@type"]+="CreativeWork"
                        elif sset.get(typ)=="g":
                            node["@type"]+="Place"
                        else:
                            node.pop("@type")
                            
                if node:
                    data=litter(data,node)
                    #data.append(node)
        if data:
            return  ArrayOrSingleValue(data)

def getsameAs(jline,keys,entity):
    sameAs=[]
    for key in keys:
        data=getmarc(jline,key,entity)
        if isinstance(data,list):
            for elem in data:
                if not "DE-576" in elem: #ignore old SWB id for root SameAs
                    data=gnd2uri(elem)
                    if isinstance(data,str):
                        if data.startswith("http"):
                            sameAs.append(data)
                    elif isinstance(data,list):
                        for elem in data:
                            if elem and elem.startswith("http"):
                                sameAs.append(data)
    return sameAs
        
def deathDate(jline,key,entity):
    return marc_dates(jline.get(key),"deathDate")

def birthDate(jline,key,entity):
    return marc_dates(jline.get(key),"birthDate")

def marc_dates(record,event):
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
                    sset["9"]=[sset.get("9")]
                if isinstance(sset.get("9"),list):
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

def honorificSuffix(jline,key,entity):
    data=None
    if key in jline:
        for subfield in jline[key]:
            for i in subfield:
                sset={}
                for j in subfield[i]:
                    for k,v in dict(j).items():
                        sset[k]=v
                    conti=False
                    if "9" in sset:
                        if sset["9"]=='4:adel' or sset["9"]=='4:akad':
                                conti=True
                    if conti and "a" in sset:
                        data=litter(data,sset["a"])
    if data:
        return data

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
            ret["@type"]="GeoCoordinates"
            ret[k]=coord.replace("N","").replace("S","-").replace("E","").replace("W","-")
    if ret:
        return ret

def getav_katalogbeta(record,key,entity):#key should be a string: 001
    retOffers=list()
    finc_id=getmarc(record,key[1],entity)
    branchCode=getmarc(record,key[0],entity)
    #eprint(branchCode,finc_id)
    if finc_id and isinstance(branchCode,str) and branchCode=="DE-14":
        branchCode=[branchCode]
    if finc_id and isinstance(branchCode,list):
        for bc in branchCode:
            if bc=="DE-14":
                retOffers.append({
           "@type": "Offer",
           "offeredBy": {
                "@id": "http://data.slub-dresden.de/organizations/195657810",
                "@type": "Library",
                "name": isil2sameAs.get(bc),
                "branchCode": "DE-14"
            },
           "availability": "https://katalogbeta.slub-dresden.de/id/"+finc_id
       })
    if retOffers:
        return retOffers
        

def getav(record,key,entity):
    retOffers=list()
    offers=getmarc(record,[0],entity)
    ppn=getmarc(record,key[1],entity)
    if isinstance(offers,str):
        offers=[offers]
    if ppn and isinstance(offers,list):
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


#make data more RDF-like and do some post-mapping cleanups and data-transformations
def check(ldj,entity):
    ldj=removeNone(removeEmpty(ldj))
    for k,v in ldj.items():
        v=ArrayOrSingleValue(v)
    if not ldj:
        return
    
    #if ldj.get("identifier") and ldj.get("_isil") and isil2sameAs.get(ldj.get("_isil")):
        #if "sameAs" in ldj and isinstance(ldj.get("sameAs"),str):
            #ldj["sameAs"]=[ldj["sameAs"]]
            #if entity!="resources":
                #ldj["sameAs"].append(uri2url(ldj["_isil"],ldj["identifier"]))
            #elif ldj.get("_ppn"):
                #ldj["sameAs"].append(uri2url(ldj["_isil"],ldj["_ppn"]))
        #elif "sameAs" in ldj and isinstance(ldj["sameAs"],list):
            #if entity!="resources":
                #ldj["sameAs"].append(uri2url(ldj["_isil"],ldj["identifier"]))
            #elif ldj.get("_ppn"):
                #ldj["sameAs"].append(uri2url(ldj["_isil"],ldj["_ppn"]))
        #else:
            #if entity!="resources":
                #ldj["sameAs"]=[uri2url(ldj["_isil"],ldj["identifier"])]
            #elif ldj.get("_ppn"):
                #ldj["sameAs"]=[uri2url(ldj["_isil"],ldj["_ppn"])]
   
    for label in ["name","alternativeHeadline","alternateName","nameSub"]:
        if isinstance(ldj.get(label),str):
            if ldj[label][-2:]==" /":
                ldj[label]=ldj[label][:-2]
        elif isinstance(ldj.get(label),list):
            for n,i in enumerate(ldj[label]):
                if i[-2:]==" /":
                    ldj[label][n]=i[:-2]
            if label=="name":
                ldj[label]=" ".join(ldj[label])
                        
    if "publisherImprint" in ldj:
        if not isinstance(ldj["@context"],list) and isinstance(ldj["@context"],str):
            ldj["@context"]=list([ldj.pop("@context")])
        ldj["@context"].append(URIRef(u'http://bib.schema.org/'))
    if "isbn" in ldj:
        ldj["@type"]=URIRef(u'http://schema.org/Book')
    if "issn" in ldj:
        ldj["@type"]=URIRef(u'http://schema.org/CreativeWorkSeries')
    if "pub_name" in ldj or "pub_place" in ldj:
        ldj["publisher"]={}
        if "pub_name" in ldj:
            ldj["publisher"]["name"]=ldj.pop("pub_name")
            ldj["publisher"]["@type"]="Organization"
        if "pub_place" in ldj:
            ldj["publisher"]["location"]={"name":ldj.pop("pub_place"),
                                          "type":"Place"}
            if ldj["publisher"]["location"]["name"][-1] in [".",",",";",":"]:
                ldj["publisher"]["location"]["name"]=ldj["publisher"]["location"]["name"][:-1].strip()
        for value in ["name","location"]:        # LOD-JIRA Ticket #105
            if ldj.get("publisher") and ldj.get("publisher").get(value) and isinstance(ldj.get("publisher").get(value),str)and ldj.get("publisher").get(value)[-1] in [",",":",";"]:
                ldj["publisher"][value]=ldj.get("publisher").get(value)[:-1].strip()
    if "sameAs" in ldj:
        ldj["sameAs"]=removeNone(cleanup_sameAs(ldj.pop("sameAs")))
    return single_or_multi(ldj,entity)

def single_or_multi(ldj,entity):
    for k in entities[entity]:
        for key,value in ldj.items():
            if key in k:
                if "single" in k:
                    ldj[key]=ArrayOrSingleValue(value)
                elif "multi" in k:
                    if not isinstance(value,list):
                        ldj[key]=[value]
                else:
                    continue
    return ldj

def cleanup_sameAs(sameAs):
    if isinstance(sameAs,list):
        for n,elem in enumerate(sameAs):
            sameAs[n]=cleanup_sameAs(elem)
    elif isinstance(sameAs,str):
        if not sameAs.strip().startswith("http"):
            return None
    return sameAs
        

map_entities={
        "p":"persons",      #Personen, individualisiert
        "n":"persons",      #Personen, namen, nicht individualisiert
        "s":"topics",        #Schlagwörter/Berufe
        "b":"organizations",         #Organisationen
        "g":"geo",          #Geographika
        "u":"works",     #Werktiteldaten
        "f":"events"
}

def getentity(record):
    zerosevenninedotb=getmarc(record,"079..b",None)
    if zerosevenninedotb in map_entities:
        return map_entities[zerosevenninedotb]
    elif not zerosevenninedotb:
        return "resources"  # Titeldaten ;)
    else:
        return

def getdateModified(record,key,entity):
    date=getmarc(record,key,entity)
    newdate=""
    if date:
        for i in range(0,13,2):
            if isint(date[i:i+2]):
                newdate+=date[i:i+2]
            else:
                newdate+="00"
            if i in (2,4):
                newdate+="-"
            elif i==6:
                newdate+="T"
            elif i in (8,10):
                newdate+=":"
            elif i==12:
                newdate+="Z"
        return newdate


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


def get_source_include_str():
    items=set()
    items.add("079")
    for k,v in traverse(entities,""):
        #eprint(k,v)
        if isinstance(v,str) and isint(v[:3]) and v not in items:
                items.add(v[:3])
    _source=",".join(items)
    #eprint(_source)
    return _source
    
def process_field(record,value,entity):
    ret=[]
    if isinstance(value,dict):
        for function,parameter in value.items():
            ret.append(function(record,parameter,entity))   #Function with parameters defined in mapping
    elif isinstance(value,str):
        return value
    elif isinstance(value,list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record,elem,entity)))
    elif callable(value):
        return ArrayOrSingleValue(value(record,entity)) #Function without paremeters defined in mapping
    if ret:
        return ArrayOrSingleValue(ret)


#processing a single line of json without whitespace 
def process_line(jline,host,port,index,type):
    entity=getentity(jline)
    if entity:
        mapline={}
        mapline["@type"]=[URIRef(u'http://schema.org/'+entity)]
        mapline["@context"]=[URIRef(u'http://schema.org')]
        for sortkey,val in entities[entity].items():
            key=sortkey.split(":")[1]
            value=process_field(jline,val,entity)
            if value:
                if "related" in key and  isinstance(value,dict) and "_key" in value:
                    dictkey=value.pop("_key")
                    mapline[dictkey]=litter(mapline.get(dictkey),value)
                elif "related" in key and isinstance(value,list):
                    for elem in value:
                        if "_key" in elem:
                            relation=elem.pop("_key")
                            dictkey=relation
                            mapline[dictkey]=litter(mapline.get(dictkey),elem)
                else:
                    mapline[key]=value
        mapline=check(mapline,entity)
        if index:
            mapline["isBasedOn"]=target_id+"source/"+index+"/"+getmarc(jline,"001",None)
        return {entity:mapline}
    


def setupoutput(prefix):
    if prefix:
        if not os.path.isdir(prefix):
            os.mkdir(prefix)
        if not prefix[-1]=="/":
            prefix+="/"
    else:
        prefix=""
    for entity in entities:
        if not os.path.isdir(prefix+entity):
            os.mkdir(prefix+entity)

def init_mp(h,p,pr,z):
    global host
    global port
    global prefix
    global comp
    if not pr:
        prefix = ""
    elif pr[-1]!="/":
        prefix=pr+"/"
    else:
        prefix=pr
    comp=z
    port = p
    host = h

def worker(ldj):
    #out={}
    #for entity in entities:
    #    out[entity]=open(prefix+entity+"/"+str(current_process().name)+"-records.ldj","a")
    try:
        if isinstance(ldj,dict):
            ldj=[ldj]
        if isinstance(ldj,list):    # list of records
            for source_record in ldj:
                target_record=process_line(source_record.pop("_source"),host,port,source_record.pop("_index"),source_record.pop("_type"))
                if target_record:
                    for entity in target_record:
                        name=prefix+entity+"/"+str(current_process().name)+"-records.ldj"
                        if comp:
                            opener=gzip.open
                            name+=".gz"
                        else:
                            opener=open
                        with opener(name,"at") as out:
                            print(json.dumps(target_record[entity],indent=None),file=out)
    except Exception as e:
        with open("errors.txt",'a') as f:
            traceback.print_exc(file=f)
        
        
"""Mapping:
 a dict() (json) like table with function pointers
 entitites={"entity_types:{"single_or_multi:target":"string",
                           "single_or_multi:target":{function:"source"},
                           "single_or_multi:target:function}
                           }
 
"""




entities = {
   "resources":{   # mapping is 1:1 like works
        "single:@type"                     :"CreativeWork",
        "single:@context"                  :"http://schema.org",
        "single:@id"                       :{getid:"001"},
        "single:identifier"                :{getmarc:["001"]},
#       "single:offers"                    :{getav:["852..a","980..a"]}, for SLUB and UBL via broken UBL DAIA-API
        "single:offers"                    :{getav_katalogbeta:["852..a","001"]}, #for SLUB via katalogbeta
        "single:_isil"                     :{getisil:["003","852..a","924..b"]},
        "single:_ppn"                      :{getmarc:"980..a"},
        "single:_sourceID"                 :{getmarc:"980..b"},
        "single:dateModified"              :{getdateModified:"005"},
        "multi:sameAs"                     :{getsameAs:["035..a","670..u"]},
        "single:name"                      :{getmarc:["245..a","245..b"]},
        "single:nameShort"                 :{getmarc:"245..a"},
        "single:nameSub"                   :{getmarc:"245..b"},
        "single:alternativeHeadline"       :{getmarc:["245..c"]},
        "multi:alternateName"              :{getmarc:["240..a","240..p","246..a","246..b","245..p","249..a","249..b","730..a","730..p","740..a","740..p","920..t"]},
        "multi:author"                     :{get_subfields:["100","110"]},
        "multi:contributor"                :{get_subfields:["700","710"]},
        "single:pub_name"                  :{getmarc:["260..b","264..b"]},
        "single:pub_place"                 :{getmarc:["260..a","264..a"]},
        "single:datePublished"             :{getmarc:["130..f","260..c","264..c","362..a"]},
        "single:Thesis"                    :{getmarc:["502..a","502..b","502..c","502..d"]},
        "multi:issn"                       :{getmarc:["022..a","022..y","022..z","029..a","490..x","730..x","773..x","776..x","780..x","785..x","800..x","810..x","811..x","830..x"]},
        "multi:isbn"                       :{getisbn:["020..a","022..a","022..z","776..z","780..z","785..z"]},
        "multi:genre"                      :{getgenre:"655..a"},
        "multi:hasPart"                    :{getmarc:"773..g"},
        "multi:isPartOf"                   :{getmarc:["773..t","773..s","773..a"]}, 
        "multi:partOfSeries"               :{get_subfield:"830"},
        "single:license"                   :{getmarc:"540..a"},
        "multi:inLanguage"                 :{getmarc:["377..a","041..a","041..d","130..l","730..l"]},
        "single:numberOfPages"             :{getnumberofpages:["300..a","300..b","300..c","300..d","300..e","300..f","300..g"]},
        "single:pageStart"                 :{getmarc:"773..q"},
        "single:issueNumber"               :{getmarc:"773..l"},
        "single:volumeNumer"               :{getmarc:"773..v"},
        "multi:locationCreated"            :{get_subfield_if_4:"551^4:orth"},
        "multi:relatedTo"                  :{relatedTo:"500..0"},
        "multi:about"                      :{handle_about:["936","084","083","082","655"]},
        "single:description"               :{getmarc:"520..a"},
        "multi:mentions"                   :{get_subfield:"689"},
        "multi:relatedEvent"               :{get_subfield:"711"}
        },
    "works":{   
        "single:@type"             :"CreativeWork",
        "single:@context"      :"http://schema.org",
        "single:@id"           :{getid:"001"},
        "single:identifier"    :{getmarc:"001"},
        "single:_isil"         :{getisil:"003"},
        "single:dateModified"  :{getdateModified:"005"},
        "multi:sameAs"         :{getsameAs:["035..a","670..u"]},
        "multi:name"           :{getmarc:["100..t"]},
        "single:alternativeHeadline"      :{getmarc:["245..c"]},
        "multi:alternateName"     :{getmarc:["400..t","240..a","240..p","246..a","246..b","245..p","249..a","249..b","730..a","730..p","740..a","740..p","920..t"]},
        "multi:author"            :{get_subfield:"500"},
        "multi:contributor"       :{get_subfield:"700"},
        "single:pub_name"          :{getmarc:["260..b","264..b"]},
        "single:pub_place"         :{getmarc:["260..a","264..a"]},
        "single:datePublished"     :{getmarc:["130..f","260..c","264..c","362..a"]},
        "single:Thesis"            :{getmarc:["502..a","502..b","502..c","502..d"]},
        "multi:issn"              :{getmarc:["022..a","022..y","022..z","029..a","490..x","730..x","773..x","776..x","780..x","785..x","800..x","810..x","811..x","830..x"]},
        "multi:isbn"              :{getmarc:["020..a","022..a","022..z","776..z","780..z","785..z"]},
        "single:genre"             :{getmarc:"655..a"},
        "single:hasPart"           :{getmarc:"773..g"},
        "single:isPartOf"          :{getmarc:["773..t","773..s","773..a"]},
        "single:license"           :{getmarc:"540..a"},
        "multi:inLanguage"        :{getmarc:["377..a","041..a","041..d","130..l","730..l"]},
        "single:numberOfPages"     :{getnumberofpages:["300..a","300..b","300..c","300..d","300..e","300..f","300..g"]},
        "single:pageStart"         :{getmarc:"773..q"},
        "single:issueNumber"       :{getmarc:"773..l"},
        "single:volumeNumer"       :{getmarc:"773..v"},
        "single:locationCreated"   :{get_subfield_if_4:"551^4:orth"},
        "single:relatedTo"         :{relatedTo:"500..0"}
        },
    "persons": {
        "single:@type"             :"Person",
        "single:@context"      :"http://schema.org",
        "single:@id"           :{getid:"001"},
        "single:identifier"    :{getmarc:"001"},
        "single:_isil"         :{getisil:"003"},
        "single:dateModified"   :{getdateModified:"005"},
        "multi:sameAs"         :{getsameAs:["035..a","670..u"]},
        
        "single:name"          :{getmarc:"100..a"},
        "single:gender"        :{handlesex:"375..a"},
        "multi:alternateName" :{getmarc:["400..a","400..c"]},
        "single:relatedTo"     :{relatedTo:"500..0"},
        "single:hasOccupation" :{get_subfield:"550"},
        "single:birthPlace"    :{get_subfield_if_4:"551^4:ortg"},
        "single:deathPlace"    :{get_subfield_if_4:"551^4:orts"},
        "single:honorificSuffix" :{honorificSuffix:"550"},
        "single:birthDate"     :{birthDate:"548"},
        "single:deathDate"     :{deathDate:"548"},
        "single:workLocation"  :{get_subfield_if_4:"551^4:ortw"},
        "single:about"                     :{handle_about:["936","084","083","082","655"]},
    },
    "organizations": {
        "single:@type"             :"Organization",
        "single:@context"      :"http://schema.org",
        "single:@id"           :{getid:"001"},
        "single:identifier"    :{getmarc:"001"},
        "single:_isil"         :{getisil:"003"},
        "single:dateModified"   :{getdateModified:"005"},
        "multi:sameAs"         :{getsameAs:["035..a","670..u"]},
        
        "single:name"              :{getmarc:"110..a+b"},
        "multi:alternateName"     :{getmarc:"410..a+b"},
        
        "single:additionalType"    :{get_subfield_if_4:"550^4:obin"},
        "single:parentOrganization":{get_subfield_if_4:"551^4:adue"},
        "single:location"          :{get_subfield_if_4:"551^4:orta"}, 
        "single:fromLocation"      :{get_subfield_if_4:"551^4:geoa"},
        "single:areaServed"        :{get_subfield_if_4:"551^4:geow"},
        "multi:about"                     :{handle_about:["936","084","083","082","655"]},
        },
    "geo": {
        "single:@type"             :"Place",
        "single:@context"      :"http://schema.org",
        "single:@id"           :{getid:"001"},
        "single:identifier"    :{getmarc:"001"},
        "single:_isil"         :{getisil:"003"},
        "single:dateModified"   :{getdateModified:"005"},
        "multi:sameAs"         :{getsameAs:["035..a","670..u"]},
        
        "single:name"              :{getmarc:"151..a"},
        "multi:alternateName"     :{getmarc:"451..a"},
        "single:description"       :{get_subfield:"551"},
        "single:geo"               :{getGeoCoordinates:{"longitude":["034..d","034..e"],"latitude":["034..f","034..g"]}},
        "single:adressRegion"      :{getmarc:"043..c"},
        "multi:about"             :{handle_about:["936","084","083","082","655"]},
        },
    "topics":{         
        "single:@type"             :"Thing",
        "single:@context"      :"http://schema.org",
        "single:@id"           :{getid:"001"},
        "single:identifier"    :{getmarc:"001"},
        "single:_isil"         :{getisil:"003"},
        "single:dateModified"   :{getdateModified:"005"},
        "multi:sameAs"         :{getsameAs:["035..a","670..u"]},
        "single:name"              :{getmarc:"150..a"},
        "multi:alternateName"     :{getmarc:"450..a+x"},
        "single:description"       :{getmarc:"679..a"},
        "single:additionalType"    :{get_subfield:"550"},
        "single:location"          :{get_subfield_if_4:"551^4:orta"},
        "single:fromLocation"      :{get_subfield_if_4:"551^4:geoa"},
        "single:areaServed"        :{get_subfield_if_4:"551^4:geow"},
        "single:contentLocation"   :{get_subfield_if_4:"551^4:punk"},
        "single:participant"       :{get_subfield_if_4:"551^4:bete"},
        "single:relatedTo"         :{get_subfield_if_4:"551^4:vbal"},
        "multi:about"             :{handle_about:["936","084","083","082","655"]},
        },
    
    "events": {
        "single:@type"         :"Event",
        "single:@context"      :"http://schema.org",
        "single:@id"           :{getid:"001"},
        "single:identifier"    :{getmarc:"001"},
        "single:_isil"         :{getisil:"003"},
        "single:dateModified"   :{getdateModified:"005"},
        "multi:sameAs"         :{getsameAs:["035..a","670..u"]},
        
        "single:name"          :{getmarc:["111..a"]},
        "multi:alternateName" :{getmarc:["411..a"]},
        "single:location"      :{get_subfield_if_4:"551^4:ortv"},
        "single:startDate"     :{birthDate:"548"},
        "single:endDate"       :{deathDate:"548"},
        "single:adressRegion"  :{getmarc:"043..c"},
        "multi:about"         :{handle_about:["936","084","083","082","655"]},
    },
}


if __name__ == "__main__":
    main()

