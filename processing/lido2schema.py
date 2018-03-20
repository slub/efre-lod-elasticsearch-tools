#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import sys
import argparse
#from getindex import eprint
from dpath.util import get 
from pprint import pprint
from multiprocessing import Pool, Lock, Manager
from functools import partial
from es2json import ArrayOrSingleValue
from es2json import eprint

lock=None


baseuri="http://data.slub-dresden.de/resources/hcn-"


schema = {
    "name"          :   "lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet/lido:appellationValue/_",
    "image"         :   "lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation/0/lido:linkResource",
    "url"           :   "lido:administrativeMetadata/lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink/_",
    "datePublished" :   "lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventDate/lido:displayDate/_",
    "license"       :   "lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsType/lido:conceptID/_",
    "citation"      :   "lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet/lido:relatedWork/lido:object/lido:objectNote/_",
    "comment"       :   "lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectMeasurementsWrap/lido:objectMeasurementsSet/lido:displayObjectMeasurements/_",
    "genre"         :   {
        "Text"      :   "lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term/_"
            },
    "comment"       :   "lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectMeasurementsWrap/lido:objectMeasurementsSet/lido:displayObjectMeasurements/_",
    "identifier"    :   "lido:lidoRecID/_",
    "author"        :   {"@id": "lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventActor/lido:actorInRole/lido:actor/lido:actorID/_"},
    "copyrightHolder" : {
        "name"          :   "lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsHolder/lido:legalBodyName/lido:appellationValue/_",
        "sameAs"        :   "lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsHolder/lido:legalBodyID/_"
            }#,
    #"placePublished" :  {
    #    "sameAs"        :   "lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventPlace/lido:place/lido:placeID/_",
    #    "name"          :   "lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventPlace/lido:place/lido:namePlaceSet/lido:appellationValue/_"
    #        }
}


def lido(record,target,attribut,path):
    try:
        if attribut not in target:
            if "@id" not in target:
                target[attribut]=get(record,path)
            else:
                target[attribut]="hcn-"+str(get(record,path))
    except:
        pass
    
def init(l):
    global lock
    lock = l

def checkids(record):
    removekeys=[]
    for _id in ["sameAs","@id"]:
        if _id in record:
            if " " in record[_id]:
                record.pop(_id)
    for key in ["author", "copyrightHolder", "placePublished","mentions"]:
        if key in record:
            if isinstance(record[key],list):
                for elem in record[key]:
                    elem=checkids(elem)
            elif isinstance(key,dict):
                record[key]=checkids(record[key])
    for k,v in record.items():
        if v:
            if isinstance(v,str):
                v=ArrayOrSingleValue(ArrayOrSingleValue(v))
            elif isinstance(v,list):
                for elem in v:
                    if not elem:
                        del elem
                    else:
                        elem=ArrayOrSingleValue(elem)
        elif not v:
            removekeys.append(k)
    if "copyrightHolder" in record:
        if "sameAs" in record["copyrightHolder"]:
            if record["copyrightHolder"]["sameAs"]=="http://d-nb.info/gnd/2002498-8":
                record["copyrightHolder"]["@id"]="http://data.slub-dresden.de/orga/191584568"
    for k in removekeys:
        record.pop(k)
    return record


def process_stuff(l, record):
        data=json.loads(record)
        target={}
        #1:1
        target["@context"]="http://schema.org"
        target["@type"]='http://schema.org/CreativeWork'
        
        for k,v in schema.items():
            if isinstance(v,dict):
                target[k]={}
                for c,w in v.items():
                    lido(data,target[k],c,w)
            elif isinstance(v,list):
                target[k]=[]
                for elem  in v:
                    temp={}
                    for c,w in elem.items():
                        lido(data,temp,c,w)
                    target[k].append(temp)
            elif isinstance(v,str):
                lido(data,target,k,v)
        #generate @id
        if "genre" in target:
            target["genre"]["@type"]="Text"
        _id=baseuri+str(target["identifier"].rsplit('-')[-1])
        target["@id"]=_id
        #bnodes 1:n
        target['mentions']=[]
        try:
            for i in get(data,"lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectConcept"):
                tag={}
                tag['sameAs']=get(i,"lido:conceptID/_")
                tag['name']=get(i,"lido:term")
                target['mentions'].append(tag)
        except:
            pass
        target=checkids(target)
        lock.acquire()
        sys.stdout.write(json.dumps(target)+"\n"),
        sys.stdout.flush()
        lock.release()

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='heidicon to schemaorg')
    args=parser.parse_args()
    
    m = Manager()
    l = m.Lock()
    pool = Pool(initializer=init,initargs=(l,))
    func = partial(process_stuff,l)
    pool.map(func,sys.stdin)
    pool.close()
    pool.join()
            #print(json.dumps(data,indent=4))
        
