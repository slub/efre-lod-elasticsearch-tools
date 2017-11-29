#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
#from getindex import eprint
from dpath.util import get 
from pprint import pprint
from multiprocessing import Pool, Lock, Manager
from functools import partial

lock=None

def lido(record,target,attribut,path):
    try:
        if attribut not in target:
            target[attribut]=get(record,path)
    except:
        pass
    
def init(l):
    global lock
    lock = l


def process_stuff(l, record):
        data=json.loads(record)
        target={}
        #1:1
        target["@context"]="http://schema.org"
        lido(data,target,"name",'lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet/lido:appellationValue/_')
        lido(data,target,"identifier",'lido:lidoRecID/_')
        lido(data,target,"image",'lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation/0/lido:linkResource')
        lido(data,target,"@id",'lido:administrativeMetadata/lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink/_')
        lido(data,target,"datepublished","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventDate/lido:displayDate/_")
        lido(data,target,"license","lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsType/lido:conceptID/_")
        lido(data,target,"citation","lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet/lido:relatedWork/lido:object/lido:objectNote/_")
        lido(data,target,"genre","lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term/_")
        lido(data,target,"comment",'lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectMeasurementsWrap/lido:objectMeasurementsSet/lido:displayObjectMeasurements/_')
        
        #bnodes 1:1
        target["mainEntity"]={"preferredName":"HeidICON : Die Heidelberger Bilddatenbank / Universitätsbibliothek Heidelberg","@id":"http://d-nb.info/104709214X"}
        target['author']={}
        lido(data,target['author'],"preferredName","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventActor/lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue/_")
        lido(data,target['author'],"sameAs","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventActor/lido:actorInRole/lido:actor/lido:actorID/_")
        
        target['copyrightHolder']={}
        lido(data,target['copyrightHolder'],"preferredName","lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsHolder/lido:legalBodyName/lido:appellationValue/_")
        lido(data,target['copyrightHolder'],"@id","lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsHolder/lido:legalBodyID/_")
        
        target['placePublished']={}
        lido(data,target['placePublished'],"@id","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventPlace/lido:place/lido:placeID/_")
        lido(data,target['placePublished'],"@preferredName","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventPlace/lido:place/lido:namePlaceSet/lido:appellationValue/_")
            
        #bnodes 1:n
        target['mentions']=[]
        try:
            for i in get(data,"lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectConcept"):
                tag={}
                tag['@id']=get(i,"lido:conceptID/_")
                tag['preferredName']=get(i,"lido:term")
                target['mentions'].append(tag)
        except:
            pass
        lock.acquire()
        sys.stdout.write(json.dumps(target)+"\n")
        lock.release()

if __name__ == "__main__":
    #parser=argparse.ArgumentParser(description='heidicon to schemaorg')
    #parser.add_argument('-i',type=str,help='Input file to process.')
    #args=parser.parse_args()
    
    m = Manager()
    l = m.Lock()
    pool = Pool(initializer=init,initargs=(l,))
    func = partial(process_stuff,l)
    pool.map(func,sys.stdin)
    pool.close()
    pool.join()
            #print(json.dumps(data,indent=4))
        
