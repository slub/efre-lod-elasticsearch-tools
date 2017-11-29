#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
#from getindex import eprint
from dpath.util import get 
from pprint import pprint

def lido(record,target,attribut,path):
    try:
        if attribut not in target:
            target[attribut]=get(record,path)
    except:
        #pprint(record["lido:descriptiveMetadata"]["lido:eventWrap"])
        return
    

if __name__ == "__main__":
    #parser=argparse.ArgumentParser(description='heidicon to schemaorg')
    #parser.add_argument('-i',type=str,help='Input file to process.')
    #args=parser.parse_args()
    for record in sys.stdin:
        data=json.loads(record)
        target={}
        target["@context"]="http://schema.org"
        lido(data,target,"name",'lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet/lido:appellationValue/_')
        lido(data,target,"identifier",'lido:lidoRecID/_')
        lido(data,target,"image",'lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation/0/lido:linkResource')
        lido(data,target,"@id",'lido:administrativeMetadata/lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink/_')
        lido(data,target,"datepublished","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventDate/lido:displayDate/_")
        lido(data,target,"license","lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsType/lido:conceptID/_")
        lido(data,target,"citation","lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet/lido:relatedWork/lido:object/lido:objectNote/_")
        lido(data,target,"genre","lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term/_")
        
        #bnodes
        target["mainEntity"]={"preferredName":"HeidICON : Die Heidelberger Bilddatenbank / Universitätsbibliothek Heidelberg","@id":"http://d-nb.info/104709214X"}
        target['author']={}
        lido(data,target['author'],"preferredName","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventActor/lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue/_")
        lido(data,target['author'],"sameAs","lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventActor/lido:actorInRole/lido:actor/lido:actorID/_")
        
        target['copyrightHolder']={}
        lido(data,target['copyrightHolder'],"preferredName","lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsHolder/lido:legalBodyName/lido:appellationValue/_")
        lido(data,target['copyrightHolder'],"sameAs","lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsHolder/lido:legalBodyID/_")
        
        target['placePublished']={}
        
        
        #pprint(data)
        try:
            target["physical"]=get(data,'lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectMeasurementsWrap/lido:objectMeasurementsSet/lido:displayObjectMeasurements/_')
        except:
            pass
            #print(json.dumps(data,indent=4))
        pprint(target)
        
        
