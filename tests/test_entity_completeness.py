#!/usr/bin/python3

import sys
from elasticsearch import Elasticsearch


es=Elasticsearch([{'host':"127.0.0.1"}],port=9200)

map_entities={
    "p":"persons",      #Personen, individualisiert
    "n":"persons",      #Personen, namen, nicht individualisiert
    "s":"topics",        #Schlagwörter/Berufe
    "b":"organizations",         #Organisationen
    "g":"geo",          #Geographika
    "u":"works",     #Werktiteldaten
    "f":"events"
}

person_count_raw=0
person_count_map=0
for k,v in map_entities.items():
    result_raw=es.count(index="swb-aut",doc_type="mrc",body={"query":{"match":{"079.__.b.keyword":k}}})
    result_map=es.count(index=v,doc_type="schemaorg")
    if v=="persons":
        person_count_raw+=result_raw.get("count")
        person_count_map=result_map.get("count")
    else:
        if result_raw.get("count") == result_map.get("count") and result_map.get("count")>0:
            print(v, "is ok with ",result_map.get("count"),"records")
        else:
            print(v, "is NOT ok with ",result_raw.get("count"),"raw records and",result_map.get("count"),"mapped records")
if person_count_raw > 0 and person_count_raw == person_count_map:
    print("persons is ok with",person_count_map,"records")
else:
    print("persons is NOT ok with",person_count_raw,"raw records and",person_count_map,"mapped records")
    
    
