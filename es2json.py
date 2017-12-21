#!/usr/bin/python3
from elasticsearch import Elasticsearch
import sys

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)   
    
def esgenerator(host=None,port=9200,index=None,type=None,body=None,source=True,headless=False):
    es=Elasticsearch([{'host':host}],port=port)
    try:
        page = es.search(
            index = index,
            doc_type = type,
            scroll = '2m',
            size = 1000,
            body = body,
            _source=source)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("not found: "+host+":"+port+"/"+index+"/"+type+"/_search\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    for hits in page['hits']['hits']:
        if headless:
            yield hits['_source']
        else:
            yield hits
    while (scroll_size > 0):
        pages = es.scroll(scroll_id = sid, scroll='2m')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        for hits in pages['hits']['hits']:
            if headless:
                yield hits['_source']
            else:
                yield hits
