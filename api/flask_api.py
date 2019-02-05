#!/usr/bin/python3
import sys
from flask import Flask
from flask import jsonify
from flask import abort
from flask import request
from flask_restful import inputs

from elasticsearch import Elasticsearch

host=sys.argv[1]

app=Flask(__name__)

es=Elasticsearch([{'host':host}],port="9200",timeout=5)


@app.route('/')
def index():
    return 'LOD'

@app.route('/<index>',methods=['GET'])
def returnsearch(index):
    size_arg=request.args.get('size',default= 10, type = int)
    if index in ["persons","tags","geo","orga","works","resources","events"]:
        res=es.search(index=index,body={"_source":{"excludes":["_isil"]},"query":{"match_all":{}}},size=size_arg)
        retarray=[]
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return jsonify(retarray)
    else:
        abort(404)

@app.route('/<index>/<id>')
def return_doc(index,id):
    isParent=request.args.get('isAttr')
    size_arg=request.args.get('size',default= 10, type = int)
    if isParent is not None:
        indices=["persons","tags","geo","orga","works","resources","events"]
        #if index in indices:
            #indices.remove(index)
        search={"_source":{"excludes":["_isil"]},"query":{"query_string" : {"query":"\"http://data.slub-dresden.de/"+index+"/"+id+"\""}}}
        res=es.search(index=','.join(indices),body=search,size=size_arg)
        retarray=[]
        #retarray.append({"search":search})
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                if hit.get("_id")!=id:
                    retarray.append(hit.get("_source"))
        return jsonify(retarray)
    else:
        try:
            res=es.get(index=index,doc_type="schemaorg",id=id,_source_exclude="_isil")
            return jsonify(res.get("_source"))
        except:
            abort(404)

app.run(host=host,port=80,debug=True)
