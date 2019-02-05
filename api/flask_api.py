import json
from flask import Flask
from flask import jsonify
from flask import abort
from flask import request
from flask import Response
from flask import redirect
from flask_restful import inputs
from rdflib import ConjunctiveGraph,Graph
from elasticsearch import Elasticsearch

host="127.0.0.1"

app=Flask(__name__)

es=Elasticsearch([{'host':host}],port="9200",timeout=5)


# reserialize the data to other RDF implementations to output
def output(data,format):
    if not data:
        abort(404)
    elif format=="n3":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format='nt').decode('utf-8')
        return Response(data,mimetype='text/plain')
    elif format=="rdfxml":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format="application/rdf+xml").decode('utf-8')
        return Response(data,mimetype='application/rdf+xml')
    elif format=="turtle":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format='turtle').decode('utf-8')
        return Response(data,mimetype='text/plain')
    elif format=="nquads":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format='nquads').decode('utf-8')
        return Response(data,mimetype='application/n-quads')
    else:
        return jsonify(data)

@app.route('/')
def index():
    return redirect("https://github.com/efre-lod/efre-lod-elasticsearch-tools/blob/master/api/flask_api.py")

@app.route('/<index>',methods=['GET'])
def returnsearch(index):
    retarray=[]
    format=request.args.get('format',default='json',type=str)
    size_arg=request.args.get('size',default= 10, type = int)
    if index in ["persons","tags","geo","orga","works","resources","events"]:
        res=es.search(index=index,body={"_source":{"excludes":["_isil"]},"query":{"match_all":{}}},size=size_arg)
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
    else:
        abort(404)
    return output(retarray,format)


#returns an single document given by index or id. if you use /index/search, then you can execute simple searches
@app.route('/<index>/<id>',methods=['GET'])
def return_doc(index,id):
    retarray=[]
    query=request.args.get('q',type=str)
    format=request.args.get('format',default='json',type=str)
    isParent=request.args.get('isAttr')
    size_arg=request.args.get('size',default= 10, type = int)
    if isParent is not None:
        indices=["persons","tags","geo","orga","works","resources","events"]
        #if index in indices:
            #indices.remove(index)
        search={"_source":{"excludes":["_isil"]},"query":{"query_string" : {"query":"\"http://data.slub-dresden.de/"+index+"/"+id+"\""}}}
        res=es.search(index=','.join(indices),body=search,size=size_arg)
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                if hit.get("_id")!=id:
                    retarray.append(hit.get("_source"))
    elif id=="search" and query is not None:
        if index in ["persons","tags","geo","orga","works","resources","events"]:
            search={"_source":{"excludes":["_isil"]},"query":{"query_string" : {"query":query}}}
            res=es.search(index=index,body=search,size=size_arg)
            if "hits" in res and "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    retarray.append(hit.get("_source"))
        else:
            abort(404)
        
    else:
        try:
            res=es.get(index=index,doc_type="schemaorg",id=id,_source_exclude="_isil")
            retarray.append(res.get("_source"))
        except:
            abort(404)
    return output(retarray,format)
        

app.run(host=host,port=80,debug=True)
