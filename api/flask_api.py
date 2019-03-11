# API-Grundlagen
# 
# HTTP Get
# Personen:
# http://data.slub-dresden.de/persons/161142842
# 
# Organisationen:
# http://data.slub-dresden.de/orga/195657810
# 
# Geographika:
# http://data.slub-dresden.de/geo/20890140X
# 
# Ressourcen:
# http://data.slub-dresden.de/resources/finc-63-9783486711608
# 
# Stichwörter:
# http://data.slub-dresden.de/tags/213180294
# 
# Konferenzen:
# http://data.slub-dresden.de/events/197036880
# 
# Werktitel:
# http://data.slub-dresden.de/works/135949343
# 
# 
# Attributsuche:
# 
# mit dem Paramter isAttr wird nach allen Records gesucht, die den gegebenen Wert als Attribut haben:
# http://data.slub-dresden.de/persons/164292160?isAttr
# 
# 
# Abfragemöglichkeiten
# 
# Suche über alle Felder
# http://data.slub-dresden.de/resources/search?q=Märchen
# 
# Einfache Feldsuche: 
# http://data.slub-dresden.de/resources/search?q=name:Märchen
# 
# Verschachtelte Feldsuche:
# http://data.slub-dresden.de/resources/search?q=contributor.name:Rettich
# 
# 
# 
# 
# Inhaltstypen
# 
# Über Query-parameter "format" (Werte: json, turtle, rdfxml, n3), standardmäßig json:
# 
# http://data.slub-dresden.de/orga/195657810?format=rdfxml
import json
import gzip
from io import BytesIO
from io import StringIO
from flask import Flask
from flask import jsonify
from flask import abort
from flask import Response
from flask import redirect
from flask import request
from flask_restplus import reqparse
from flask_restplus import Resource
from flask_restplus import Api
from rdflib import ConjunctiveGraph,Graph
from elasticsearch import Elasticsearch
from werkzeug.contrib.fixers import ProxyFix

host="194.95.145.44"

app=Flask(__name__)

#app.wsgi_app = ProxyFix(app.wsgi_app)

api = Api(app, title="EFRE LOD for SLUB", default='Elasticsearch Wrapper API',default_label='search and access operations',default_mediatype="application/json",contact="Bernhard Hering",contact_email="bernhard.hering@slub-dresden.de")

es=Elasticsearch([{'host':host}],port="9200",timeout=5)

indices=["persons","tags","geo","orga","works","resources","events"]

authorities={
            "gnd":"http://d-nb.info/gnd/",
            "swb":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
            "viaf":"http://viaf.org/viaf/",
            "wd":"http://www.wikidata.org/entity/"
            }

excludes=["_isil","identifier","nameSub","nameShort","url"]

# build a Response-Object to give back to the client
# first reserialize the data to other RDF implementations if needed
# gzip-compress if the client supports it via Accepted-Encoding
def gunzip(data):
    gzip_buffer = BytesIO()
    with gzip.open(gzip_buffer,mode="wb",compresslevel=6) as gzip_file:
        gzip_file.write(data.data)
    data.data=gzip_buffer.getvalue()
    data.headers['Content-Encoding'] = 'gzip'
    data.headers['Vary'] = 'Accept-Encoding'
    data.headers['Content-Length'] = data.content_length
    return data

def output(data,format,fileending,request):
    retformat=""
    encoding=request.headers.get("Accept")
    print(encoding)
    if fileending and fileending in ["nt","rdf","jsonld","json","nq","jsonl"]:
        retformat=fileending
    elif not fileending and format in ["nt","rdf","jsonld","json","nq","jsonl"]:
        retformat=format
    elif encoding in ["application/n-triples","application/rdf+xml",'text/turtle','application/n-quads','application/x-jsonlines']:
        retformat=encoding
    else:
        retformat="json"
    ret=None
    if not data:    # give back 400 if no data
        abort(404)
    # check out the format string for ?format= or Content-Type Headers
    elif retformat=="nt" or retformat=="application/n-triples":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format='nt').decode('utf-8')
        ret=Response(data,mimetype='text/plain')
        if encoding and "gzip" in encoding:
            return output_nt(gunzip(ret))
        else:
            return output_nt(ret)
    elif retformat=="rdf" or retformat=="application/rdf+xml":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format="application/rdf+xml").decode('utf-8')
        if encoding and "gzip" in encoding:
            return output_rdf(gunzip(Response(data,mimetype='application/rdf+xml')))
        else:
            return output_rdf(Response(data,mimetype='application/rdf+xml'))
    elif retformat=="ttl" or retformat=="text/turtle":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format='turtle').decode('utf-8')
        if encoding and "gzip" in encoding:
            return output_ttl(gunzip(Response(data,mimetype='text/turtle')))
        else:
            return output_ttl(Response(data,mimetype='text/turle'))
    elif retformat=="nq" or retformat=="application/n-quads":
        g=ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        data=g.serialize(format='nquads').decode('utf-8')
        if encoding and "gzip" in encoding:
            return output_nq(gunzip(Response(data,mimetype='application/n-quads')))
        else:
            return output_nq(Response(data,mimetype='application/n-quads'))
    elif retformat=="jsonl" or retformat=="application/x-jsonlines":
        ret=""
        if isinstance(data,list):
            for item in data:
              ret+=json.dumps(item,indent=None)+"\n"
        elif isinstance(data,dict):
            ret+=json.dumps(data,indent=None)+"\n"
        if encoding and "gzip" in encoding:
            return gunzip(output_jsonl(Response(ret,mimetype='application/x-jsonlines')))
        else:
            return output_jsonl(Response(ret,mimetype='application/x-jsonlines'))
    else:
        if encoding and "gzip" in encoding:
             return gunzip(jsonify(data))
        else:
            return jsonify(data)

@api.representation('application/n-triples')
def output_nt(data):
    return data

@api.representation('application/rdf+xml')
def output_rdf(data):
    return data

@api.representation('application/n-quads')
def output_nq(data):
    return data

@api.representation('text/turtle')
def output_ttl(data):
    return data

@api.representation('application/x-jsonlines')
def output_jsonl(data):
    return data
#returns an single document given by index or id. if you use /index/search, then you can execute simple searches
@api.route('/<any({}):entityindex>/<string:id>'.format(str(indices)),methods=['GET'])
@api.param('entityindex','The name of the entity-index to access. Allowed Values: {}.'.format(str(indices)))
@api.param('id','The ID-String of the record to access. Possible Values (examples):118695940, 130909696')
class RetrieveDoc(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('isAttr',type=bool,location="args",default=False,help="set to True if you want to search for this entity as an attribut in other records")
    parser.add_argument('size_arg',type=int,help="Configure the maxmimum amount of hits to be returned when searching, if isAttr is True.",location="args",default=100)
    parser.add_argument('from_arg',type=int,help="Configure the offset from the frist result you want to fetch when searching, if isAttr is True.",location="args",default=0)
    parser.add_argument('sort',type=str,help="how to sort the returned datasets when searching, if isAttr is True. like: path_to_property:[asc|desc]",location="args")
    
    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc('search for authority-id')
    def get(self,entityindex,id):
        """
        get a single record of an entity-index, or search for all records containing this record as an attribute via isAttr parameter
        """
        retarray=[]
        args=self.parser.parse_args()
        name=""
        ending=""
        if "." in id:
            dot_fields=id.split(".")
            name=dot_fields[0]
            ending=dot_fields[1]
        else:
            name=id
            ending=""
        if args.get("isAttr"):
        #if index in indices:
            #indices.remove(index)
            search={"_source":{"excludes":["_isil"]},"query":{"query_string" : {"query":"\"http://data.slub-dresden.de/"+','.join(indices)+"/"+name+"\""}}}
            if args.get("sort") and "|" in args.get("sort") and ( "asc" in args.get("sort") or "desc" in args.get("sort") ):
                sort_fields=args.get("sort").split("|")
                search["sort"]=[{sort_fields[0]+".keyword":sort_fields[1]}]
            res=es.search(index=','.join(indices),body=search,size=args.get("size_arg"), from_=args.get("from_arg"))
            if "hits" in res and "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    if hit.get("_id")!=name:
                        retarray.append(hit.get("_source"))
        else:
            try:
                res=es.get(index=entityindex,doc_type="schemaorg",id=name,_source_exclude=excludes)
                retarray.append(res.get("_source"))
            except:
                abort(404)
        return output(retarray,args.get("format"),ending,request)

@api.route('/search',methods=['GET',"PUT", "POST"])
class ESWrapper(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('q',type=str,help="Lucene Query String Search Parameter",location="args")
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('sort',type=str,help="how to sort the returned datasets. like: path_to_property:[asc|desc]",location="args")
    parser.add_argument('size_arg',type=int,help="Configure the maxmimum amount of hits to be returned",location="args")
    parser.add_argument('from_arg',type=int,help="Configure the offset from the frist result you want to fetch",location="args")
    parser.add_argument('filter',type=str,help="filter the search by a defined value in a path. e.g. path_to_property:value",location="args")
    
    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    def get(self):
        """
        search over all entity-indices
        """
        retarray=[]
        args=self.parser.parse_args()
        search={}
        search["_source"]={"excludes":excludes}
        if args["q"] and not args["filter"]:
            search["query"]={"query_string" : {"query":args["q"]}}
        elif args["filter"] and ":" in args["filter"] and not args["q"]:
            filter_fields=args["filter"].split(":")
            search["query"]={"match":{filter_fields[0]:filter_fields[1]}}
        elif args["q"] and args["filter"] and ":" in args["filter"]:
            filter_fields=args["filter"].split(":")
            search["query"]={"bool":{"must":[{"query_string":{"query":args["filter"]}},{"match":{filter_fields[0]:filter_fields[1]}}]}}
        else:
            search["query"]={"match_all":{}}
        if args["sort"] and ":" in args["sort"] and ( "asc" in args["sort"] or "desc" in args["sort"] ):
            sort_fields=args["sort"].split(":")
            search["sort"]=[{sort_fields[0]+".keyword":sort_fields[1]}]
        #    print(json.dumps(search,indent=4))
        res=es.search(index=','.join(indices),body=search,size=args["size_arg"],from_=args["from_arg"])
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return output(retarray,args.get("format"),"",request)

#search in an index.
@api.route('/<any({}):entityindex>/search'.format(str(indices)),methods=['GET'])
@api.param('entityindex','The name of the entity-index to access. Allowed Values: {}.'.format(str(indices)))
@api.param('id','The ID-String of the record to access. Possible Values (examples):161142842 (persons), 19195084X (orga)')
class searchDoc(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('q',type=str,help="Lucene Query String Search Parameter",location="args")
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('size_arg',type=int,help="Configure the maxmimum amount of hits to be returned",location="args",default=100)
    parser.add_argument('from_arg',type=int,help="Configure the offset from the frist result you want to fetch",location="args",default=0)
    parser.add_argument('sort',type=str,help="how to sort the returned datasets. like: path_to_property:[asc|desc]",location="args")
    parser.add_argument('filter',type=str,help="filter the search by a defined value in a path. e.g. path_to_property:value",location="args")
    
    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc('search in Index')
    def get(self,entityindex):
        """
        search on one given entity-index
        """
        retarray=[]
        args=self.parser.parse_args()
        if entityindex in indices:
                search={}
                search["_source"]={"excludes":excludes}
                if args.get("q") and not args.get("filter"):
                    search["query"]={"query_string" : {"query": args.get("q")}}
                elif args.get("filter") and ":" in args.get("filter") and not  args.get("q"):
                    filter_fields=args.get("filter").split(":")
                    search["query"]={"match":{filter_fields[0]:filter_fields[1]}}
                elif  args.get("q") and args.get("filter") and ":" in args.get("filter"):
                    filter_fields=args.get("filter").split(":")
                    search["query"]={"bool":{"must":[{"query_string":{"query": args.get("q")}},{"match":{filter_fields[0]:filter_fields[1]}}]}}
                else:
                    search["query"]={"match_all":{}}
                if args.get("sort") and "|" in args.get("sort") and ( "asc" in args.get("sort") or "desc" in args.get("sort") ):
                    sort_fields=args.get("sort").split("|")
                    search["sort"]=[{sort_fields[0]+".keyword":sort_fields[1]}]
                res=es.search(index=entityindex,body=search,size=args.get("size_arg"), from_=args.get("from_arg"))
                if "hits" in res and "hits" in res["hits"]:
                    for hit in res["hits"]["hits"]:
                        retarray.append(hit.get("_source"))
        return output(retarray,args.get("format"),"",request)
    
@api.route('/<any({}):authorityprovider>/<string:id>'.format(str(list(authorities.keys()))),methods=['GET'])
@api.param('authorityprovider','The name of the authority-provider to access. Allowed Values: {}.'.format(str(list(authorities.keys()))))
@api.param('id','The ID-String of the authority-identifier to access. Possible Values (examples): 208922695, 118695940, 20474817, Q1585819')
class AutSearch(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('q',type=str,help="Lucene Query String Search Parameter",location="args")
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('size_arg',type=int,help="Configure the maxmimum amount of hits to be returned",location="args",default=100)
    parser.add_argument('from_arg',type=int,help="Configure the offset from the frist result you want to fetch",location="args",default=0)
    parser.add_argument('filter',type=str,help="filter the search by a defined value in a path. e.g. path_to_property:value",location="args")
    
    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc('get record by authority-id')
    def get(self,authorityprovider,id):
        """
        search for an given ID of a given authority-provider
        """
        retarray=[]
        args=self.parser.parse_args()
        name=""
        ending=""
        if "." in id:
            dot_fields=id.split(".")
            name=dot_fields[0]
            ending=dot_fields[1]
        else:
            name=id
            ending=""
        if not authorityprovider in authorities:
            abort(404)
        search={"_source":{"excludes":excludes},"query":{"query_string" : {"query":"sameAs.keyword:\""+authorities.get(authorityprovider)+name+"\""}}}    
        res=es.search(index=','.join(indices),body=search,size=args.get("size_arg"),from_=args.get("from_arg"))
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return output(retarray,args.get("format"),ending,request)
    
@api.route('/<any({aut}):authorityprovider>/<any({ent}):entityindex>/<string:id>'.format(aut=str(list(authorities.keys())),ent=str(indices)),methods=['GET'])
@api.param('authorityprovider','The name of the authority-provider to access. Allowed Values: {}.'.format(str(list(authorities.keys()))))
@api.param('entityindex','The name of the entity-index to access. Allowed Values: {}.'.format(str(indices)))
@api.param('id','The ID-String of the authority-identifier to access. Possible Values (examples): 208922695, 118695940, 20474817, Q1585819')
class AutEntSearch(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('q',type=str,help="Lucene Query String Search Parameter",location="args")
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('size_arg',type=int,help="Configure the maxmimum amount of hits to be returned",location="args",default=100)
    parser.add_argument('from_arg',type=int,help="Configure the offset from the frist result you want to fetch",location="args",default=0)
    parser.add_argument('filter',type=str,help="filter the search by a defined value in a path. e.g. path_to_property:value",location="args")
    
    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc('get record by authority-id and entity-id')
    def get(self,authorityprovider,entityindex,id):
        """
        search for an given ID of a given authority-provider on a given entity-index
        """
        retarray=[]
        args=self.parser.parse_args()
        name=""
        ending=""
        if "." in id:
            dot_fields=id.split(".")
            name=dot_fields[0]
            ending=dot_fields[1]
        else:
            name=id
            ending=""
        if not authorityprovider in authorities or entityindex not in indices:
            abort(404)
        search={"_source":{"excludes":excludes},"query":{"query_string" : {"query":"sameAs.keyword:\""+authorities.get(authorityprovider)+name+"\""}}}    
        res=es.search(index=entityindex,body=search,size=args.get("size_arg"),from_=args.get("from_arg"))
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return output(retarray,args.get("format"),ending,request) 

if __name__ == '__main__':        
    app.run(host="localhost",port=80,debug=True)
