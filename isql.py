#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pyodbc
import json
import sys
import chardet
import validators
import codecs
import string
from rdflib import URIRef
from pprint import pprint
connection=None
cursor=None

class Isql:
    def __init__(self, connection_string):
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()
        

    def filter_nonprintable(self,text):
        import string
        # Get the difference of all ASCII characters from the set of printable characters
        nonprintable = set([chr(i) for i in range(128)]).difference(string.printable)
        # Use translate to remove all non-printable characters
        return text.translate({ord(character):None for character in nonprintable})
    
    def uri_or_literal(self,string):
        string=self.filter_nonprintable(string)
        if validators.url(string):
            ret=str("<"+str(string)+str(">"))
            return ret
        else:
            ret= str("\'"+str(string)+str("\'"))
            return ret
    def insert_triple(self,uri,s,p,o):
        cmd="SPARQL INSERT IN GRAPH " + self.uri_or_literal(uri) + " { "+self.uri_or_literal(str(s))+" "+self.uri_or_literal(p)+" "+self.uri_or_literal(o)+" . }"
        return self.cursor.execute(cmd)
    
    def delete_triple(self,uri,s,p,o):
        cmd="SPARQL DELETE DATA FROM "+str(self.uri_or_literal(uri))+" { "+str(self.uri_or_literal(str(s))) + " " +str(self.uri_or_literal(p))+" "+str(self.uri_or_literal(o))+" . }"
        return self.cursor.execute(cmd)
    
    def get_triples_by_subject(self,uri):
        cmd="SPARQL SELECT "+self.uri_or_literal(uri)+" ?p ?o FROM <http://data.slub-dresden.de> WHERE { "+self.uri_or_literal(uri)+" ?p ?o . }"
        triples=[]
        for row in self.cursor.execute(cmd).fetchall():
            rowAsList = [ x for x in row]
            triple=[]
            for x in rowAsList:
                triple.append(self.filter_nonprintable(x))
            triples.append(triple)
        return triples
    
    def count_triples(self,uri):
        for row in self.cursor.execute("select count(*) from db.dba.rdf_quad").fetchone():
            return int(row)
    
#example
if __name__ == "__main__":
    virtuoso  = Isql('DSN=Local Virtuoso;UID=dba;PWD=dba')
    uri="http://data.slub-dresden.de"
    print(virtuoso.count_triples(uri))
    triples=[]
    for s,p,o in virtuoso.get_triples_by_subject("http://data.slub-dresden.de/resources/405206240"):
        triples.append([s,p,o])
        virtuoso.delete_triple(uri,s,p,o)
        print(s,p,o)
    print(virtuoso.count_triples(uri))
    for s,p,o in triples:
        print(s,p,o)
    for s,p,o in triples:
        virtuoso.insert_triple(uri,s,p,o)
    for s,p,o in virtuoso.get_triples_by_subject("http://data.slub-dresden.de/resources/405206240"):
        print(s,p,o)
    print(virtuoso.count_triples(uri))
    #for row in cursor.execute("SPARQL SELECT * WHERE {?s ?p ?o } LIMIT 500000").fetchall():
    #    rowAsList = [x for x in row]
    #    for x in rowAsList:
    #        sys.stdout.write(str(x)+"\t")
    #    print("")
