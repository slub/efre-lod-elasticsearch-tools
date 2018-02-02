#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pyodbc
import json
import sys
import chardet
import validators
import codecs
import string

class Isql: #howto: http://docs.openlinksw.com/virtuoso/virtmanconfodbcdsnunix/
    def __init__(self, connection_string='DSN=Local Virtuoso;UID=dba;PWD=dba',detecturis=False):
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()
        self.detecturis = detecturis
        

    def filter_nonprintable(self,text):
        import string
        # Get the difference of all ASCII characters from the set of printable characters
        nonprintable = set([chr(i) for i in range(128)]).difference(string.printable)
        # Use translate to remove all non-printable characters
        return text.translate({ord(character):None for character in nonprintable})
    
    def uri_or_literal(self,string):
        if self.detecturis:
            string=self.filter_nonprintable(string)
            if validators.url(string):
                ret=str("<"+str(string)+str(">"))
                return ret
            else:
                ret= str("\'"+str(string)+str("\'"))
                return ret
        else:
            return string
        
    def create_triple_string(self,uri,sentence):
        cmd="SPARQL INSERT IN GRAPH " + self.uri_or_literal(uri) + " { " + sentence + " }"
        return self.cursor.execute(cmd)
    
    def create_triple(self,uri,s,p,o):
        sentence = self.uri_or_literal(str(s))+" "+self.uri_or_literal(p)+" "+self.uri_or_literal(o)+" ."
        return self.create_triple_string(uri,sentence)


    def read_triples(self,uri,subject):
        cmd="SPARQL SELECT "+self.uri_or_literal(uri)+" ?p ?o FROM <http://data.slub-dresden.de> WHERE { "+self.uri_or_literal(subject)+" ?p ?o . }"
        triples=[]
        try:
            for row in self.cursor.execute(cmd).fetchall():
                rowAsList = [ x for x in row]
                triple=[]
                for x in rowAsList:
                    triple.append(self.filter_nonprintable(x))
                triples.append(triple)
            return triples
        except pyodbc.ProgrammingError as e:
            print(e,cmd)
            
            
    def update_triples_by_spo(self,uri,triples):
        for s,p,o in self.read_triples(uri):
            self.delete_triple(uri,s,p,o)
        for s,p,o in triples:
            self.create_triple(uri,s,p,o)

    def update_triples_by_full_sentence(self,uri,subject,sentences):
        for s,p,o in self.read_triples(uri,subject):
            self.delete_triple(uri,s,p,o)
        for sentence in sentences:
            self.create_triple_string(self,uri,sentence)


    def delete_triple(self,uri,s,p,o):
        cmd="SPARQL DELETE DATA FROM "+str(self.uri_or_literal(uri))+" { "+str(self.uri_or_literal(str(s))) + " " +str(self.uri_or_literal(p))+" "+str(self.uri_or_literal(o))+" . }"
        return self.cursor.execute(cmd)

    def count_triples(self,uri):
        for row in self.cursor.execute("select count(*) from db.dba.rdf_quad").fetchone():
            return int(row)
    
#example
if __name__ == "__main__":
    virtuoso  = Isql(detecturis=True)
    uri="http://data.slub-dresden.de"
    print(virtuoso.count_triples(uri))
    triples=[]
    for s,p,o in virtuoso.read_triples("http://data.slub-dresden.de","http://data.slub-dresden.de"):
        print(s,p,o)
