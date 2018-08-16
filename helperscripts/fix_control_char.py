#!/usr/bin/python3
import codecs
import sys
sub={}
for n in range(27):
    sub[chr(n)]=""
contents=codecs.open(sys.argv[1],encoding='utf-8').read()
for c in sub:
    if c in contents:
        contents=contents.replace(c,c.encode("unicode_escape").decode("utf-8"))
x=open(sys.argv[2],"w")
x.write(contents)
x.close()
        
