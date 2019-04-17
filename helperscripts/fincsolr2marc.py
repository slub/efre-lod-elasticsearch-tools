#!/usr/bin/python3

import sys
import json
import pymarc
import argparse
from es2json import eprint
from pymarc.exceptions import *
from pymarc import marcxml

replaceMethods = {
        'decimal':(( '#29;', '#30;', '#31;' ), ( "\x1D", "\x1E", "\x1F" )),
        'unicode':(( '\u001d', '\u001e', '\u001f' ), ( "\x1D", "\x1E", "\x1F" )),
        'hex':(( '\x1D', '\x1E', '\x1F' ), ( "\x1D", "\x1E", "\x1F" ))
       }
parser=argparse.ArgumentParser(description='FincSolr 2 Marc binary Records')
parser.add_argument('-format',type=str,default="marc",help='Format of the fullrecord. supported: marc (includes fincmarc), marcxml')
parser.add_argument('-frfield',type=str,default="fullrecord",help='field name of the fullrecord. default is \'fullrecord\'')
parser.add_argument('-help',action="store_true",help="print this help")
parser.add_argument('-replaceMethod',type=str,default="decimal",help="Which kind of replace method to use. available: ")
parser.add_argument('-valid',action="store_true",help="validate MARC Records")
args=parser.parse_args()
if args.help:
    parser.print_help(sys.stderr)
    exit()        

for line in sys.stdin:
    record=json.loads(line)
    if args.format=="marc" and "marc" in record.get("recordtype") and not "xml" in record.get("recordtype"):
        recordFixed=record.get("recordtype")
        marcFullRecordFixed=record.get(args.frfield)
        for i in range(0,3):
            marcFullRecordFixed=marcFullRecordFixed.replace(replaceMethods.get(args.replaceMethod)[0][i],replaceMethods.get(args.replaceMethod)[1][i])
        if args.valid:
            try:
                reader=pymarc.MARCReader(marcFullRecordFixed.encode('utf8'),utf8_handling='replace')
                marcrecord=next(reader)
            except (RecordLengthInvalid, RecordLeaderInvalid, BaseAddressNotFound, BaseAddressInvalid, RecordDirectoryInvalid, NoFieldsFound, UnicodeDecodeError) as e:
                eprint("record id {0}:".format(record.get("id")+str(e)))
                with open('invalid_records.txt','a') as error:
                    #file_out.pluserror()
                    print(marcFullRecordFixed,file=error)
                    continue
        sys.stdout.write(marcFullRecordFixed)
    elif "marcxml" in record.get("recordtype") and args.format=="marcxml":
            pymarc.marcxml.parse_xml_to_array(StringIO(record.get(args.frfield))) #need wrapper in StringIO for read()-need in marcxml lib
                
            
            
        
    
