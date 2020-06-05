#!/usr/bin/env python3.6

import sys
import json
import pymarc
import argparse
from io import BytesIO
from es2json import eprint
from pymarc.exceptions import *
from pymarc import marcxml,MARCReader
from marc2jsonl import transpose_to_ldj

def fixRecord(record="",record_id=0,validation=False,replaceMethod='decimal'):
        replaceMethods = {
            'decimal':(( '#29;', '#30;', '#31;' ), ( "\x1D", "\x1E", "\x1F" )),
            'unicode':(( '\u001d', '\u001e', '\u001f' ), ( "\x1D", "\x1E", "\x1F" )),
            'hex':(( '\x1D', '\x1E', '\x1F' ), ( "\x1D", "\x1E", "\x1F" ))
        }
        marcFullRecordFixed=record
        for i in range(0,3):
            marcFullRecordFixed=marcFullRecordFixed.replace(replaceMethods.get(replaceMethod)[0][i],replaceMethods.get(replaceMethod)[1][i])
        if validation:
            try:
                reader=pymarc.MARCReader(marcFullRecordFixed.encode('utf8'),utf8_handling='replace')
                marcrecord=next(reader)
            except (RecordLengthInvalid, RecordLeaderInvalid, BaseAddressNotFound, BaseAddressInvalid, RecordDirectoryInvalid, NoFieldsFound, UnicodeDecodeError) as e:
                eprint("record id {0}:".format(record_id)+str(e))
                with open('invalid_records.txt','a') as error:
                    #file_out.pluserror()
                    eprint(marcFullRecordFixed,file=error)
                    return None
        return marcFullRecordFixed

def main():
    parser=argparse.ArgumentParser(description='FincSolr 2 Marc binary Records')
    parser.add_argument('-format',type=str,default="marc",help='Format of the fullrecord. supported: marc (includes fincmarc), marcxml')
    parser.add_argument('-frfield',type=str,default="fullrecord",help='field name of the fullrecord. default is \'fullrecord\'')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-replaceMethod',type=str,default="decimal",help="Which kind of replace method to use. available: ")
    parser.add_argument('-valid',action="store_true",help="validate MARC Records")
    parser.add_argument('-toJson',action="store_true",default=False,help="Transpose to MarcXchange JSON on the fly")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()        

    for line in sys.stdin:
        record=json.loads(line)
        if record and record.get("recordtype") and args.format=="marc" and "marc" in record.get("recordtype") and not "xml" in record.get("recordtype"):
            marcFullRecordFixed=fixRecord(record=record.get(args.frfield),record_id=record.get("record_id"),validation=args.valid,replaceMethod=args.replaceMethod)
            if not args.toJson:
                sys.stdout.write(marcFullRecordFixed)
            else:
                for record in MARCReader(marcFullRecordFixed.encode('utf-8'), to_unicode=True):
                    sys.stdout.write(json.dumps(transpose_to_ldj(record)))
        elif record and record.get("recordtype") and "marcxml" in record.get("recordtype") and args.format=="marcxml":
                pymarc.marcxml.parse_xml_to_array(StringIO(record.get(args.frfield))) #need wrapper in StringIO for read()-need in marcxml lib
                

if __name__ == "__main__":
    main()
