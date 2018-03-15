#!/usr/bin/python3
import json
from es2json import esgenerator

indices={
	"geo":"schemaorg",
	"persons":"schemaorg",
	"orga":"schemaorg",
	"resources":"schemaorg"
}



if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Backup your ES-Index')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-help',action="store_true",help="print this help")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    for ind,typ in indices.items():
        with open(str(ind)+"."+str(typ)+".ldj","w") as fileout:
            for record in esgenerator(host=args.host,type=args.type,index=ind,type=typ,headless=true):
                fileout.write(json.dumps(record)+"\n")
