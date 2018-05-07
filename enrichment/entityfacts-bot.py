#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
from elasticsearch import Elasticsearch
import json
import argparse
import sys, time
import os.path
import signal
import codecs
import syslog
import socket
import threading
import urllib3.request
from multiprocessing import Pool
from multiprocessing import Lock
from es2json import esgenerator
from es2json import eprint
from daemon import Daemon

es, args, ppn = None, None, None
gnd_field = 'id'


schema2entity = {
    "forename": "givenName",
    "surname": "familyName",
    "professionorOccupation": "hasOccupation",
    "dateOfDeath": "deathDate",
    "dateOfBirth": "birthDate",
    "placeOfBirth": "birthPlace",
    "sameAs": "sameAs",
}

def printout(string):
    if args.debug:
        eprint(string)
    else:
        syslog.syslog(string)

def push_ppns(host, port, ppns):
    if not isinstance(ppns, list):
        raise ValueError("ppns must be a list")
    message = json.dumps(ppn)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host,port))
    sock.send(message.encode('utf-8'))
    sock.close()



def entityfacts(record):
    changed = False
    if gnd_field in record:
        eprint(record)
        if "http://d-nb.info/gnd/" in record[gnd_field]:
            http = urllib3.PoolManager()
            url = "http://hub.culturegraph.org/entityfacts/%s" % (record[gnd_field].split('/')[-1])
            r = http.request('GET', url)
            try:
                data = json.loads(r.data.decode('utf-8'))
                if 'Error' in data:
                    return [0,0]
                eprint(data)
            except:
                return [0,0]
            for k, v in schema2entity.items():
                    if k == "sameAs":
                        if isinstance(record[gnd_field], str):
                            dnb = record[gnd_field]
                            record[gnd_field] = [dnb]
                        if k in data:
                            if v not in record:
                                if isinstance(data[k], list):
                                    for elem in data[k]:
                                        record[v] = elem["@id"]
                                        changed = True
                                else:
                                    record[v] = data[k]["@id"]
                                    changed = True
                            else:
                                for sameAs in data[k]:
                                    record[v].append(sameAs["@id"])
                                    changed = True
                    if k in data and v not in record:
                        record[v] = data[k]
                        changed = True
                    elif k in data and v in record and k != "gnd_field":
                        if k == "dateOfDeath" or k == "dateOfBirth":
                            if data[k] != record[v] and record[v] and data[k]:
                                if record[v] in data[k]:
                                    record[v] = data[k]
                                elif k in record:
                                    printout("Error! %s birthDate in SWB differs from entityFacts! SWB: %s EF: %s" % (
                                        record[gnd_field],
                                        record["birthDate"],
                                        data["dateOfBirth"],
                                    )
    return record if changed else [0, 0]

def process_stuff(jline):
    length = len(jline.items())
    body = entityfacts(jline)
    if (len(body) > length):
        eprint(body)
        es.index(index=args.index, doc_type=args.type, body=body, id=body["id"])
        return

def update_by_ppn(ppn):
    process_stuff(es.get(index=args.index, doc_type=args.type, id=ppn)["_source"])

def update_ppns(ppns):
    if not args.debug:
        pool = Pool(12)
        pool.map(update_by_ppn, ppns)
    else:
        for ppn in ppns:
            update_by_ppn(ppn)

class ThreadedServer(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            try:
                client, address = self.sock.accept()
                printout("connection from %s", address)
                client.settimeout(60)
                t = threading.Thread(target=self.listenToClient, args=(client, address))
                t.start()
                t.join()
            finally:
                client.close()

    def listenToClient(self, client, address):
        size, message, ppn = 1024, "", None
        while True:
            try:
                data = client.recv(size)
                if data:
                    message += data.decode('utf-8')  # we expect a json-array
                if not data:
                    try:
                        ppn = json.loads(message)
                    except Exception as exc:
                        printout("json error: %s" % exc)
                    data = None
                    printout("going to update %s PPNs got from %s" % (len(ppn), address))
                    s = threading.Thread(target=self.update, args=([ppn]))
                    s.start()
                    client.close()
                    break
            except:
                client.close()
        client.close()

    def update(self, ppns):
        update_ppns(ppns)

class entityfactsd(Daemon):
    def run(self):
        if args.full_index:
            printout("going to update the full index: %s:%s/%s/%s" % (args.host, args.port, args.index, args.type))
            if not args.debug:
                pool = Pool(16)
                pool.map(process_stuff, esgenerator(host=args.host, port=args.port, type=args.type, index=args.index, headless=True))
            else:
                for record in esgenerator(host=args.host, port=args.port, type=args.type, index=args.index, headless=True):
                    process_stuff(record)
            printout("finished updating the full index! %s" % args.index)
        else:
            printout("started. waiting for connections on %s:%s" % (args.address, args.socketport))
            ThreadedServer(args.address,args.socketport).listen()


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich your ElasticSearch Search Index with data from entityfacts!')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-file',type=str,help='File with line-delimited IDs to update')
    parser.add_argument('-start',action='store_true',help='start a deaemon to listen on a socket to receive SWB-PPN to update')
    parser.add_argument('-stop',action='store_true',help='stop to listen on a socket to receive SWB-PPN to update')
    parser.add_argument('-restart',action='store_true',help='start a deaemon to listen on a socket to receive SWB-PPN to update')
    parser.add_argument('-full_index',action="store_true",help='update full index')
    parser.add_argument('-listen',action="store_true",help='listen for PPNs')
    parser.add_argument('-debug',action="store_true",help='no deaemon')
    parser.add_argument('-pid_file',type=str,help="Path to store the pid_file of the daemon")
    parser.add_argument('-conf',type=str,help='Daemon config file')
    parser.add_argument('-address',type=str,default="127.0.0.1",help='address of the interface to listen to')
    parser.add_argument('-socketport',type=int,help='port to listen for')

    args = parser.parse_args()
    if args.conf:
        with open(args.conf, 'r') as cfg:
            config = json.loads(cfg.read().replace('\n',''))
            if not args.index and not 'index' in config:
                sys.stderr.write("no ElasticSearch index defined in config or -index parameter. exiting.\n")
                exit(-1)
            elif not args.index and 'index' in config:
                args.index = config['index']

            if not args.type and not 'type' in config:
                sys.stderr.write("no ElasticSearch doc type defined in config -type parameter. exiting\n")
                exit(-1)
            elif not args.type and 'type' in config:
                args.type = config['type']

            if not args.host and not 'host' in config:
                sys.stderr.write("no elasticsearch host defined in config or -host parameter. exiting\n")
                exit(-1)
            elif not args.host and 'host' in config:
                args.host = config['host']

            if not args.port and not 'port' in config:
                sys.stderr.write("no elasticsearch port defined in config or -host parameter. exiting\n")
                exit(-1)
            elif not args.port and 'port' in config:
                args.port = config['port']

            if not args.address and not 'ef_host' in config:
                sys.stderr.write("no ef_host defined in config or -address parameter. exiting\n")
                exit(-1)
            elif not args.address and 'ef_host' in config:
                args.address = config['ef_host']

            if not args.socketport and not 'ef_port' in config:
                sys.stderr.write("no ef_port defined in config or -socketport parameter. exiting\n")
                exit(-1)
            elif not args.socketport and 'ef_port' in config:
                args.socketport = int(config['ef_port'])

            if args.start or args.stop or args.restart:
                if not args.pid_file and not 'pid_file' in config:
                    sys.stderr.write("no pid_file defined in config or paramter. exiting\n")
                    exit(-1)
                if not args.pid_file and 'pid_file' in config:
                    args.pid_file = config['pid_file']

    es = Elasticsearch([{'host': args.host}], port=args.port)
    if args.start:
        daemon = entityfactsd(args.pid_file)
        daemon.start()
    elif args.stop:
        daemon = entityfactsd(args.pid_file)
        daemon.stop()
    elif args.restart:
        daemon = entityfactsd(args.pid_file)
        daemon.restart()
    elif args.debug:
        entityfactsd(args.pid_file).run()
    elif args.file:
        with codecs.open(args.file, 'r', encoding='utf-8') as f: # use with parameter to close stream after context
            ppns = []
            for line in f:
                try:
                    ppns.append(str(line).strip())
                except:
                    pass
            update_ppns(ppns)
    else:
        pass

