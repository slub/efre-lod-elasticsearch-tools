#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
import json
from pprint import pprint
from elasticsearch import Elasticsearch
import argparse
import sys, os, time, atexit
from signal import SIGTERM 

class Daemon:
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError as e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 

        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError as e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, 'r')
        so = open(self.stdout, 'a+')
        se = open(self.stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        with open(self.pidfile,'w+') as pf:
            pf.write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            with open(self.pidfile,'r') as pf:
                pid = int(pf.read().strip())
                if pid:
                    message = "pidfile %s already exist. Daemon already running?\n"
                    sys.stderr.write(message % self.pidfile)
                    sys.exit(1)
        except IOError:
            pid = None
        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            with open(self.pidfile,'r') as pf:
                pid = int(pf.read().strip())
        except IOError:
            pid = None
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process	
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err))
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """


class simplebar():
    count=0
    def __init__(self):
        self.count=0
        
    def reset(self):
        self.count=0
        
    def update(self,num=None):
        if num:
            self.count+=num
        else:
            self.count+=1
        sys.stderr.write(str(self.count)+"\n"+"\033[F")
        sys.stderr.flush()
        
def ArrayOrSingleValue(array):
    if array:
        length=len(array)
        if length>1 or isinstance(array,dict):
            return array
        elif length==1:
            for elem in array:
                 return elem
        elif length==0:
            return None
        
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)   
    
    
def esfatgenerator(host=None,port=9200,index=None,type=None,body=None,source=True,source_exclude=None):
    if not source:
        source=True
    es=Elasticsearch([{'host':host}],port=port)
    try:
        page = es.search(
            index = index,
            doc_type = type,
            scroll = '2m',
            size = 1000,
            body = body,
            _source=source,
            _source_exclude=source_exclude)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("not found: "+host+":"+port+"/"+index+"/"+type+"/_search\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    yield page.get('hits').get('hits')
    while (scroll_size > 0):
        pages = es.scroll(scroll_id = sid, scroll='2d')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        yield pages.get('hits').get('hits')
    ### avoid dublettes and nested lists when adding elements into lists
def litter(lst, elm):
    if not lst:
        return elm
    else:
        if isinstance(elm,str):
            if elm not in lst:
                if isinstance(lst,str):
                    return [lst,elm]
                elif isinstance(lst,list):
                    lst.append(elm)
                    return lst
        elif isinstance(elm,list):
            if isinstance(lst,str):
                lst=[lst]
            if isinstance(lst,list):
                for element in elm:
                    if element not in lst:
                        lst.append(element)
            return lst

def esgenerator(host=None,port=9200,index=None,type=None,body=None,source=True,headless=False):
    if not source:
        source=True
    es=Elasticsearch([{'host':host}],port=port)
    try:
        page = es.search(
            index = index,
            doc_type = type,
            scroll = '2m',
            size = 1000,
            body = body,
            _source=source)
    except elasticsearch.exceptions.NotFoundError:
        sys.stderr.write("not found: "+host+":"+port+"/"+index+"/"+type+"/_search\n")
        exit(-1)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    for hits in page['hits']['hits']:
        if headless:
            yield hits['_source']
        else:
            yield hits
    while (scroll_size > 0):
        pages = es.scroll(scroll_id = sid, scroll='2d')
        sid = pages['_scroll_id']
        scroll_size = len(pages['hits']['hits'])
        for hits in pages['hits']['hits']:
            if headless:
                yield hits['_source']
            else:
                yield hits

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='simple ES.Getter!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument('-source',type=str,help='just return this field(s)')
    parser.add_argument('-body',type=str,help='Searchbody')
    args=parser.parse_args()
    
    for json_record in esgenerator(args.host,args.port,args.index,args.type,args.body,args.source,headless=True):
        sys.stdout.write(json.dumps(json_record)+"\n")
