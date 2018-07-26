#!/usr/bin/python3

import sys
import json


def isint(num):
    try: 
        int(num)
        return True
    except:
        return False
    
def traverse(dict_or_list, path):
    iterator=None
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list, list):
        iterator = enumerate(dict_or_list)
    elif isinstance(dict_or_list,str):
        strarr=[]
        strarr.append(dict_or_list)
        iterator=enumerate(strarr)
    else:
        return
    if iterator and isinstance(dict_or_list,dict):
        for k, v in iterator:
            if not isinstance(v,dict):
                if isinstance(v,list) and isinstance(v[0],dict):
                    for elem in v:
                        for c,w in traverse(elem,k+"."):
                            yield c ,w
                else:
                    yield path + str(k), v
            else:
                for k,v in traverse(v,k+"."):
                    #print(k,v)
                    yield k ,v
    elif iterator and isinstance(dict_or_list,list):
        for k,v in iterator:
            ##print(path,v)
            yield path, v

    



if __name__ == "__main__":
    for line in sys.stdin:
        try:
            jline=json.loads(line)
        except:
            eprint("corrupt json: "+str(line))
            continue
        newrec={}
        for k,v in traverse(jline,""):
            #print(k,v)
            if k not in newrec:
                newrec[k]=v
            elif k in newrec and not isinstance(newrec.get(k),list):
                newrec[k]=[newrec.pop(k)]
                newrec[k].append(v)
        if newrec:
            print(json.dumps(newrec,indent=None))
        #sys.stdout.write(json.dumps(jline)+"\n")
        #sys.stdout.flush()

