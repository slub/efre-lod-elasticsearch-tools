# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# This is a sample controller
# this file is released under public domain and you can use without limitations
# -------------------------------------------------------------------------

def index():
    opt=None
    if request.vars.uri:
        session.uri = request.vars.uri
        redirect(URL('data'))
    else:
        from adl import loadjson
        config=loadjson("/etc/adlookup.json")
        optgr=[]
        for k,v in config["types"].items():
            optgr.append(k)
        opt=SELECT(optgr,_name="typ",_size="1")
    return dict(options=opt)

def data():
    from adl import getDataByID, loadjson
    resp=[]
    for x in getDataByID(typ=request.vars.typ,num=request.vars.uri,feld=request.vars.feld):
        resp.append(x)
    if len(resp)==1:
        if isinstance(resp[0],str):
            if resp[0].startswith("No config defined in"):
                raise HTTP(503)
        else:
            return response.json(resp[0])
    elif len(resp)==0:
        raise HTTP(404)
    else:
        return response.json(resp)

