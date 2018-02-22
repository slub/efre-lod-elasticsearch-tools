# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------
# This is a sample controller
# this file is released under public domain and you can use without limitations
# -------------------------------------------------------------------------

def index():
    if request.vars.uri:
        session.uri = request.vars.uri
        redirect(URL('data'))
    return dict()

def data():
    from adl import getDataByID
    resp=[]
    for x in getDataByID(typ=request.vars.typ,num=request.vars.uri):
        resp.append(x)
    if len(resp)==1:
        return response.json(resp[0])
    elif len(resp)==0:
        raise HTTP(404)
    else:
        return response.json(resp)

