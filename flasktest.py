from flask import Flask
from flask import redirect
from flask import request
import time
import json
import redis_part as r
import genres_parser as gp
import influ as infx
from multiprocessing import Process,Pool
import os
import subprocess
import random
import GraphQuery as n4j

app = Flask(__name__)



@app.route('/moviesearch')
def moviesearch():
    print("request received")
    starttime = request.args.get("starttime")
    if(starttime!=""):
        startyear = starttime.split(" ")[3]
    else:
        startyear=""
    endtime = request.args.get("endtime")
    if(endtime!=""):
        endyear = endtime.split(" ")[3]
    else:
        endyear=""
    partmoviename = request.args.get("title")
    director = request.args.get("director")
    actor = request.args.get("actor")
    genre = request.args.get("genre")
    movie = {}

    sfp = Pool(4)#开启一个容量为4的进程池


    begint = time.time()
    redisproc = sfp.apply_async(r.msearch, args=(genre, startyear,endyear,partmoviename,director,actor))
    #movie = r.msearch(genre=genre,begintime=startyear,endtime=endyear,moviename=partmoviename,director=director,actor=actor)
    endtime1 = time.time()

    infxproc = sfp.apply_async(infx.msearch, args=(starttime, endtime,actor,director,genre,partmoviename))
    #infx.msearch(starttime,endtime=endtime,actor=actor,director=director,genre=genre,title=partmoviename,review=None)
    endtime2 = time.time()

    # n4j.msearch()
    n4jproc = sfp.apply_async(n4j.msearch, args=(genre, startyear,endyear,partmoviename,director,actor))
    endtime3 = time.time()

    # all.msearch()
    endtime4 = time.time()

    sfp.close()
    sfp.join()#阻断主进程直到子进程执行完毕
    [redistime,movie] = redisproc.get()#返回子进程的结果
    [influxtime,none] = infxproc.get()
    [n4jtime,none] = n4jproc.get()

    limit = 0
    if len(movie.keys()) > 20000:
        limit = 500
    newmovie={}
    for key in movie.keys():
        if(key!="length"):
            if(int(movie[key]["reviewnum"])>=limit):
                newmovie[key]=movie[key]
    newmovie["length"] = len(movie.keys())
    newmovie["redis"] = round(redistime,2)
    newmovie["influxdb"] = round(influxtime,2)
    newmovie["neo4j"] = round(n4jtime,2)
    newmovie["zonghedb"] = endtime4 - endtime4
    moviejson = json.dumps(newmovie)

    return moviejson

@app.route('/collaboration')
def collaboration():
    actor = request.args.get("actor")
    director = request.args.get("director")
    #mostnumber = request.args.get("mostnumber")
    print(actor,director)
    relationships1 = {}
    relationships = {}

    clp=Pool(4)

    begint = time.time()
    redisproc = clp.apply_async(r.bsearch, args=(actor,director))
    #relationships1 = r.bsearch(actor,director)
    endtime1 = time.time()


    infxproc = clp.apply_async(infx.bsearch, args=(director,actor))
    #infx.bsearch(director,actor)
    endtime2 = time.time()

    n4jproc = clp.apply_async(n4j.bsearch, args=(actor,director))
    #n4j.bsearch(director,actor)
    endtime3 = time.time()

    #all.bsearch(director,actor)
    endtime4 = time.time()

    clp.close()
    clp.join()  # 阻断主进程直到子进程执行完毕

    [redistime,relationships1] = redisproc.get()#返回子进程的结果
    [influxtime,none] = infxproc.get()
    [n4jtime, none] = n4jproc.get()

    limit = 0
    if len(relationships1.keys())>20000:
        limit = 3


    num = 0
    for i in relationships1.keys():
        for j in relationships1[i].keys():
            n = relationships1[i][j]
            if(n>=limit):
                relationships[num] = json.dumps({"director": j, "actor": i, "times": n})
                num+=1

    relationships["len"] = num
    relationships["redis"] = round(redistime,2)
    relationships["influxdb"] = round(influxtime,2)
    relationships["neo4j"] = round(endtime3 - endtime2,2)
    relationships["zonghedb"] = round(endtime4 - endtime3,2)
    print(num)
    relationshipsjson = json.dumps(relationships)

    return relationshipsjson

@app.route('/diagram')
def diagram():
    finaljson = {}
    diagramdata = []
    diagramlabels = []
    diagramgenredic = gp.genres_parser()
    for genre in diagramgenredic.keys():
        diagramdata.append(diagramgenredic[genre])
        diagramlabels.append(genre)

    finaljson["diagramlabels"] = diagramlabels
    finaljson["diagramdata"] = diagramdata

    chart2=infx.dividebyyear()
    chart2data=[]
    chart2labels=[]

    for year in chart2.keys():
        chart2data.append(chart2[year])
        chart2labels.append(year)

    finaljson["chart2data"] = chart2data
    finaljson["chart2labels"] = chart2labels
    finaljson = json.dumps(finaljson)
    return finaljson

if __name__ == '__main__':
    app.run(host='0.0.0.0')
