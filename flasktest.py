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
import threading
import all

class MyThread(threading.Thread):

    def __init__(self,func,args=()):
        super(MyThread,self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None

app = Flask(__name__)



@app.route('/moviesearch')
def moviesearch():
    print("request received")
    starttime = request.args.get("starttime")
    print(starttime)
    if(starttime!=""):
        startyear = starttime.split("-")[0]
    else:
        startyear=""
    endtime = request.args.get("endtime")
    if(endtime!=""):
        endyear = endtime.split("-")[0]
    else:
        endyear=""
    partmoviename = request.args.get("title")
    director = request.args.get("director")
    actor = request.args.get("actor")
    genre = request.args.get("genre")
    movie = {}


    #逻辑：循环等待10s，对于每个线程，判断其是否执行完毕，完毕的话，正常返回，否则返回10.0s

    begint = time.time()
    threadlist = []
    redisproc = MyThread(r.msearch, args=(genre, startyear,endyear,partmoviename,director,actor))
    #movie = r.msearch(genre=genre,begintime=startyear,endtime=endyear,moviename=partmoviename,director=director,actor=actor)
    redisproc.start()

    infxproc = MyThread(infx.msearch, args=(starttime, endtime,actor,director,genre,partmoviename))
    #infx.msearch(starttime,endtime=endtime,actor=actor,director=director,genre=genre,title=partmoviename,review=None)
    infxproc.start()

    # n4j.msearch()
    n4jproc = MyThread(n4j.msearch, args=(genre, startyear,endyear,partmoviename,director,actor))
    n4jproc.start()

    # # all.msearch()
    # allproc = MyThread(all.msearch, args=(genre, startyear, endyear, partmoviename, director, actor))
    # allproc.start()

    threadlist.append(redisproc)
    threadlist.append(infxproc)
    threadlist.append(n4jproc)
    # threadlist.append(allproc)

    result=[[5.0,{}],[5.0,{}],[5.0,{}],[5.0,{}]]

    threadnum = len(threadlist)
    for i in range(5):
        flag = 0
        for j in range(threadnum):#函数数量
            if(threadlist[j].is_alive()==False):#如果某一进程结束
                result[j]=threadlist[j].get_result()
                flag += 1
        if flag == threadnum:#全部结束
            break
        time.sleep(1)

    if result[0][1]!={}:
        [redistime,movie] = result[0]#返回子进程的结果
    else:
        redistime = result[0][0]
    if result[1][1]!={}:
        [influxtime,movie] = result[1]#返回子进程的结果
    else:
        influxtime = result[1][0]
 
    if result[2][1]!={}:
        [n4jtime,movie] = result[2]#返回子进程的结果
    else:
        n4jtime = result[2][0]
    # if result[3][1]!={}:
    #     [alltime,movie] = result[3]#返回子进程的结果
    # else:
    #     alltime = result[3][0]

    #[influxtime,none] = result[1]
    #[n4jtime,none] = result[2]
    #[alltime,movie] = result[3]

    limit = 0
    if len(movie.keys()) > 2000:
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
    newmovie["zonghedb"] = round(0.0,2)
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
    threadlist = []


    begint = time.time()
    redisproc =MyThread(r.bsearch, args=(actor,director))
    #relationships1 = r.bsearch(actor,director)
    redisproc.start()


    infxproc = MyThread(infx.bsearch, args=(director,actor))
    #infx.bsearch(director,actor)
    infxproc.start()

    n4jproc = MyThread(n4j.bsearch, args=(actor,director))
    #n4j.bsearch(director,actor)
    n4jproc.start()

    # #all.bsearch(director,actor)
    # allproc = MyThread(all.bsearch, args=(actor,director))
    # allproc.start()


    threadlist.append(redisproc)
    threadlist.append(infxproc)
    threadlist.append(n4jproc)
    # threadlist.append(allproc)

    result = [[10.0, {}], [10.0, {}], [10.0, {}], [10.0, {}]]

    threadnum = len(threadlist)
    for i in range(10):
        flag = 0
        for j in range(threadnum):  # 函数数量
            if (threadlist[j].is_alive() == False):  # 如果某一进程结束
                result[j] = threadlist[j].get_result()
                flag += 1
        if flag == threadnum:  # 全部结束
            break
        time.sleep(1)


    if result[0][1]!={}:
        [redistime,relationships1] = result[0]#返回子进程的结果
    else:
        redistime = result[0][0]
    if result[1][1]!={}:
        [influxtime,relationships1] = result[1]#返回子进程的结果
    else:
        influxtime = result[1][0]
 
    if result[2][1]!={}:
        [n4jtime,movie] = result[2]#返回子进程的结果
    else:
        n4jtime = result[2][0]
    # if result[3][1]!={}:
    #     [alltime,movie] = result[3]#返回子进程的结果
    # else:
    #     alltime = result[3][0]

    # [redistime,none] =result[0]#返回子进程的结果
    # [influxtime,none] = result[1]
    # [n4jtime, none] = result[2]
    # [alltime, relationships1] = result[3]

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
    # relationships["neo4j"] = round(n4jtime,2)
    # relationships["zonghedb"] = round(alltime,2)
    print(num)
    relationshipsjson = json.dumps(relationships)

    return relationshipsjson

@app.route('/diagram')
def diagram():
    finaljson = {}
    diagramdata = []
    diagramlabels = []
    monthlabels = []
    monthdata = []
    (diagramgenredic, month_result) = gp.genres_parser()
    for genre in diagramgenredic.keys():
        diagramdata.append(diagramgenredic[genre])
        diagramlabels.append(genre)

    for month in month_result.keys():
        monthlabels.append(month)
        monthdata.append(month_result[month])

    finaljson["diagramlabels"] = diagramlabels
    finaljson["diagramdata"] = diagramdata
    finaljson['monthdata'] = monthdata
    finaljson['monthlabels'] = monthlabels

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
