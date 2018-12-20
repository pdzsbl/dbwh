import influxdb as idb

import re
vhs = re.compile("(.+?)(vhs|VHS)")
#print(vhs.findall("Shark Skin Man and Peach Hip Girl")[0][0])
#client = idb.InfluxDBClient('localhost', 8086, 'root', 'root', 'dbwh')
'''
result = client.query("select * from dbwarehouse where id='B00009MEJO';") # 显示数据库中的表
print("Result: {0}".format(result))
'''

monthg = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,
         "Sep":9,"Oct":10,"Nov":11,"Dec":12}

def datedeal(date):
    if(date == "None"):
        return "1800-1-1-T00:00:00Z"
    sp = date.split(" ")#['Mon', 'Nov', '05', '2018', '00:00:00', 'GMT+0800', '(China', 'Standard', 'Time)']
    month = str(monthg[sp[1]])
    day = sp[2].replace(",","")
    year = sp[3]
    return year+"-"+month+"-"+day+"T00:00:00Z"

def titledeal(t):
    if(len(vhs.findall(t))==0):
        return t
    elif len(vhs.findall(t)[0])==0:
        return t
    else:
        return vhs.findall(t)[0][0].strip()

def msearch(begintime=None,endtime=None,actor=None,director=None,genre=None,title=None,review=None):
    client = idb.InfluxDBClient('localhost', 8086, 'root', 'root', 'dbwh')
    qstr = "select * from dbwarehouse where "
    if(begintime != "" and endtime != ""):
        begintime = datedeal(begintime)
        endtime = datedeal(endtime)
        qstr+= "time >= '"+begintime+"' and time <= '"+endtime+"' and "
    if(genre != ""):
        qstr += "genre = '"+genre+"' and "
    if(title != ""):
        qstr += "title LIKE '%" + title + "%' and "
    qstr += "review != '-1' tz('Asia/Shanghai');"
    #print(qstr)
    result = client.query(qstr)


    for i in result:
        new = []
        if (actor != ""):
                for j in i:
                    try:
                        j["actor"] = eval(j["actor"])
                    except:
                        continue
                    for x in j["actor"]:
                        if(actor == x):
                            new.append(j)

        else:
            new = i
        newx = []
        if (director != ""):
                for j in new:
                    try:
                        j["director"] = eval(j["director"])
                    except:
                        continue
                    for x in j["director"]:
                        if(director == x):
                            newx.append(j)
        else:
            newx = new
        return newx

"""
for i in msearch(genre="Japanese",actor="Chikao Ohtsuka"):
    print("nnnnn",i)
"""

def bsearch(actor, director):
    client = idb.InfluxDBClient('localhost', 8086, 'root', 'root', 'dbwh')
    qstr = "select * from dbwarehouse"
    result = client.query(qstr)

    director_actor_list = []

    if actor =="" and director == "":
        k = msearch("","","","",'','')

    elif(director==""):
        for i in result:
            new = []
            for j in i:
                try:
                    j["actor"] = eval(j["actor"])
                except:
                    continue
                try:
                    j["director"] = eval(j["director"])
                except:
                    continue
                if (actor in j["actor"]):
                    new.append(j)

            dict = {}
            for every in new:
                for everydire in every["director"]:
                    if(dict.has_key(everydire)==True):
                        dict[everydire]+=1
                    else:
                        dict[everydire]=1

            director_actor_list.clear()

            finaldict = {}
            finaldict["len"]=len(dict.keys())
            nm = 1
            for dname in dict.keys():
                bina = {"actor":actor,"director":dname,"times":dict[dname]}
                finaldict[str(nm)] = bina
            return finaldict


    elif(actor==""):
        for i in result:
            new = []
            for j in i:
                try:
                    j["actor"] = eval(j["actor"])
                except:
                    continue
                try:
                    j["director"] = eval(j["director"])
                except:
                    continue
                if (director in j["director"]):
                    new.append(j)
            dict = {}
            for every in new:
                for everyactor in every["actor"]:
                    if (dict.has_key(everyactor) == True):
                        dict[everyactor] += 1
                    else:
                        dict[everyactor] = 1

            director_actor_list.clear()

            finaldict = {}
            finaldict["len"] = len(dict.keys())
            nm = 1
            for aname in dict.keys():
                bina = {"actor": aname, "director": director, "times": dict[aname]}
                finaldict[str(nm)] = bina
            return finaldict
        '''
        if(identity1 == "actor" and identity2 == "actor"):
            for i in result:
                new = []
                for j in i:
                    try:
                        j["actor"] = eval(j["actor"])
                    except:
                        continue
                    try:
                        j["director"] = eval(j["director"])
                    except:
                        continue
                    if(name1 in j["actor"] and name2 in j["actor"]):
                        new.append(j)
                title = []
                newx = []
                for every in new:
                    t = titledeal(every["title"])
                    if t not in title:
                        title.append(t)
                        newx.append(every)
                return newx
        elif(identity1 == "director" and identity2 == "director"):
            for i in result:
                new = []
                for j in i:
                    try:
                        j["actor"] = eval(j["actor"])
                    except:
                        continue
                    try:
                        j["director"] = eval(j["director"])
                    except:
                        continue
                    if(name1 in j["director"] and name2 in j["director"]):
                        new.append(j)
                title = []
                newx = []
                for every in new:
                    t = titledeal(every["title"])
                    if t not in title:
                        title.append(t)
                        newx.append(every)
                return newx
        '''

    else:
        k = []
        dname = director
        aname = actor
        k = msearch(director=director,actor=actor)

        return {"len":"1","1":{"actor":aname,"director":dname,"times":len(k)}}

