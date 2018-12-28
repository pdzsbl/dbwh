from py2neo import Graph
import json
import time

# HTTP地址（看配置），用户名（默认neo4j），数据库登陆密码
graph = Graph('http://localhost:7474', username='neo4j', password='123')


def msearch(genre, begintime, endtime, moviename, director, actor):
    global graph
    beginTime = time.time()

    matchStart = 'match (m:Movie),(a:Actor),(d:Director) where (m)-[:ACT]->(a) and (d)-[:DIRECT]->(m) '
    matchEnd = ' return m,a,d'
    matchCond = ''

    if genre != '':
        matchCond += 'and m.genre=\"' + genre + '\"'
    if moviename != '':
        matchCond += ' and m.title=~\".*' + moviename + '.*\"'
    if director != '':
        matchCond += ' and d.name=\"' + director + '\"'
    if actor != '':
        matchCond += ' and a.name=\"' + actor + '\"'
    if begintime != '':
        matchCond += ' and m.formatted_date>\"' + begintime + '\"'
    if endtime != '':
        matchCond += ' and m.formatted_date<\"' + endtime + '\"'

    matchString = matchStart + matchCond + matchEnd
    movies = graph.run(matchString).to_data_frame().reindex(columns=['m', 'a', 'd'])
    moviesList = eval(movies.to_json(orient='records'))
    jsonDict = dict()
    jsonDictA = dict()
    actorDict = dict()
    directorDict = dict()
    k = 1
    lasta = ''
    lastd = ''
    for j in moviesList:
        actorDict[j['m']['id']] = ''
        directorDict[j['m']['id']] = ''
    for j in moviesList:
        if j['a']['name'] != lasta:
            actorDict[j['m']['id']] += j['a']['name'] + ', '
            lasta = j['a']['name']
        if j['d']['name'] != lastd:
            directorDict[j['m']['id']] += j['d']['name'] + ', '
            lastd = j['d']['name']
    lastm = ''
    for i in moviesList:
        if lastm != i['m']['id']:
            lastm = i['m']['id']
            jsonDict['id'] = i['m']['id']
            jsonDict['reviewnum'] = i['m']['review']
            jsonDict['date'] = i['m']['date']
            jsonDict['title'] = i['m']['title']
            jsonDict['genre'] = i['m']['genre']
            actors = actorDict[i['m']['id']]
            jsonDict['actor'] = actors[0:len(actors)-2]
            directors = directorDict[i['m']['id']]
            jsonDict['director'] = directors[0:len(directors)-2]
            jsonDictA[str(k)] = jsonDict
            k += 1
    jsonDictA['length'] = k - 1

    return [time.time() - beginTime, json.dumps(jsonDictA)]


def bsearch(actor, director):
    global graph
    beginTime = time.time()

    matchStart = "match (a:Actor),(d:Director),p=allshortestpaths((d)-[*..10]->(a)) "
    matchEnd = " return a,d,count(p)"
    matchString = ""

    if actor != '':
        if director != '':
            matchString = matchStart \
                          + 'where a.name=\"' + actor + '\" and d.name=\"' + director + '\"' \
                          + matchEnd
        else:
            matchString = matchStart \
                          + 'where a.name=\"' + actor + '\"' \
                          + matchEnd
    else:
        if director != '':
            matchString = matchStart \
                          + 'where d.name=\"' + director + '\"' \
                          + matchEnd
        else:
            matchString = matchStart + matchEnd

    dataFrame = graph.run(matchString).to_data_frame().reindex(columns=['a', 'd', 'count(p)'])
    dataFrame.columns = ['a', 'd', 'n']

    dataList = eval(dataFrame.to_json(orient='records'))
    jsonDict = dict()
    # 演员跟导演的合作关系
    if (actor != '') & (director == ''):
        for i in dataList:
            jsonDict[i['d']['name']] = i['n']
        jsonDictA = dict()
        jsonDictA[actor] = jsonDict
        return [time.time() - beginTime, json.dumps(jsonDictA)]
    if (director != '') & (actor == ''):
        for i in dataList:
            jsonDict[i['a']['name']] = i['n']
        jsonDictA = dict()
        jsonDictA[director] = jsonDict
        print(jsonDictA)
        return [time.time() - beginTime, json.dumps(jsonDictA)]
    if (actor != '') & (director != ''):
        if len(dataList) == 0:
            jsonDict[director] = 0
        else:
            for i in dataList:
                jsonDict[i['d']['name']] = i['n']
        jsonDictA = dict()
        jsonDictA[actor] = jsonDict
        return [time.time() - beginTime, json.dumps(jsonDictA)]
    if (actor == '') & (director == ''):
        jsonDictA = dict()
        for i in dataList:
            jsonDictA[i['a']['name']] = dict()
        for i in dataList:
            jsonDictA[i['a']['name']][i['d']['name']] = i['n']
        return [time.time() - beginTime, eval(json.dumps(jsonDictA))]


'''
这里都测试过了是好的 0-2s
print(bsearch('Scott Baio', ''))
print(bsearch('', 'Bruce Seth Green'))
print(bsearch('Richard Jordan', 'Bille August'))
print(bsearch('Wayne Tippit', 'Bille August'))
'''

'''
真的会执行非常久非常久
print(bsearch('', ''))
'''

if __name__ == '__main__':
    print(bsearch('Scott Baio', ''))
    print(bsearch('', 'Bruce Seth Green'))
    print(bsearch('Richard Jordan', 'Bille August'))
    print(bsearch('Wayne Tippit', 'Bille August'))
