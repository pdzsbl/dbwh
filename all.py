import redis_part as rs
import influ as infx
import GraphQuery as n4j


def msearch(genre, begintime, endtime, moviename, actor, director):
    starttime = begintime
    if (starttime != ""):
        startyear = starttime.split(" ")[3]
    else:
        startyear = ""
    if (endtime != ""):
        endyear = endtime.split(" ")[3]
    else:
        endyear = ""
    #完成时间到年的转换

    #如果时间不为空，用infx 如果除了actor，director其他都为空，用redis，否则用sqlite，但是考虑到sqlite还么写好，暂时用infx
    if(begintime!=""):
        return infx.msearch(begintime,endtime,actor,director,genre,moviename)
    elif (genre == ""
        and begintime == ""
        and endtime ==""
        and moviename ==""
    ):
        return rs.msearch(genre,startyear,endyear,moviename,actor,director)
    else:
        return infx.msearch(begintime,endtime,actor,director,genre,moviename)

def bsearch(actor, director):
    #只要有一个不为空就用n4j 否则redis
    if(actor=="" and director ==""):
        return rs.bsearch(actor,director)
    else:
        return bsearch(actor,director)
