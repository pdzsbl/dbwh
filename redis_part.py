import redis

MONTH = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7, "August": 8,
         "September": 9, "October": 10, "November": 11, "December": 12}

def date_parser(date):
    if date == "None":
        return "1800-1-1-T00:00:00Z"
    sp = date.split()
    month = str(MONTH[sp[0]])
    day = sp[1].replace(",", "")
    year = sp[2]
    return year+"-"+month+"-"+day+"-T00:00:00Z"

def list_parser(raw_list):
    real_list = raw_list.strip('[]').split(',')
    result = [x.strip().strip('\'').strip('"') for x in real_list]
    return result

def list_to_str(raw_list):
    string = ''
    for value in raw_list:
        string += (value + ',')
    return string.strip().strip(",")

def msearch(genre, begintime, endtime, moviename, actor, director):
    """
        All params is string
    """
    db = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True)

    result = {'length': 0}

    for key in db.keys():
        movie_id = key[-10:]
        if movie_id == "w_movie_id" or key == "useful_proxy" or key == "raw_proxy":
            continue
        movie = db.hgetall(key)
        new_movie = {}
        # Director
        director_list = list_parser(movie['director'])
        if director == '':
            new_movie['director'] = list_to_str(director_list)
        else:
            if director not in director_list:
                continue
            else:
                new_movie['director'] = director
        # Actor

        actor_list = list_parser(movie['actor'])
        if actor == '':
            new_movie['actor'] = list_to_str(actor_list)
        else:
            if actor not in actor_list:
                continue
            else:
                new_movie['actor'] = actor
        # Title
        if moviename not in movie['title']:
            continue
        else:
            new_movie['title'] = movie['title']
        # Genre
        raw_genre = ''
        if 'genre' in movie and movie['genre'] != 'None':
            raw_genre = movie['genre']
        else:
            raw_genre = movie['genres']

        if genre == '':
            new_movie['genre'] = raw_genre
        else:
            if genre == raw_genre:
                new_movie['genre'] = raw_genre
            else:
                continue
        # Date
        if begintime == '':
            new_movie['time'] = movie['date']
        else:
            raw_time = movie['date'][-4:]
            if raw_time == 'None':
                continue
            elif int(raw_time) >= int(begintime) and int(raw_time) <= int(endtime):
                new_movie['time'] = raw_time
            else:
                continue
        new_movie['id'] = movie['id']
        new_movie['reviewnum'] = movie['review']
        result[result['length']] = new_movie
        result['length'] += 1
    return result

def bsearch(actor, director):
    db = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True)

    relation = {}
    if actor != '' and director != '':
        relation[actor] = {director: 0}
        for key in db.keys():
            movie_id = key[-10:]
            if movie_id == "w_movie_id" or key == "useful_proxy" or key == "raw_proxy":
                continue
            movie = db.hgetall(key)

            director_list = list_parser(movie['director'])
            actor_list = list_parser(movie['actor'])
            if actor in actor_list and director in director_list:
                relation[actor][director] += 1
        return relation
    elif actor == '' and director == '':
        for key in db.keys():
            movie_id = key[-10:]
            if movie_id == "w_movie_id" or key == "useful_proxy" or key == "raw_proxy":
                continue
            movie = db.hgetall(key)
            director_list = list_parser(movie['director'])
            actor_list = list_parser(movie['actor'])

            for key_actor in actor_list:
                if key_actor not in relation:
                    relation[key_actor] = {}
                for key_director in director_list:
                    if key_director not in relation[key_actor]:
                        relation[key_actor] = {key_director: 1}
                    else:
                        relation[key_actor][key_director] += 1
        return relation
    elif actor == '':
        for key in db.keys():
            movie_id = key[-10:]
            if movie_id == "w_movie_id" or key == "useful_proxy" or key == "raw_proxy":
                continue
            movie = db.hgetall(key)
            director_list = list_parser(movie['director'])
            if director not in director_list:
                continue
            else:
                actor_list = list_parser(movie['actor'])
                for key_actor in actor_list:
                    if key_actor in relation:
                        relation[key_actor][director] += 1
                    else:
                        relation[key_actor] = {director: 1}
        return relation
    else: # Only Actor
        relation[actor] = {}
        for key in db.keys():
            movie_id = key[-10:]
            if movie_id == "w_movie_id" or key == "useful_proxy" or key == "raw_proxy":
                continue
            movie = db.hgetall(key)
            actor_list = list_parser(movie['actor'])
            if actor not in actor_list:
                continue
            else:
                director_list = list_parser(movie['director'])
                for key_director in director_list:
                    if key_director in relation[actor]:
                        relation[actor][key_director] += 1
                    else:
                        relation[actor] = {key_director: 1}
        return relation

if __name__ == '__main__':

    print(msearch(actor="Akira Kamiya",genre="",begintime="",endtime="",moviename="",director=""))
