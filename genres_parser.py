import redis

def genres_parser():
    db = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True)
    result = {}
    month = {'January': 0, 'February': 0, 'March': 0, 'April': 0, 'May': 0, 'June': 0, 'July': 0, 'August': 0, 'September': 0, 'October': 0, 'November': 0, 'December': 0}
    for key in db.keys():
        movie_id = key[-10:]
        if movie_id == "w_movie_id" or key == "useful_proxy" or key == "raw_proxy":
            continue
        movie = db.hgetall(key)
        if movie['date'].split()[0] in month:
            month[movie['date'].split()[0]] += 1

        raw_genre = ''
        if 'genre' in movie and movie['genre'] != 'None':
            raw_genre = movie['genre']
        else:
            raw_genre = movie['genres']

        if raw_genre in result:
            result[raw_genre] += 1
        else:
            result[raw_genre] = 1

    final_result = {}
    final_result['others'] = 0
    for key in result:
        if result[key] <= 1400:
            final_result['others'] += result[key]
        else:
            final_result[key] = result[key]

    return final_result, month

if __name__ == '__main__':
    (genre, month) = genres_parser()
    print(month)
