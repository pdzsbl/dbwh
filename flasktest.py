from flask import Flask
from flask import redirect
from flask import request
app = Flask(__name__)


@app.route('/movieserach')
def moviesearch():
    #return redirect('/static/ns-3-tutorial.pdf')
    starttime = request.args.get("starttime")
    endtime = request.args.get("endtime")
    partmoviename = request.args.get("moviename")
    director = request.args.get("director")
    actor = request.args.get("actor")
    genre = request.args.get("genre")

    #movie = dbsearch function

    movie = None
    return movie

@app.route('/collaboration')
def collaboration():
    actorname1 = request.args.get("actorname1")
    actorname2 = request.args.get("actorname2")
    directorname1 = request.args.get("directorname1")
    directorname2 = request.args.get("directorname2")
    mostnumber = request.args.get("mostnumber")
    #relationships = dbsearch function
    relationships = None
    return relationships



if __name__ == '__main__':
    app.run()
