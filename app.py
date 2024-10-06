from flask import Flask, render_template
import pymongo

app = Flask(__name__)

# MongoDB client setup
client = pymongo.MongoClient("mongodb://54.156.245.11:27017/")
db = client["Music_Database"]
collection = db["log_data"]

def get_top_songs():
    pipeline = [
        {
            '$addFields': {
                'log_date': {'$toDate': '$log_date'},
                'dayOfMonth': {'$dayOfMonth': {'$toDate': '$log_date'}}
            }
        },
        {
            '$addFields': {
                'weekOfMonth': {'$ceil': {'$divide': ['$dayOfMonth', 7]}}
            }
        },
        {
            '$match': {'song': {'$ne': None}, 'log_date': {'$ne': None}}
        },
        {
            '$group': {
                '_id': {
                    'song': '$song',
                    'weekOfMonth': '$weekOfMonth',
                    'month': {'$month': '$log_date'},
                    'year': {'$year': '$log_date'}
                },
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id.year': -1, '_id.month': -1, '_id.weekOfMonth': -1, 'count': -1}
        },
        {
            '$group': {
                '_id': {
                    'year': '$_id.year',
                    'month': '$_id.month',
                    'weekOfMonth': '$_id.weekOfMonth'
                },
                'topSongs': {
                    '$push': {
                        'song': '$_id.song',
                        'count': '$count'
                    }
                }
            }
        },
        {
            '$project': {
                'year': '$_id.year',
                'month': '$_id.month',
                'weekOfMonth': '$_id.weekOfMonth',
                'topSongs': {'$slice': ['$topSongs', 10]}
            }
        },
        {
            '$sort': {'year': -1, 'month': -1, 'weekOfMonth': -1}
        }
    ]

    results = collection.aggregate(pipeline)
    return list(results)

def get_top_artists():
    pipeline = [
        {
            '$addFields': {
                'log_date': {'$toDate': '$log_date'},
                'dayOfMonth': {'$dayOfMonth': {'$toDate': '$log_date'}}
            }
        },
        {
            '$addFields': {
                'weekOfMonth': {'$ceil': {'$divide': ['$dayOfMonth', 7]}}
            }
        },
        {
            '$match': {'artist': {'$ne': None}, 'log_date': {'$ne': None}}
        },
        {
            '$group': {
                '_id': {
                    'artist': '$artist',
                    'weekOfMonth': '$weekOfMonth',
                    'month': {'$month': '$log_date'},
                    'year': {'$year': '$log_date'}
                },
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id.year': -1, '_id.month': -1, '_id.weekOfMonth': -1, 'count': -1}
        },
        {
            '$group': {
                '_id': {
                    'year': '$_id.year',
                    'month': '$_id.month',
                    'weekOfMonth': '$_id.weekOfMonth'
                },
                'topArtists': {
                    '$push': {
                        'artist': '$_id.artist',
                        'count': '$count'
                    }
                }
            }
        },
        {
            '$project': {
                'year': '$_id.year',
                'month': '$_id.month',
                'weekOfMonth': '$_id.weekOfMonth',
                'topArtists': {'$slice': ['$topArtists', 10]}
            }
        },
        {
            '$sort': {'year': -1, 'month': -1, 'weekOfMonth': -1}
        }
    ]

    results = collection.aggregate(pipeline)
    return list(results)

@app.route("/top_songs")
def get_most_frequent_by_week_of_month():
    frequently_played_by_week_of_month = get_top_songs()
    return render_template("frequent.html", songs=frequently_played_by_week_of_month)

@app.route("/top_artist")
def get_most_frequent_artist():
    top_artists_by_week_of_month = get_top_artists()
    return render_template("artists.html", artist=top_artists_by_week_of_month)

@app.route("/")
def hello_world():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
