from flask import Flask, render_template
import pymongo

app = Flask(__name__)

client = pymongo.MongoClient("mongodb://54.156.245.11:27017/")
db = client["Music_Database"]
collection = db["log_data"]

pipeline_songs = [
    {
        '$addFields': {
            'log_date': {'$toDate': '$log_date'},
            'dayOfMonth': {'$dayOfMonth': {'$toDate': '$log_date'}}
        }
    },
    {
        '$addFields': {
            'weekOfMonth': {'$toInt': {'$ceil': {'$divide': ['$dayOfMonth', 7]}}}          }
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
        '$sort': {'year': -1, 'month': -1, 'weekOfMonth': 1}  
    }
]

results_songs = collection.aggregate(pipeline_songs)
frequently_played_by_week_of_month = {}
for document in results_songs:
    week_key = f"Week {document['weekOfMonth']}"  
    if week_key not in frequently_played_by_week_of_month:
        frequently_played_by_week_of_month[week_key] = []
    frequently_played_by_week_of_month[week_key].append(document['topSongs'])


pipeline_artists = [
    {
        '$addFields': {
            'log_date': {'$toDate': '$log_date'},
            'dayOfMonth': {'$dayOfMonth': {'$toDate': '$log_date'}}
        }
    },
    {
        '$addFields': {
            'weekOfMonth': {'$toInt': {'$ceil': {'$divide': ['$dayOfMonth', 7]}}}  
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
        '$sort': {'year': -1, 'month': -1, 'weekOfMonth': 1}  
    }
]

results_artists = collection.aggregate(pipeline_artists)
top_artists_by_week_of_month = {}
for document in results_artists:
    week_key = f"Week {document['weekOfMonth']}"  
    if week_key not in top_artists_by_week_of_month:
        top_artists_by_week_of_month[week_key] = []
    top_artists_by_week_of_month[week_key].append(document['topArtists'])

week_1_songs = frequently_played_by_week_of_month["Week 1"]
week_2_songs = frequently_played_by_week_of_month["Week 2"]
week_3_songs = frequently_played_by_week_of_month["Week 3"]
week_4_songs = frequently_played_by_week_of_month["Week 4"]
week_5_songs = frequently_played_by_week_of_month["Week 5"]

week_1_artists = top_artists_by_week_of_month["Week 1"]
week_2_artists = top_artists_by_week_of_month["Week 2"]
week_3_artists = top_artists_by_week_of_month["Week 3"]
week_4_artists = top_artists_by_week_of_month["Week 4"]
week_5_artists = top_artists_by_week_of_month["Week 5"]

@app.route("/top_songs")
def get_most_frequent_by_week_of_month():
    return render_template("artist.html",  week_1=week_1_songs[0],week_2=week_2_songs[0],week_3=week_3_songs[0],week_4=week_4_songs[0],week_5=week_5_songs[0])

@app.route("/top_artist")
def get_most_frequent_artist():
    return render_template("songs.html", week_1=week_1_artists[0],week_2=week_2_artists[0],week_3=week_3_artists[0],week_4=week_4_artists[0],week_5=week_5_artists[0])

@app.route("/")
def hello_world():
    return render_template("index.html")

result = frequently_played_by_week_of_month
print(result["Week 1"])
if __name__ == "__main__":
    app.run(debug=True)
