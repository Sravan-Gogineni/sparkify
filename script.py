import os
import json
import pymongo

client = pymongo.MongoClient("mongodb://54.156.245.11:27017/")
db = client["Music_Database"]
collection = db["log_data"]  

pipeline = [
    { '$match': { 'song': { '$ne': None } } },  
    { '$group': { '_id': '$song', 'count': { '$sum': 1 } } },  
    { '$sort': { 'count': -1 } } ,
    { '$limit': 10 }
]

results = collection.aggregate(pipeline)

for document in results:
    print(f"Song: {document['_id']}, Count: {document['count']}")
