import os
import json
import pymongo
import time 

client = pymongo.MongoClient("mongodb://54.156.245.11:27017/")
db = client["Music_Database"]


folder_path = r"D:\Downloads\data-1\data\log_data\2018\11"
log_number = 0 
for item in os.listdir(folder_path):

    file_path = os.path.join(folder_path, item)
  
    
    with open(file_path, 'r') as f:

        for line in f:
            data = json.loads(line.strip())  
            data["log_date"] = item[0:10] 
            doc = db["log_data"]
            unix_each_ts = data["ts"]
            ts_seconds = unix_each_ts / 1000.0
            local_time = time.localtime(ts_seconds)


            utc_time = time.gmtime(ts_seconds)
            time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", utc_time)
            data["time_stamp"] = time_stamp
            log_id = doc.insert_one(data).inserted_id
            log_number = log_number +1
            print(log_id,log_number)
            
    
    
    
   