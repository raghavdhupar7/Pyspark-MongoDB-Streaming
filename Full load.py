"""
Dated: 1/11/2024
Author: Raghav Dhupar
"""

import json
import os
import boto3
import botocore
import time
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
import sys
import re
from pymongo import MongoClient
from datetime import datetime
from bson.json_util import dumps
from urllib.parse import quote_plus
import uuid
import bson
import logging as lambdaLogger

Date_format = '%Y-%m-%d %H:%M:%S.%f'
today = datetime.today().strftime(Date_format)
raw = "raw/"

connectUrl = "mongodb+srv://aryandeepak2025:xxxxxxxxxx@cluster0.bgm1ffu.mongodb.net/"
try:
    client = MongoClient(connectUrl)
except Exception as e:
    print(e)
print("Connected Successfully!")
schema = "Policy_Data"
coll = "car"
database = client[schema]
coll_client = database[coll]

# Get all documents at once for full load
cursor = coll_client.find({})
documents = list(cursor)  # Fetch all documents into a list
cnt=coll_client.count_documents({})
cnt = coll_client.count_documents({"_id": {"$exists": True}})
# Process and upload the retrieved documents
upsert_list = []
for doc in documents:
    oid_temp = doc.get("_id")
    if oid_temp is not None and isinstance(oid_temp, bson.objectid.ObjectId):
        create_dt = doc.get("_id").generation_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        print(doc)
        create_dt = "NA"

    temp_data = doc
    temp_data["hive_audit_create_ts"] = create_dt
    temp_data["hive_audit_update_ts"] = today
    temp_data["is_deleted"] = "false"
    temp_data[schema + "_" + coll + "_id"] = str(oid_temp)
    temp_data.pop("_id")
    upsert_list.append(dumps(temp_data))
flag = 5
with coll_client.watch(full_document="updateLookup") as stream:
    while stream.alive:
        change = stream.try_next()
        #print("Change details:", change)
        resume_token = stream.resume_token 
        if change is None:
            flag=flag-1
            print("flag:",flag)
            if(flag<=0):
                break 
        
# Write to file and DBFS
batch_id = "1"
current_date = today
data_file_location = "/dbfs/FileStore/files"
data_file_location = data_file_location[:-1] if data_file_location[-1] == "/" else data_file_location

data_file_temp = str(data_file_location) + "/" + "_temp/" + batch_id + "/" + coll + "_" + current_date + ".json"
data_file = str(data_file_location) + "/" + coll + "_" + current_date + ".json"
if len(upsert_list) == 0:
    print("No items found")
    meta_details = {
        "data_file": data_file,
        "data_file_temp": data_file_temp,
        "sequence": 0,
        "record_length": 0,
        "file_size": 0,
    }
else:
    record_length = len(upsert_list)
    sequence = 1
    upsert_file_name = f"/tmp/{coll}"
    data = ""
    with open(upsert_file_name, "w") as file:
        for l in upsert_list:
            file.write(f"{l}\n")
            print(f"{l}\n")
            data_to_write = l
            data += l + ","

    file_size = os.path.getsize(upsert_file_name)
    print(os.path.isfile(upsert_file_name))

    meta_details = {
        "data_file": data_file,
        "data_file_temp": data_file_temp,
        'resume_token': resume_token['_data'],
        "sequence": sequence,
        "record_length": record_length,
        "file_size": file_size,
    }

    dbfs_destination = f"{data_file}"
    dbutils.fs.rm("dbfs:/FileStore/files/direct_write.json")  # Remove existing file if any
    dbutils.fs.put("dbfs:/FileStore/files/direct_write.json", data)

    update_data = {
            'status': "BATCH_IMDT",
            'mongo_count': cnt,
            'end_time': today,
            'event_details': meta_details
        }
    print(update_data)

client.close()

