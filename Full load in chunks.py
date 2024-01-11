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

def _write_chunk_to_dbfs(chunk_data, chunk_number):
    batch_id = "1"
    current_date = today
    data_file_location = "/dbfs/FileStore/files"
    data_file_location = data_file_location[:-1] if data_file_location[-1] == "/" else data_file_location

    data_file_temp = str(data_file_location) + "/" + "_temp/" + batch_id + "/" + coll + "_" + current_date + f"_{chunk_number}.json"
    data_file = str(data_file_location) + "/" + coll + "_" + current_date + f"_{chunk_number}.json"

    with open(f"/tmp/{coll}_{chunk_number}", "w") as file:
        for l in chunk_data:
            file.write(f"{l}\n")

    file_size = os.path.getsize(f"/tmp/{coll}_{chunk_number}")
    print(f"Chunk {chunk_number} written to file: {data_file}")

    dbutils.fs.put(data_file, r"/tmp/{coll}_{chunk_number}")


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
cnt = coll_client.count_documents({})
cnt = coll_client.count_documents({"_id": {"$exists": True}})

# Process and upload the retrieved documents in chunks of 1000 records
current_chunk = []
chunk_number = 1

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
    current_chunk.append(dumps(temp_data))

    if len(current_chunk) == 1000:
        # Write the current chunk to a separate file
        _write_chunk_to_dbfs(current_chunk, chunk_number)
        chunk_number += 1
        current_chunk = []

# Write any remaining documents in the last chunk
if current_chunk:
    _write_chunk_to_dbfs(current_chunk, chunk_number)

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

client.close()
