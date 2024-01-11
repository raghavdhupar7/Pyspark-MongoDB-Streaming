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

#uploading to S3 when S3 is setup
def upload_file_to_s3(bucketname, filePathInS3, localFilePath, region=dict['REGION']):

    """
    Uploads the local file to S3
    :param bucketname: Name of the bucket
    :param filePathInS3: Path of file in S3
    :param localFilePath: Path of file in local
    :param region: AWS region
    :return:
    """

    resource = boto3.resource(service_name='s3', region_name=region)
    resource.Bucket(bucketname).upload_file(localFilePath, filePathInS3,

                                            ExtraArgs={'ServerSideEncryption': 'aws:kms',
                                                       'SSEKMSKeyId': dict["S3_ENCRYPTION_KEY"]})
    return localFilePath


#uploading to DBFS because s3 is not there
def upload_file_to_dbfs(local_file_path,dbfs_path):
    """
    Uploads the local file to the specified DBFS path.

    :param dbfs_path: The full path to the destination file on DBFS (e.g., "dbfs:/FileStore/files/my_file.txt")
    :param local_file_path: The path to the local file to upload
    """

    try:
        dbutils.fs.cp(local_file_path, dbfs_path)
        print(f"File uploaded to DBFS: {dbfs_path}")
    except Exception as e:
        print(f"Error uploading file: {e}")

#main function
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

cnt=coll_client.count_documents({})
cnt = coll_client.count_documents({"_id": {"$exists": True}})
print(cnt)

document_collection = {}
upsert_list = []

flag=40
res_token = '82659BB888000000072B0229296E04'
#res_token = '82659E4688000000072B0229296E04'
#res_token = None
resume_token = {}
if res_token == None:
    resume_token = None
else:
    resume_token['_data'] = res_token
lambdaLogger.debug("### This is resume Token \n")
lambdaLogger.debug(resume_token)
            
with coll_client.watch(resume_after=resume_token, full_document="updateLookup") if resume_token is not None else coll_client.watch(full_document="updateLookup") as stream:
    while stream.alive:
        change = stream.try_next()
        #print("Change details:", change)
        resume_token = stream.resume_token  # Update the resume_token variable
        #print(coll_client.list_indexes())
        #print(resume_token)
        if change is not None:
            print("Receiving the data...")
            print("Change details:", change.get('updateDescription', 'No update description'))
            print("Full Document:", change.get('fullDocument', 'No full document'))
            if "fullDocument" in change and change["fullDocument"] is not None:
                oid_temp=change["fullDocument"].get("_id")
                if(oid_temp != {} and type(oid_temp)==bson.objectid.ObjectId):
                    create_dt = change["fullDocument"].get("_id").generation_time.strftime(Date_format)
                else:
                    print(change["fullDocument"])
                    create_dt='NA'
                    
                temp_data = change["fullDocument"]
                temp_data["hive_audit_create_ts"] = create_dt
                temp_data["hive_audit_update_ts"] = today
                temp_data["is_deleted"] = "false"
                temp_data[schema+"_"+coll+"_id"] = str(oid_temp)
                temp_data.pop('_id')
                upsert_list.append(dumps(temp_data))
            else:
                oid_temp=change["documentKey"].get("_id")
                if(oid_temp != {} and type(oid_temp)==bson.objectid.ObjectId):
                    create_dt = change["documentKey"].get("_id").generation_time.strftime(Date_format)
                else:
                    print(change["documentKey"])
                    create_dt='NA'
                temp_data = change["documentKey"]
                temp_data["hive_audit_create_ts"] = create_dt
                temp_data["hive_audit_update_ts"] = today
                temp_data["is_deleted"] = "true"
                temp_data.pop('_id')
                upsert_list.append(dumps(temp_data))
        else:
            flag=flag-1
            print("flag:",flag)
            if(flag<=0):
                break
    batch_id = '1'
    print("len==", len(upsert_list))
    current_date = str(datetime.now())
    current_date = str(current_date[:4]) + str(current_date[5:7]) + str(current_date[8:10]) + str(current_date[11:13])
    data_file_location = "/dbfs/FileStore/files"
    data_file_location = data_file_location[:-1] if data_file_location[-1] == '/' else data_file_location

    data_file_temp = str(data_file_location)  + '/' + '_temp/' + batch_id + '/' + coll + '_' + current_date + '.json'

    data_file_temp2 = '{}_temp/{}/{}_{}.data'.format(str(data_file_location),batch_id,coll,current_date)

    #data_file =  str(data_file_location) + '/' + coll + '_' + today + '.data'
    data_file =  str(data_file_location) + '/' + coll + '_' + current_date + '.json'

    if len(upsert_list) == 0:
        print("No items found")
        meta_details = {'data_file': data_file, 'data_file_temp': data_file_temp, 'sequence': 0, \
                        'record_length': 0, 'file_size': 0}
    else:
        # Collecting meta information
        # Collecting meta information
        record_length = len(upsert_list)
        sequence = 1
        upsert_file_name = f"/tmp/{coll}"  # Use f-string for clarity
        data = ""
        with open(upsert_file_name, 'w') as file:
            for l in upsert_list:
                file.write(f"{l}\n")  # Ensure newline is added
                print(f"{l}\n")
                data_to_write = l
                data += l + ","
            
        

        
        file_size = os.path.getsize(upsert_file_name)
        print(os.path.isfile(upsert_file_name))
        #print(file_size)
        meta_details = {
            'data_file': data_file,
            'data_file_temp': data_file_temp,
            'sequence': sequence,
            'record_length': record_length,
            'file_size': file_size
        }

        # Uploading file to DBFS
        
        dbfs_destination = f"{data_file}"  # Use coll for filename

        #dbutils.fs.rm(f"dbfs:/FileStore/files/{data_file}")
        #dbutils.fs.put(f"dbfs:/FileStore/files/{data_file}", data_to_write)
        dbutils.fs.rm("dbfs:/FileStore/files/direct_write.json")
        dbutils.fs.put("dbfs:/FileStore/files/direct_write.json", data)
        #dbutils.fs.put(dbfs_destination, upsert_file_name, overwrite=True)
        #upload_file_to_dbfs(upsert_file_name, dbfs_destination)  # Correct function call order
        #df.write.format("json").save(dbfs_destination)  # Replace "csv" with desired format
        #dbutils.fs.put("/dbfs/FileStore/files", upsert_file_name)
        
        update_data = {
            'status': "BATCH_IMDT",
            'mongo_count': cnt,
            'resume_token': resume_token['_data'],
            'end_time': today,
            'event_details': meta_details
        }
        print(update_data)
client.close()