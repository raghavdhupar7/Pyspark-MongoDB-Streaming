# MongoDB streaming using Databricks or any other Python supporting platform!


Things to note:

- Incremental Logic works on the concept of token streaming in MongoDb, You first need to ingest the data using the full load, then you need to take the "Token" as Resume_Token for the next incremental cases, to do that just keep the Resume_Token = {} and,
  for next run take the MOST RECENT TOKEN that is being generated, THAT WILL BE YOUR MOST RECENT STATE OF THE MONGODB COLLECTION! 
