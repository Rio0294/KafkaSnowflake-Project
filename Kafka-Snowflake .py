#Importing required libraries
import faker
from faker import Faker
import random
from datetime import time 
from datetime import datetime
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

load_dotenv()

#Initialize generator object from library
fake = Faker()

#Creating lists of attributes for social media activities
content_type = ['post', 'story']
event_type = ['post_created', 'story_created', 'post_share', 'story_share', 'like_added', 'comment_added']
details = ['Your post is created','Your story is created', 'You shared a post', 'You shared a story', 'You added a like', 'You added a comment']

#Mapping the lists of event types with details
event_details_map = {
    'post_created': 'Your post is created',
    'story_created': 'Your story is created',
    'post_share': 'You shared a post',
    'story_share': 'You shared a story',
    'like_added': 'You added a like',
    'comment_added': 'You added a comment'
}

#Initialize start time in start variable
start = datetime(2010,11,1)

#Define function to generate data
def generate_data():
    event_time = fake.date_time_between(start_date= start, end_date= 'now').strftime('%Y-%m-%d %H:%M:%S')
    ingestion_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    selected_event = random.choice(list(event_details_map.keys()))
    selected_detail = event_details_map[selected_event]

    #To get the comment id only when event is "comment added"
    if selected_event == 'comment_added':
        comment_id = random.randint(301,399)
    else:
        comment_id = None
    return {

    #Creating a dictionary to generate random data
    'EventId' : random.randint(1000, 9999), 'EventType' : selected_event, 'UserId' : random.randint(101,199), 
    'ContentId' : random.randint(201,299), 'ContentType' : random.choice(content_type), 'CommentId' : comment_id,   
    'EventTime' : event_time, 'Details' : selected_detail, 'TimewhileIngesting' : ingestion_time
}

#Creating variable to add in generated data

value = [generate_data() for i in range(10)]


#Kafka Producer Setup
producer = KafkaProducer(bootstrap_servers = '127.0.0.1:9092',
                         value_serializer = lambda v: json.dumps(v).encode('utf-8')
                         )

#Producer send 
for record in value:
    producer.send('Social_Media_Activity', value=record)
producer.flush()
print(f"Produced {len(value)} records")

#Kafka Consumer Setup
consumer = KafkaConsumer("Social_Media_Activity", 
                        bootstrap_servers = '127.0.0.1:9092',                        
                        value_deserializer = lambda v: json.loads(v.decode('utf-8')),
                        auto_offset_reset = 'earliest',enable_auto_commit = False
                        )

#Consumer receive
consumed_records = []

count = 0
for message in consumer:
    print(message.value)
    consumed_records.append(message.value)   
    count += 1
    if count == 10:
        break

 
print("Consumer finished")

df = pd.DataFrame(consumed_records)
print(df.head(10))


#Snowflake Connector Setup
conn = snowflake.connector.connect(
    account=os.getenv("SNOW_ACCOUNT"),
    user=os.getenv("SNOW_USER"),
    password=os.getenv("SNOW_PASSWORD"),
    role=os.getenv("SNOW_ROLE")
)

cs = conn.cursor()

try:
    cs.execute("CREATE WAREHOUSE IF NOT EXISTS MY_WH")
    print("Warehouse created")
    cs.execute("CREATE DATABASE IF NOT EXISTS MY_DB")
    print("Database created")
    cs.execute("USE DATABASE MY_DB")
    cs.execute("CREATE SCHEMA IF NOT EXISTS MY_SCHEMA")
    print("Schema created")
    create_table_snowflake = """
    CREATE TABLE IF NOT EXISTS SOCIAL_MEDIA_ACTIVITY(
        EventId INTEGER, EventType VARCHAR, UserId INTEGER, 
        ContentId INTEGER, ContentType VARCHAR, 
        CommentID INTEGER, EventTime TIMESTAMP,
        Details VARCHAR, TimewhileIngesting TIMESTAMP)
        """
    cs.execute(create_table_snowflake)
    print("Table created")
finally:
    cs.close()


print("Connected to SNOWFLAKE")

#Write to Snowflake
def insert_data(df, SOCIAL_MEDIA_ACTIVITY):
    success, nchunks, nrows, _ = write_pandas(conn,
                                            df= df, 
                                            table_name=SOCIAL_MEDIA_ACTIVITY,
                                            quote_identifiers=False)

insert_data(df, 'SOCIAL_MEDIA_ACTIVITY')

print("Inserted into SNOWFLAKE")

conn.close()