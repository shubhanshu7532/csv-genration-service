import os
import json
import uuid
import time
import boto3
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession

load_dotenv()

# ENV
AWS_REGION = os.getenv("AWS_REGION")
SQS_URL = os.getenv("SQS_URL")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PATH = os.getenv("S3_PATH")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 5))  # seconds

# Spark
spark = SparkSession.builder \
    .appName("MongoToS3ContinuousService") \
    .getOrCreate()

# AWS SQS
sqs = boto3.client('sqs', region_name=AWS_REGION)

def receive_message():
    response = sqs.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )
    messages = response.get('Messages', [])
    if not messages:
        return None
    msg = messages[0]
    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=msg['ReceiptHandle'])
    return json.loads(msg['Body'])

def fetch_data(query, table_name):
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][table_name]
    results = list(collection.find(query))
    client.close()
    return results

def apply_column_mapping(df, mapping):
    selected = [col for col in mapping.keys() if col in df.columns]
    df = df.select(*selected)
    for mongo_col, csv_col in mapping.items():
        df = df.withColumnRenamed(mongo_col, csv_col)
    return df

def save_to_s3(df, table_name):
    file_name = f"{table_name}_{uuid.uuid4().hex}.csv"
    local_path = f"/tmp/{file_name}"
    s3_key = f"{S3_PATH}/{file_name}"

    df.toPandas().to_csv(local_path, index=False)
    boto3.client('s3').upload_file(local_path, S3_BUCKET, s3_key)
    os.remove(local_path)

    print(f"✅ File uploaded to s3://{S3_BUCKET}/{s3_key}")

def process_message(message):
    print("📨 Message Received:", message)

    table_name = message.get("table_name")
    query = message.get("query", {})
    column_mapping = message.get("column_mapping", {})

    if not table_name:
        print("❌ 'table_name' missing.")
        return

    data = fetch_data(query, table_name)
    if not data:
        print("⚠️ No matching data found.")
        return

    df = spark.createDataFrame(data)

    if column_mapping:
        df = apply_column_mapping(df, column_mapping)

    save_to_s3(df, table_name)

def main():
    print("🚀 Starting SQS Mongo-to-S3 service...")

    while True:
        try:
            message = receive_message()
            if message:
                process_message(message)
            else:
                print(f"⏳ No messages in queue. Waiting {POLL_INTERVAL} seconds...")
                time.sleep(POLL_INTERVAL)
        except Exception as e:
            print("❌ Error:", str(e))
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
