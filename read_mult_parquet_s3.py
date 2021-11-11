import io
import pandas as pd
import boto3
from boto3.session import Session
import pyarrow.parquet as pq
import json

# Ler credenciais de um json

with open('credenciais.json') as f:
    entrada = json.load(f)

    aws_access_key_id = entrada['aws_access_key_id']
    aws_secret_access_key = entrada['aws_secret_access_key']
    bucket_name = entrada['bucket_name']
    aws_key = entrada['aws_key']

session = Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3 = session.resource('s3')
bucket = s3.Bucket(bucket_name)
client = boto3.client('s3', aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
objects_dict = client.list_objects_v2(Bucket=bucket_name, Prefix=aws_key)

def download_s3_parquet_file(s3, bucket, key):
    buffer = io.BytesIO()
    s3.Object(bucket, key).download_fileobj(buffer)
    return buffer
	
s3_keys = [item['Key'] for item in objects_dict['Contents'] if item['Key'].endswith('.parquet')]

def read_parquet():
    buffers = [download_s3_parquet_file(s3, bucket_name, key) for key in s3_keys]
    dfs = [pq.read_table(buffer).to_pandas() for buffer in buffers]
    return pd.concat(dfs, ignore_index=True)

df = read_parquet()

print(df)
