import pandas as pd
import boto3
import time
import os


CSV_PATH = 'data/transactions.csv'
BUCKET_NAME = 'databricksbucket79'  
UPLOAD_PREFIX = 'stream_input/'
CHUNK_SIZE = 10000
SLEEP_INTERVAL = 1


s3 = boto3.client('s3')
df = pd.read_csv(CSV_PATH)
total_chunks = (len(df) // CHUNK_SIZE) + 1

print(f"Uploading {total_chunks} chunks...")

for i in range(total_chunks):
    chunk = df.iloc[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE]
    filename = f'transactions_part_{i+1}.csv'
    local_path = f'temp/{filename}'

    chunk.to_csv(local_path, index=False)
    s3.upload_file(local_path, BUCKET_NAME, f'{UPLOAD_PREFIX}{filename}')
    print(f" Uploaded: {filename}")

    os.remove(local_path)
    time.sleep(SLEEP_INTERVAL)