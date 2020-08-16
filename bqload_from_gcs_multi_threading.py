
import json
import logging
import os
import traceback
from datetime import datetime
import re

from google.cloud import bigquery
from google.cloud import storage
import pytz
from queue import Queue
from threading import Thread

task_queue = Queue()
PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'fastly_logs'
BQ_TABLE = 'test5'
bucket_name='fastly_from_aws'
destination_bucket='fastly_errors'
CS = storage.Client()
BQ = bigquery.Client()

source_bucket = CS.get_bucket(bucket_name)
destination_bucket = CS.get_bucket(destination_bucket)

table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)

def worker():
    while True:
      blob = task_queue.get()
      print(blob.name)
      blob1= blob.download_as_string()
      blob2= blob1.splitlines()
      for row in blob2:
          blob_str = str(row,'ISO-8859-1')
          blob_split = blob_str.split(']: ')
          pt_1 = blob_split[0]
          decodedString = blob_split[1]
          if 'Googlebot' in decodedString:
              row_temp = re.sub(r'(?<!\\)\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})', r'', decodedString)
              row = json.loads(row_temp)
              errors = BQ.insert_rows_json(table,
                                   json_rows=[row],
                                   row_ids=[blob.name])
      if errors != []:
        print(blob.name + " processing error")
      else:
        print(blob.name + " processing complete")
        source_blob = source_bucket.blob(blob.name)
        source_bucket.copy_blob(blob, destination_bucket, blob.name)
        source_blob.delete()


list_len =len(list(source_bucket.list_blobs(prefix='20')))
threads = [Thread(target=worker) for _ in range(10)]
[task_queue.put(item) for item in source_bucket.list_blobs(prefix='20')]
[thread.start() for thread in threads]
task_queue.join() 

time.sleep(60) 

print('Script Done')
