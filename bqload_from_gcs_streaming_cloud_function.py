'''
This Cloud function is responsible for:
- Parsing and validating new files added to Cloud Storage.
- Checking for duplications.
- Inserting files' content into BigQuery.
- Logging the ingestion status into Cloud Firestore and Stackdriver.
- Publishing a message to either an error or success topic in Cloud Pub/Sub.
'''

import json
import logging
import os
import traceback
from datetime import datetime

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz
import re



PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'fastly_logs'
BQ_TABLE = 'fastly_logs'
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_error_topic')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'streaming_success_topic')
DB = firestore.Client()
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
BQ = bigquery.Client()


def fastly_streaming(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    db_ref = DB.document(u'streaming_files/%s' % file_name)
    if _was_already_ingested(db_ref):
        _handle_duplication(db_ref)
    else:
        try:
            _insert_into_bigquery(bucket_name, file_name)
            _handle_success(db_ref)
        except Exception:
            _handle_error(db_ref)


def _was_already_ingested(db_ref):
    status = db_ref.get()
    return status.exists and status.to_dict()['success']


def _handle_duplication(db_ref):
    dups = [_now()]
    data = db_ref.get().to_dict()
    if 'duplication_attempts' in data:
        dups.extend(data['duplication_attempts'])
    db_ref.update({
        'duplication_attempts': dups
    })
    logging.warn('Duplication attempt streaming file \'%s\'' % db_ref.id)


def _insert_into_bigquery(bucket_name, file_name):
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    blob = CS.get_bucket(bucket_name).blob(file_name)
    blob1= blob.download_as_string()
    blob2= blob1.splitlines()
    json_arr = []
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
                                 row_ids=None,
                                 retry=retry.Retry(deadline=30))
        
    if errors != []:
        raise BigQueryError(errors)


def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error streaming file \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(ERROR_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.error(message)


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
