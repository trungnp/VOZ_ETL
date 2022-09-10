import gzip
import io
import json
from io import StringIO

import boto3
import pandas as pd


def upload_to_s3(obj, filepath, BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY):
    '''
    Upload gzipped object (DataFrame of json) to s3 in the directory {filepath}
    '''

    if isinstance(obj, pd.DataFrame):
        str_buffer = StringIO()
        obj.to_csv(str_buffer, index=False, header=True, sep='\t')
        str_buffer.seek(0)
        obj = str_buffer.getvalue()

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as fh:
        with io.TextIOWrapper(fh, encoding='utf-8') as wrapper:
            wrapper.write(obj)
    buffer.seek(0)
    body = buffer.getvalue()

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY, )
    s3_client.put_object(Bucket=BUCKET_NAME, Key=filepath, Body=body)
    print(f'Upload {filepath} to s3 successfully!')



def get_gzip_s3(bucket, key, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY):
    '''Read gzipped file from s3'''
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read()
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
        if key.endswith('json.gz'):
            return json.load(fh)
        else:
            return pd.read_csv(fh, sep='\t')
