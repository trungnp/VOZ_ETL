import gzip
import io
import json
import boto3
import pandas as pd


# config = configparser.ConfigParser()
# config.read('../conf.ini')
# aws = config(section="s3")
# AWS_ACCESS_KEY_ID = aws['AWS_ACCESS_KEY_ID']
# AWS_SECRET_KEY = aws['AWS_SECRET_KEY']
# BUCKET_NAME = aws['BUCKET_NAME']


def rename_file_s3(s3_client, source_bucket, source_key, dest_bucket, dest_key):
    s3_client.copy_object(Bucket=dest_bucket, CopySource=f'{source_bucket}/{source_key}', Key=dest_key)
    s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    print(f'Copied file from {source_bucket}/{source_key} to {dest_bucket}/{dest_key} successful!')


def get_file_from_s3(s3_client, bucket: str, key: str, rename=False):
    """read file from s3 and return a df if csv or dict if json, also rename file if required"""
    f = s3_client.get_object(Bucket=bucket, Key=key)
    if key.endswith('.csv'):
        data = pd.read_csv(f['Body'])
    elif key.endswith('.json'):
        # tmp = f['Body'].read().decode('utf-8')
        data = json.loads(f['Body'].read().decode('utf-8'))
    else:
        data = f
    if rename:
        index = key.rfind('.')
        if index > -1:
            new_name = key[:index] + '_read' + key[index:]
            rename_file_s3(s3_client, source_bucket=bucket, source_key=key, dest_bucket=bucket, dest_key=new_name)
    return data


def get_gzip_s3(bucket, key, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY):
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read()
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
        if key.endswith('json.gz'):
            return json.load(fh)
        else:
            return pd.read_csv(fh, sep='\t')


def upload_to_s3(obj, filepath, BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY):
    '''
    Upload dataframe {df} to s3 in the directory {filepath}
    '''
    # df1 = pd.read_csv(df)
    if isinstance(obj, pd.DataFrame):
        str_buffer = io.StringIO()
        obj.to_csv(str_buffer, index=False,header=True, sep='\t')
        str_buffer.seek(0)
        obj = str_buffer.getvalue()

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb') as fh:
        with io.TextIOWrapper(fh, encoding='utf-8') as wrapper:
            wrapper.write(obj)
    buffer.seek(0)
    body = buffer.getvalue()



    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY,)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=filepath, Body=body)
    print(f'Upload {filepath} to s3 successfully!')
# def get_files_from_s3_prefix(bucket_name: str, prefix: str, is_rename: False):
#     """read files from from s3 by prefix, rename file if required"""
#     s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY)
#     res = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
#     file_to_read = [f['Key'] for f in res['Contents'] if '_read' not in f['Key'].replace(prefix, '') and f['Size'] > 0]
#     list_of_files = []
#     for file in file_to_read:
#         list_of_files.append(get_file_from_s3(s3_client, bucket=bucket_name, key=file, rename=is_rename))
#
#     return list_of_files
#     # else:
#     #     list_of_files = []
#     #     res = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
#     #     file_to_read = [f['Key'] for f in res['Contents'] if 'read' not in f['Key']]
#     #
#     #     for file in file_to_read:
#     #         f = s3_client.get_object(Bucket='voz-s3-npt', Key=file)
#     #         list_of_files.append(json.load(f['Body']))
#     #         rename_file_s3(s3_client, source_bucket='voz-s3-npt', source_key=file, dest_bucket='voz-s3-npt',
#     #                        dest_key=file.replace('.json', '_read.json'))
#     #
#     #     return pd.concat(list_of_files)
