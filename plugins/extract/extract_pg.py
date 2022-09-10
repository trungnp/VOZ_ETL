from datetime import datetime

import pandas as pd

from utilities.s3_utils import upload_to_s3


def extract_threads_pg(pg_engine, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY):
    latest_update_date = pg_engine.execute("SELECT * FROM latest_update_date").fetchone()
    extraction_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if latest_update_date:
        threads = f"SELECT * FROM threads WHERE updated_at > '{latest_update_date[0]}'"
    else:
        threads = "SELECT * from threads"
    data = pd.read_sql(threads, pg_engine)
    participants = data[['thread_id', 'participants']]
    fp_p = f'files/participants/raw/{extraction_date}.csv.gz'
    fp_t = f'files/threads/raw/{extraction_date}.csv.gz'
    upload_to_s3(data, fp_t, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)
    upload_to_s3(participants, fp_p, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)
    #print(data.head())

    return {'threads': fp_t, 'participants': fp_p}


def extract_users_pg(pg_engine, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY):
    latest_update_date = pg_engine.execute("SELECT * FROM latest_update_date").fetchone()
    extraction_date = datetime.now().strftime("%Y%m%d%H%M%S")
    if latest_update_date:
        users = f"SELECT * FROM users WHERE updated_at > '{latest_update_date[0]}'"
    else:
        users = "SELECT * from users"
    data = pd.read_sql(users, pg_engine)

    fp_u = f'files/users/raw/{extraction_date}.csv.gz'
    upload_to_s3(data, fp_u, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)

    return fp_u


# def extract_participants():
#     if latest_update_date:
#         participants = f"SELECT thread_id, participants FROM threads WHERE updated_at > '{latest_update_date[0]}'"
#     else:
#         participants = "SELECT thread_id, participants from threads"
#     fp_p = f'files/participants/raw/{extraction_date}.csv.gz'
#     data = pd.read_sql(participants, pg_engine)
#     upload_to_s3(data, fp_p, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)
#
#     return fp_p