import string
import pandas as pd

from utilities.s3_utils import get_gzip_s3, upload_to_s3


def replace_M_K(df:pd.DataFrame):
    for i in ['reply_count', 'view_count']:
        df[i] = df[i].apply(lambda x: str(x).replace("M", "000000").replace("K", "000")).astype(int)


# def removePunctuation(column):
#     """Removes punctuation, changes to lower case, and strips leading and trailing spaces.
#
#     Note:
#         Only spaces, letters, and numbers should be retained.  Other characters should should be
#         eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
#         punctuation is removed.
#
#     Args:
#         column (Column): A Column containing a sentence.
#
#     Returns:
#         Column: A Column named 'sentence' with clean-up operations applied.
#     """
#     return f.trim(f.lower(f.translate(column, string.punctuation, '')))


def count_word(reply):
    '''Remove punctuation from reply, split into single word then count occurrences of each word'''
    def count_w(a_list):
        if isinstance(a_list,float) or a_list is None:
            return None
        # print(a_list)
        tmp = pd.DataFrame(eval(a_list))
        # Remove punctuation
        a = tmp['reply'].str.translate(str.maketrans('', '', string.punctuation))
        # Split into single word
        b = a.str.split(expand=True)
        # Group by word and count occurrence
        c = b.stack()
        d = c.value_counts()


        # rep = sqlContext.createDataFrame(a_list).withColumn('rep_trim', f.trim(f.lower(f.col('reply'))))
        # result = rep.withColumn('single_word', f.explode(f.split(removePunctuation(f.col('rep_trim')), '\s+'))) \
        #     .where(f.col('single_word') != ' ') \
        #     .groupBy(f.col('single_word')) \
        #     .count() \
            # .sort('count', ascending=False) \
        # .collect()
        # Zip result into pair key:value
        return dict(d)
        # return dict(result.collect())

    # Read each row of participants at a time
    return reply.apply(lambda x: count_w(x))


def transform_threads(fp, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY):
    '''
    Add column word_count and exclude column participants
    Convert DataFrame into json to ready to load into redshift
    '''
    df_threads = get_gzip_s3(S3_BUCKET, fp, S3_ACCESS_KEY, S3_SECKET_KEY)
    replace_M_K(df_threads)
    df_threads = df_threads.assign(word_count=lambda x: count_word(x['participants']))
    df_threads['created_at'] = df_threads['created_at'].astype(str)
    df_threads['updated_at'] = df_threads['updated_at'].astype(str)
    cols = df_threads.columns.tolist()
    new_cols = cols[:6] + cols[7:] + cols[6:7]
    data_json = df_threads[new_cols].to_json(orient="records")
    threads_json = data_json[1:-1].replace(',{"thread_id":', '{"thread_id":')

    # print(df_threads[~df_threads['word_count'].isna()]['word_count'])
    clean_fp = fp.replace("/raw/", "/clean/").replace(".csv", ".json")
    upload_to_s3(threads_json, clean_fp, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)

    return clean_fp


def transform_participants(S3_BUCKET, fp, S3_ACCESS_KEY, S3_SECKET_KEY):
    '''Transform column participants into a new DataFrame along with thread_id'''
    df_participants = get_gzip_s3(S3_BUCKET, fp, S3_ACCESS_KEY, S3_SECKET_KEY)
    participants = df_participants[~df_participants['participants'].isna()]
    new_par_df = None
    for i, row in participants.iterrows():
        df_par = pd.DataFrame(eval(row['participants']))
        df_par['thread_id'] = row['thread_id']
        df_par = df_par[['thread_id', 'user_id', 'username', 'reply', 'created_at', 'love', 'brick']]
        new_par_df = df_par if new_par_df is None else pd.concat([new_par_df, df_par])
    new_par_df.drop_duplicates(subset=['thread_id', 'user_id', 'created_at'], keep="last", inplace=True)
    # print(new_par_df.head())
    clean_fp = fp.replace("/raw/", "/clean/")
    upload_to_s3(new_par_df, clean_fp, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)

    return clean_fp


def replace_blank_location(df):
    '''Replace blank location into Namek'''
    df.fillna(value={'location': 'Namek'}, inplace=True)


def transform_users(fp, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY):
    df_users = get_gzip_s3(S3_BUCKET, fp, S3_ACCESS_KEY, S3_SECKET_KEY)
    replace_blank_location(df_users)
    for i in ['created_at', 'updated_at', 'last_seen', 'join_date']:
        df_users[i] = df_users[i].astype(str)

    # print(df_users.head())
    clean_fp = fp.replace("/raw/", "/clean/")
    upload_to_s3(df_users, clean_fp, S3_BUCKET, S3_ACCESS_KEY, S3_SECKET_KEY)

    return clean_fp