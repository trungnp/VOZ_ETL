import string
import pandas as pd


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
    """Remove punctuation from reply, split into single word then count occurrences of each word"""
    def count_w(a_list):
        if a_list is None:
            return None
        tmp = pd.DataFrame(a_list)
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


def transform_threads(df):
    """
    Add column word_count and exclude column participants
    Convert DataFrame into json to ready to load into redshift
    """
    replace_M_K(df)
    df = df.assign(word_count=lambda x: count_word(x['participants']))
    df['created_at'] = df['created_at'].astype(str)
    df['updated_at'] = df['updated_at'].astype(str)
    cols = df.columns.tolist()
    new_cols = cols[:6] + cols[7:] + cols[6:7]
    data_json = df[new_cols].to_json(orient="records")
    threads_json = data_json[1:-1].replace(',{"thread_id":', '{"thread_id":')

    return threads_json


def transform_participants(df):
    """Transform column participants into a new DataFrame along with thread_id"""
    participants = df[~df['participants'].isna()]
    participants_df = None
    for i in range(participants.shape[0]):
        df_par = pd.DataFrame(participants['participants'].iloc[i])
        df_par['thread_id'] = participants['thread_id'].iloc[i]
        df_par = df_par[['thread_id', 'user_id', 'username', 'reply', 'created_at', 'love', 'brick']]
        participants_df = df_par if participants_df is None else pd.concat([participants_df, df_par])
    # result['created_at'] = result['created_at'].astype(str)
    participants_cleaned = participants_df.drop_duplicates(subset=['thread_id', 'user_id', 'created_at'], keep="last")

    return participants_cleaned


def transform_users(df):
    replace_blank_location(df)
    for i in ['created_at', 'updated_at', 'last_seen', 'join_date']:
        df[i] = df[i].astype(str)

    return df


def replace_blank_location(df):
    """Replace blank location into Namek"""
    df.fillna(value={'location': 'Namek'}, inplace=True)

