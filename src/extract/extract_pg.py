import pandas as pd


def extract_users_pg(pg_engine) -> pd.DataFrame:
    """
    Get the datetime of the last extraction and extract users after this datetime (new users)
    Then upload to S3 Bucket
    """
    # extraction_date = datetime.now().strftime("%Y%m%d%H%M%S")
    latest_update_date = pg_engine.execute("SELECT * FROM latest_update_date").fetchone()
    if latest_update_date:
        users = f"SELECT * FROM users WHERE updated_at > '{latest_update_date[0]}'"
    else:
        users = "SELECT * from users"
    data = pd.read_sql(users, pg_engine)

    return data


def extract_threads_pg(pg_engine) -> pd.DataFrame:
    """
    Get the datetime of the last extraction and extract threads after this datetime (new threads)
    Also extract thread_id and participants from threads to create a new DataFrame
    Then upload both DataFrames to S3 Bucket
    """
    latest_update_date = pg_engine.execute("SELECT * FROM latest_update_date").fetchone()
    if latest_update_date:
        threads = f"SELECT * FROM threads WHERE updated_at > '{latest_update_date[0]}'"
    else:
        threads = "SELECT * from threads"
    data = pd.read_sql(threads, pg_engine)

    return data


def extract_participants_pg(threads: pd.DataFrame) -> pd.DataFrame:
    """Extract participants from threads"""
    return threads[['thread_id', 'participants']]
